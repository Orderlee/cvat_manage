import os, json, zipfile, argparse, torch, time
from pathlib import Path
from ultralytics import YOLO
from PIL import Image
from dotenv import load_dotenv
from datetime import datetime
import requests, colorsys
from math import ceil
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import csv
from itertools import cycle
from typing import Optional, Set, Iterable, List, Dict

# ====== ENV ======
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)
CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORGANIZATIONS = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",")]
ASSIGN_LOG_PATH = Path(f"./logs/assignments_log.csv")
ASSIGN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# ====== Utils ======
def hsv_to_hex(h, s, v):
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return '#{:02X}{:02X}{:02X}'.format(int(r * 255), int(g * 255), int(b * 255))

def _debug_http_error(prefix, res):
    print(f"[{prefix}] status={res.status_code}")
    try:
        print(f"[{prefix}] body.json=", res.json())
    except Exception:
        print(f"[{prefix}] body.text=", res.text)

def _safe_json(res):
    try:
        return res.json()
    except Exception:
        return {"_text": res.text}

# ====== CVAT API (organizations / headers / preflight) ======
def get_or_create_organization(name):
    """조직 조회/생성. (헤더에 org 미포함)"""
    headers = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}
    res = requests.get(f"{CVAT_URL}/api/organizations", headers=headers)
    res.raise_for_status()
    for org in res.json().get("results", []):
        if org["slug"] == name or org["name"] == name:
            return org["id"], org["slug"]
    slug = name.lower().replace(" ", "-")
    res = requests.post(f"{CVAT_URL}/api/organizations", headers=headers, json={"name": name, "slug": slug})
    res.raise_for_status()
    return res.json()["id"], slug

def build_headers(org_slug):
    """공통 헤더: 일부 배포에서 커스텀 헤더 드롭 방지를 위해 쿼리스트링도 병행 사용"""
    return {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json",
        "X-Organization": org_slug
    }

def preflight_check(headers, org_slug):
    """동일 컨텍스트로 /api/tasks 접근이 허용되는지 사전 확인"""
    url = f"{CVAT_URL}/api/tasks?org={org_slug}"
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            print("✅ Preflight OK: /api/tasks GET authorized with org context")
            return True
        else:
            _debug_http_error("Preflight /api/tasks", res)
            return False
    except Exception as e:
        print("❌ Preflight exception:", e)
        return False

def _normalize_and_dedupe_labels(labels):
    """라벨 정규화+중복 제거(순서 보존) 및 기본 attributes 포함"""
    seen = set()
    uniq = []
    for raw in labels:
        lbl = str(raw).strip()
        if not lbl:
            continue
        if lbl in seen:
            print(f"[WARN] Duplicate label skipped: '{lbl}'")
            continue
        seen.add(lbl)
        uniq.append(lbl)
    if not uniq:
        raise ValueError("No valid labels after normalization. Check --labels arguments.")
    label_defs = []
    for i, lbl in enumerate(uniq):
        label_defs.append({
            "name": lbl,
            "color": hsv_to_hex(i / max(1, len(uniq)), 0.7, 0.95),
            "attributes": []  # 일부 버전에서 필수
        })
    return label_defs, uniq

def create_project(name, labels, headers, org_slug):
    """프로젝트 생성(중복 라벨 방지, org 쿼리 병행)"""
    label_defs, uniq_labels = _normalize_and_dedupe_labels(labels)
    base = f"{CVAT_URL}/api/projects"
    url_candidates = [f"{base}/?org={org_slug}", f"{base}?org={org_slug}", f"{base}/", base]
    last_err = None
    for url in url_candidates:
        res = requests.post(url, headers=headers, json={"name": name, "labels": label_defs})
        if res.status_code >= 400:
            _debug_http_error(f"Project create POST {url}", res)
        try:
            res.raise_for_status()
            pid = res.json()["id"]
            print(f"✅ Project created via {url} → id={pid}, labels={uniq_labels}")
            return pid
        except requests.HTTPError as e:
            last_err = e
    raise last_err

def create_task_with_zip(name, project_id, zip_path, headers, org_slug):
    """Task 생성 + ZIP 업로드. org 쿼리 병행 및 트레일링 슬래시 호환."""
    base = f"{CVAT_URL}/api/tasks"
    url_candidates = [f"{base}/?org={org_slug}", f"{base}?org={org_slug}", f"{base}/", base]
    payload = {
        "name": name,
        "project_id": project_id,
        "image_quality": 100,
        "segment_size": 100
    }
    task_id = None
    last_err = None
    for url in url_candidates:
        res = requests.post(url, headers=headers, json=payload)
        if res.status_code >= 400:
            _debug_http_error(f"Task create POST {url}", res)
        try:
            res.raise_for_status()
            task_id = res.json()["id"]
            print(f"✅ Task created via {url} → id={task_id}")
            break
        except requests.HTTPError as e:
            last_err = e
            continue
    if task_id is None:
        raise last_err

    # ---- 데이터 업로드 (files 업로드 시 Content-Type 제거 필수) ----
    upload_headers = headers.copy()
    upload_headers.pop("Content-Type", None)
    with open(zip_path, "rb") as zip_file:
        files = {"client_files[0]": (os.path.basename(zip_path), zip_file, "application/zip")}
        data = {
            "image_quality": 100,
            "use_zip_chunks": "false",
            "use_cache": "false",
            "sorting_method": "lexicographical",
            "upload_format": "zip"
        }
        data_url = f"{CVAT_URL}/api/tasks/{task_id}/data?org={org_slug}"
        res = requests.post(data_url, headers=upload_headers, files=files, data=data)
        if res.status_code >= 400:
            _debug_http_error("Task data upload", res)
        res.raise_for_status()
        print(f"📦 Uploaded ZIP to task {task_id}")

    return task_id

def wait_until_task_ready(task_id, headers, org_slug, timeout=120):
    """프레임 인덱싱 완료까지 대기 (size>0)"""
    start = time.time()
    while time.time() - start < timeout:
        res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}?org={org_slug}", headers=headers)
        if res.status_code != 200:
            print(f"❌ Task 상태 확인 실패: {res.status_code}")
            break
        task_info = res.json()
        if task_info.get("size", 0) > 0:
            return True
        time.sleep(2)
    return False

def upload_annotations(task_id, json_path, headers, org_slug):
    """COCO 1.0 어노테이션 업로드 (정확한 org 전달)"""
    print(f"⏳ 어노테이션 업로드 시작: Task ID {task_id}")
    upload_headers = headers.copy()
    upload_headers.pop("Content-Type", None)
    with open(json_path, "rb") as jf:
        files = {"annotation_file": (json_path.name, jf, "application/json")}
        url = f"{CVAT_URL}/api/tasks/{task_id}/annotations"
        params = {
            "org": org_slug,
            "format": "COCO 1.0",
            "filename": json_path.name,
            "conv_mask_to_poly": "true"
        }
        res = requests.put(url, headers=upload_headers, files=files, params=params)

    if res.status_code in [200, 202]:
        print(f"✅ 어노테이션 업로드 성공: Task {task_id}")
    else:
        print(f"❌ 어노테이션 업로드 실패: {res.status_code}")
        print(res.text)
        return False
    return True

def refresh_and_check_counts(task_id, headers, org_slug):
    """업로드 직후 서버 측 요약값을 갱신/조회."""
    try:
        url_reload = f"{CVAT_URL}/api/tasks/{task_id}/annotations?action=reload&org={org_slug}"
        r = requests.post(url_reload, headers=headers)
        print("🔄 annotations reload:", r.status_code)
    except Exception as e:
        print("reload skip:", e)

    time.sleep(1.0)
    meta = requests.get(f"{CVAT_URL}/api/tasks/{task_id}?org={org_slug}", headers=headers)
    if meta.status_code == 200:
        j = meta.json()
        print(f"🧾 Task meta: size={j.get('size')} | segments={j.get('segments')}")
    else:
        _debug_http_error("Task meta refresh", meta)

def get_jobs(task_id, headers, org_slug):
    res = requests.get(f"{CVAT_URL}/api/jobs?task_id={task_id}&org={org_slug}", headers=headers)
    res.raise_for_status()
    return res.json().get("results", [])

def get_user_id(username: str, headers: dict, org_slug: str, page_size: int = 100):
    """특정 username 에 해당하는 user.id 반환 (끝 페이지까지 탐색)."""
    url = f"{CVAT_URL}/api/users?org={org_slug}&page_size={page_size}"
    while url:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        for user in data.get("results", []) or []:
            if user.get("username") == username:
                return user.get("id")
        url = data.get("next")
    return None

# ====== memberships 페이지네이션 & role=worker 필터 ======
def _iter_paginated(url: str, headers: dict):
    """CVAT API의 표준 페이지네이션(next 링크)을 따라가며 results를 yield."""
    while url:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json() or {}
        for item in data.get("results", []) or []:
            yield item
        url = data.get("next")

def get_all_memberships(headers: dict, org_slug: str, page_size: int = 100):
    """조직의 모든 membership을 끝 페이지까지 수집."""
    start_url = f"{CVAT_URL}/api/memberships?org={org_slug}&page_size={page_size}"
    return list(_iter_paginated(start_url, headers))

def get_worker_usernames(headers: dict, org_slug: str, page_size: int = 100) -> Set[str]:
    """
    조직에서 role이 'worker'인 사용자들의 username 집합을 반환.
    memberships 항목 예시: { "user": {...}, "role": "worker", ... }
    """
    workers = set()
    for m in get_all_memberships(headers, org_slug, page_size=page_size):
        role = (m or {}).get("role")
        u = (m or {}).get("user") or {}
        uname = u.get("username")
        if role == "worker" and uname:
            workers.add(uname)
    return workers

def filter_assignees_by_role_and_exclude(
    candidates: Iterable[str],
    worker_usernames: Set[str],
    exclude_users: Optional[Set[str]] = None,
) -> List[str]:
    """
    (candidates ∩ workers) − exclude_users 순서 유지 리스트 반환
    - 같은 username 중복은 1회만 사용
    """
    exclude_users = exclude_users or set()
    seen = set()
    filtered = []
    for a in candidates:
        if a in worker_usernames and a not in exclude_users and a not in seen:
            filtered.append(a)
            seen.add(a)
    return filtered

# ====== 라운드로빈 by job ======
def assign_jobs_round_robin(
    jobs: List[dict],
    headers: dict,
    assignees: List[str],
    org_slug: str,
) -> Dict[str, int]:
    """
    여러 명(assignees) 사이에서 'Job 단위'로 균등 분배(라운드로빈)
    반환값: {username: 할당된 job 개수}
    """
    if not assignees:
        print("⛔ 라운드로빈 대상자가 없습니다.")
        return {}

    # username → user_id 캐시
    id_cache: Dict[str, Optional[int]] = {}
    for name in assignees:
        uid = get_user_id(name, headers, org_slug)
        if not uid:
            print(f"❌ 사용자 '{name}'의 user_id를 찾지 못했습니다. 분배 대상에서 제외합니다.")
        id_cache[name] = uid
    assignees = [a for a in assignees if id_cache.get(a)]
    if not assignees:
        print("⛔ 유효한 user_id가 있는 라운드로빈 대상자가 없습니다.")
        return {}

    cyc = cycle(assignees)
    assigned_count: Dict[str, int] = {a: 0 for a in assignees}

    # 안정적 분배를 위해 job id 기준 정렬
    jobs_sorted = sorted(jobs, key=lambda j: j.get("id", 0))

    for job in jobs_sorted:
        if job.get("assignee"):
            continue
        assignee = next(cyc)
        user_id = id_cache[assignee]
        try:
            res = requests.patch(
                f"{CVAT_URL}/api/jobs/{job['id']}?org={org_slug}",
                headers=headers,
                json={"assignee": user_id}
            )
            res.raise_for_status()
            assigned_count[assignee] += 1
            print(f"✅ Job {job['id']} → '{assignee}' (누적 {assigned_count[assignee]})")
        except requests.HTTPError as e:
            print(f"⚠️ 할당 실패 (job {job['id']} → {assignee}): {e.response.status_code} - {e.response.text}")

    return assigned_count

def get_user_display_name(username):
    return os.getenv(f"USERMAP_{username}", username)

def log_assignment(task_name, task_id, assignee_name, num_jobs, project_name, organization):
    """사용자별 할당 결과를 한 줄씩 기록"""
    now_dt = datetime.now()
    now_str = now_dt.strftime("%Y/%m/%d %H:%M")
    log_columns = ["timestamp","organization","project","task_name","task_id","assignee","num_jobs"]
    display_name = get_user_display_name(assignee_name)

    log_entry_dict = {
        "timestamp": now_str,
        "organization": organization,
        "project": project_name,
        "task_name": task_name,
        "task_id": task_id,
        "assignee": display_name,
        "num_jobs": num_jobs
    }

    if not ASSIGN_LOG_PATH.exists():
        with open(ASSIGN_LOG_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=log_columns)
            writer.writeheader()
            writer.writerow(log_entry_dict)
        return
    
    with open(ASSIGN_LOG_PATH, "r", newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        rows = []
        for r in reader:
            completed_row = {col: r.get(col, "") for col in log_columns}
            rows.append(completed_row)

    def parse_timestamp(ts):
        for fmt in ("%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M"):
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
        print(f"[⚠️] 잘못된 날짜 포맷: {ts}")
        return datetime.min
    
    rows.append(log_entry_dict)
    rows.sort(key=lambda r: parse_timestamp(r["timestamp"]), reverse=True)

    with open(ASSIGN_LOG_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=log_columns)
        writer.writeheader()
        writer.writerows(rows)

# ====== YOLO / COCO 생성 ======
def run_yolo_on_image(model, img_path, image_id, annotation_id_start):
    start = time.time()
    img = Image.open(img_path)
    width, height = img.size
    image_entry = {
        "id": image_id,
        "file_name": img_path.name,
        "width": width,
        "height": height
    }
    results = model(img_path)
    annotations = []
    aid = annotation_id_start
    for r in results:
        for i, box in enumerate(r.boxes):
            cls_id = int(r.boxes.cls[i].item())
            cls_name = r.names[cls_id]
            if cls_name != "person":
                continue
            x1, y1, x2, y2 = map(float, box.xyxy[0])
            bbox = [x1, y1, x2 - x1, y2 - y1]
            area = bbox[2] * bbox[3]
            annotations.append({
                "id": aid,
                "image_id": image_id,
                "category_id": 1,
                "bbox": bbox,
                "area": area,
                "iscrowd": 0
            })
            aid += 1
    elapsed = time.time() - start
    print(f"🕒 {img_path.name} 추론 소요시간: {elapsed:.2f}초 (person {len(annotations)}개 감지)")
    return image_entry, annotations, aid

def run_yolo_and_create_json_parallel(images, output_json_path, model0, model1):
    coco = {
        "images": [],
        "annotations": [],
        "categories": [{"id": 1, "name": "person", "supercategory": "object"}]
    }
    def process(i_img_ann):
        i, (img_path, aid) = i_img_ann
        model = model0 if i % 2 == 0 else model1
        return run_yolo_on_image(model, img_path, i + 1, aid)
    start_all = time.time()
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(process, enumerate([(img, i * 1000 + 1) for i, img in enumerate(images)]))
    for image_entry, anns, _ in results:
        coco["images"].append(image_entry)
        coco["annotations"].extend(anns)
    with open(output_json_path, "w") as f:
        json.dump(coco, f, indent=2)
    print(f"✅ 전체 YOLO 추론 시간: {time.time() - start_all:.2f}초")

# ====== 메인 파이프라인 ======
def compress_and_upload_all(
    image_root_dir: Path,
    project_id,
    headers,
    project_name,
    organization="",
    batch_size=100,
    org_slug="",
    exclude_users: Optional[Set[str]] = None,
):
    """
    [기능 요약]
      - 이미지 폴더 트리 순회
      - (bboxes / keypoints 폴더가 있는 폴더는 스킵)
      - 배치 단위로 ZIP 생성 + YOLO(person) 감지 → COCO JSON 생성
      - CVAT Task 생성 → ZIP 업로드 → (프레임 인덱싱 대기) → COCO 1.0 어노테이션 업로드
      - 업로드 직후 서버 메타 리프레시/조회
      - 🔹 memberships에서 role='worker' 전체를 불러와 (제외 목록 제거 후) **라운드로빈 by job** 분배
      - 모든 단계 성공 시, 생성한 .json / .zip 파일 삭제
    """
    exclude_users = exclude_users or set()

    # --- 두 개의 YOLO 모델을 서로 다른 GPU에 올려 병렬 추론 ---
    model0 = YOLO("yolov8s.pt").to("cuda:0")
    model1 = YOLO("yolov8s.pt").to("cuda:1")

    # --- 1) role=worker 집합(한 번만 조회하여 캐시) ---
    worker_usernames = get_worker_usernames(headers, org_slug)
    if not worker_usernames:
        raise ValueError("⛔ 조직에 role='worker' 사용자가 없습니다.")

    # --- 2) 제외 목록 제거한 최종 후보 리스트(원본 순서의 의미가 없으므로 정렬로 고정성 부여) ---
    #     - 안정적 순서를 위해 알파벳 정렬; 라운드로빈은 이 순서를 기준으로 순환
    eligible_assignees = sorted([u for u in worker_usernames if u not in (exclude_users or set())])
    if not eligible_assignees:
        raise ValueError(
            f"⛔ 제외 목록을 적용하니 배분할 워커가 없습니다. exclude={sorted(exclude_users)}"
        )

    print(f"✅ GPU 사용 확인: {torch.cuda.get_device_name(0)}, {torch.cuda.get_device_name(1)}")
    print(f"👷 최종 대상(워커 & 제외반영): {eligible_assignees}")

    # --- 상위 image_root_dir 이하 모든 하위 폴더 순회 ---
    for group_dir in image_root_dir.rglob("*"):
        if not group_dir.is_dir():
            continue

        # 안전장치: 라벨 산출물 폴더 스킵
        if any((group_dir / skip_name).exists() for skip_name in ["bboxes", "keypoints"]):
            print(f"⏩ 스킵: {group_dir} (하위에 bboxes 또는 keypoints 폴더 존재)")
            continue

        # 이미지 파일만 수집
        image_files = sorted([
            f for f in group_dir.glob("*")
            if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".bmp"]
        ])
        if not image_files:
            continue

        # 배치 나누기
        num_batches = ceil(len(image_files) / batch_size)

        for i in range(num_batches):
            batch_files = image_files[i * batch_size : (i + 1) * batch_size]

            zip_filename = f"{group_dir.name}_{i+1:02d}.zip"
            json_filename = f"{group_dir.name}_{i+1:02d}.json"
            zip_path = group_dir / zip_filename
            json_path = group_dir / json_filename

            # 1) YOLO 감지 + COCO JSON 생성
            run_yolo_and_create_json_parallel(batch_files, json_path, model0, model1)

            # 2) ZIP 압축
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for img_path in batch_files:
                    zipf.write(img_path, arcname=img_path.name)
            print(f"[Batch] {zip_filename} created with {len(batch_files)} images")

            # 3) Task 생성 + ZIP 업로드
            task_name = zip_path.stem
            try:
                task_id = create_task_with_zip(task_name, project_id, zip_path, headers, org_slug=org_slug)
            except Exception as e:
                print(f"❌ Task 생성/ZIP 업로드 실패: {task_name} | 에러: {e}")
                continue

            # 4) 프레임 인덱싱 대기
            if not wait_until_task_ready(task_id, headers, org_slug):
                print(f"[CVAT] Task {task_name} 초기화 실패(프레임 인덱싱 미완료)")
                continue

            # 5) COCO 1.0 어노 업로드
            ok = upload_annotations(task_id, json_path, headers, org_slug)
            if not ok:
                print(f"[CVAT] Task {task_name} 어노 업로드 실패")
                continue

            # 6) 서버 메타 리프레시/조회
            refresh_and_check_counts(task_id, headers, org_slug)
            print(f"[CVAT] Task {task_name} 등록 및 어노테이션 완료")

            # 7) 🔹 작업자 라운드로빈 by job 분배 (모든 워커에게 균등 분배)
            try:
                jobs = get_jobs(task_id, headers, org_slug)
                counts = assign_jobs_round_robin(
                    jobs=jobs,
                    headers=headers,
                    assignees=eligible_assignees,
                    org_slug=org_slug,
                )
                # 사용자별 배분 결과를 로그에 기록 (여러 줄)
                for name, c in counts.items():
                    if c > 0:
                        log_assignment(
                            task_name, task_id, name, c,
                            project_name, organization
                        )
            except Exception as e:
                print(f"⚠️ 작업자 분배(라운드로빈) 중 오류 발생: {e}")

            # 8) 산출물(.json / .zip) 삭제
            try:
                if json_path.exists():
                    os.remove(json_path)
                    print(f"🗑️ Deleted JSON: {json_path}")
                if zip_path.exists():
                    os.remove(zip_path)
                    print(f"🗑️ Deleted ZIP: {zip_path}")
            except Exception as e:
                print(f"⚠️ 파일 삭제 중 오류 발생: {e}")

# ====== Entry Point ======
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLO 병렬 추론 + COCO JSON + CVAT 자동 업로드")
    parser.add_argument("--org_name", required=True)
    parser.add_argument("--image_dir", required=True)
    parser.add_argument("--project_name", required=True)
    parser.add_argument("--labels", type=str, nargs="+", required=True)
    # --assignees 제거!
    parser.add_argument("--exclude_users", type=str, nargs="*", default=[], help="할당에서 제외할 username 목록")
    args = parser.parse_args()
    
    ORGANIZATION = args.org_name

    # .env 의 ORGANIZATIONS 체크
    if args.org_name not in ORGANIZATIONS:
        raise ValueError(f"❌ 지정된 조직({args.org_name})이 .env의 조직 리스트에 없습니다: {ORGANIZATIONS}")
    
    image_dir = Path(args.image_dir)
    org_id, org_slug = get_or_create_organization(args.org_name)
    headers = build_headers(org_slug)

    # 인증/조직 컨텍스트 사전 확인
    preflight_check(headers, org_slug)

    # 프로젝트 생성(중복 라벨 방지)
    project_id = create_project(args.project_name, labels=args.labels, headers=headers, org_slug=org_slug)

    # 본 처리
    exclude_set = set(args.exclude_users or [])
    compress_and_upload_all(
        image_dir, project_id, headers,
        project_name=args.project_name,
        organization=ORGANIZATION, batch_size=100, org_slug=org_slug,
        exclude_users=exclude_set
    )
