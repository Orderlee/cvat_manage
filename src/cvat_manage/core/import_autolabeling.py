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

# ====== CVAT API ======
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
            "org": org_slug,          # ← 단일 slug 로 수정 (기존: 리스트였음)
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
    """
    작업자들이 'Job에선 보이는데 Task/Projects에선 안 보임' 혼선을 줄이기 위해
    업로드 직후 서버 측 요약값을 갱신/조회.
    - 일부 배포에선 어노 업로드 직후 카운트가 늦게 반영될 수 있어 요청으로 리프레시 시도
    """
    # (A) 가능한 경우: reload 액션 시도 (버전 의존적, 실패해도 무시)
    try:
        url_reload = f"{CVAT_URL}/api/tasks/{task_id}/annotations?action=reload&org={org_slug}"
        r = requests.post(url_reload, headers=headers)
        print("🔄 annotations reload:", r.status_code)
    except Exception as e:
        print("reload skip:", e)

    # (B) Task 메타 재조회
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

# def get_user_id(username, headers, org_slug):
#     res = requests.get(f"{CVAT_URL}/api/users?org={org_slug}", headers=headers)
#     res.raise_for_status()
#     for user in res.json().get("results", []):
#         if user["username"] == username:
#             return user["id"]
#     return None

def get_user_id(username: str, headers: dict, org_slug: str, page_size: int = 100):
    """
    특정 username 에 해당하는 user.id 반환
    - 조직 내 모든 페이지를 순회하며 검색
    - 없으면 None 반환
    """
    url = f"{CVAT_URL}/api/users?org={org_slug}&page_size={page_size}"

    while url:
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()

        # 현재 페이지에서 username 매칭 검사
        for user in data.get("results", []) or []:
            if user.get("username") == username:
                return user.get("id")

        # 다음 페이지로 이동
        url = data.get("next")

    # 끝까지 못 찾으면 None
    return None


def assign_jobs_to_one_user(jobs, headers, assignee_name, org_slug):
    user_id = get_user_id(assignee_name, headers, org_slug)
    if not user_id:
        print(f"❌ 사용자 '{assignee_name}'를 찾을 수 없습니다.")
        return
    for job in jobs:
        if job.get("assignee"): 
            continue
        try:
            res = requests.patch(
                f"{CVAT_URL}/api/jobs/{job['id']}?org={org_slug}",
                headers=headers,
                json={"assignee": user_id}
            )
            res.raise_for_status()
            print(f"✅ Job {job['id']} → '{assignee_name}' 할당 완료")
        except requests.HTTPError as e:
            print(f"⚠️ 할당 실패: {e.response.status_code} - {e.response.text}")

def get_user_display_name(username):
    return os.getenv(f"USERMAP_{username}", username)

def log_assignment(task_name, task_id, assignee_name, num_jobs, project_name, organization):
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

# ====== Main flow ======
# def compress_and_upload_all(image_root_dir: Path, project_id, headers, assignees, project_name, organization="", batch_size=100, org_slug=""):
#     # 모델 두 개로 병렬 추론
#     model0 = YOLO("yolov8s.pt").to("cuda:0")
#     model1 = YOLO("yolov8s.pt").to("cuda:1")
#     assignee_cycle = cycle(assignees)
#     print(f"✅ GPU 사용 확인: {torch.cuda.get_device_name(0)}, {torch.cuda.get_device_name(1)}")
    
#     for group_dir in image_root_dir.rglob("*"):
#         if not group_dir.is_dir():
#             continue

#         # 하위에 bboxes / keypoints 폴더가 있으면 스킵
#         if any((group_dir / skip_name).exists() for skip_name in ["bboxes", "keypoints"]):
#             print(f"⏩ 스킵: {group_dir} (하위에 bboxes 또는 keypoints 폴더 존재)")
#             continue
        
#         image_files = sorted([f for f in group_dir.glob("*") if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".bmp"]])
#         if not image_files:
#             continue
        
#         num_batches = ceil(len(image_files) / batch_size)
#         for i in range(num_batches):
#             batch_files = image_files[i * batch_size : (i + 1) * batch_size]
#             zip_filename = f"{group_dir.name}_{i+1:02d}.zip"
#             json_filename = f"{group_dir.name}_{i+1:02d}.json"
#             zip_path = group_dir / zip_filename
#             json_path = group_dir / json_filename
            
#             run_yolo_and_create_json_parallel(batch_files, json_path, model0, model1)
            
#             with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
#                 for img_path in batch_files:
#                     zipf.write(img_path, arcname=img_path.name)
#             print(f"[Batch] {zip_filename} created with {len(batch_files)} images")
            
#             task_name = zip_path.stem
#             task_id = create_task_with_zip(task_name, project_id, zip_path, headers, org_slug=org_slug)
#             if wait_until_task_ready(task_id, headers, org_slug):
#                 ok = upload_annotations(task_id, json_path, headers, org_slug)
#                 if not ok:
#                     print(f"[CVAT] Task {task_name} 어노 업로드 실패")
#                     continue
#                 # 업로드 후 요약값 갱신/확인
#                 refresh_and_check_counts(task_id, headers, org_slug)
#                 print(f"[CVAT] Task {task_name} 등록 및 어노테이션 완료")
#             else:
#                 print(f"[CVAT] Task {task_name} 초기화 실패")
#                 continue
            
#             jobs = get_jobs(task_id, headers, org_slug)
#             assignee_name = next(assignee_cycle)
#             if assignee_name:
#                 assign_jobs_to_one_user(jobs, headers, assignee_name, org_slug)
#                 log_assignment(task_name, task_id, assignee_name, len(jobs), project_name, organization)

def compress_and_upload_all(
    image_root_dir: Path,
    project_id,
    headers,
    assignees,
    project_name,
    organization="",
    batch_size=100,
    org_slug=""
):
    """
    [기능 요약]
      - 이미지 폴더 트리 순회
      - (bboxes / keypoints 폴더가 있는 폴더는 스킵)  <-- 안전장치
      - 배치 단위로 ZIP 생성 + YOLO(person) 감지 → COCO JSON 생성
      - CVAT Task 생성 → ZIP 업로드 → (프레임 인덱싱 대기) → COCO 1.0 어노테이션 업로드
      - 업로드 직후 서버 메타 리프레시/조회
      - 작업(Job) 목록 조회 후 라운드로빈 방식으로 작업자 할당
      - 🎯 모든 단계 성공 시, 생성한 .json / .zip 파일 삭제

    [삭제 정책]
      - 어노테이션 업로드까지 성공했을 때만 .json/.zip 삭제
      - 실패 시에는 디버깅을 위해 파일을 남김
    """
    # --- 두 개의 YOLO 모델을 서로 다른 GPU에 올려 병렬 추론 (짝/홀 인덱스 분배) ---
    model0 = YOLO("yolov8s.pt").to("cuda:0")
    model1 = YOLO("yolov8s.pt").to("cuda:1")

    # assignees 리스트를 라운드로빈으로 순환
    assignee_cycle = cycle(assignees)

    # GPU 정보 출력(디버깅용)
    print(f"✅ GPU 사용 확인: {torch.cuda.get_device_name(0)}, {torch.cuda.get_device_name(1)}")

    # --- 상위 image_root_dir 이하 모든 하위 폴더 순회 ---
    for group_dir in image_root_dir.rglob("*"):
        # 파일은 건너뛰고, 폴더만 처리
        if not group_dir.is_dir():
            continue

        # ⛔ 안전장치: 하위에 'bboxes' 또는 'keypoints' 폴더가 있으면 스킵
        if any((group_dir / skip_name).exists() for skip_name in ["bboxes", "keypoints"]):
            print(f"⏩ 스킵: {group_dir} (하위에 bboxes 또는 keypoints 폴더 존재)")
            continue

        # 이미지 파일만 수집
        image_files = sorted([
            f for f in group_dir.glob("*")
            if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".bmp"]
        ])
        if not image_files:
            continue  # 이 폴더에 이미지가 없으면 다음 폴더로

        # --- 배치 나누기 ---
        num_batches = ceil(len(image_files) / batch_size)

        for i in range(num_batches):
            # 현재 배치의 파일들
            batch_files = image_files[i * batch_size : (i + 1) * batch_size]

            # 배치 단위 산출물 파일명 (예: 폴더명_01.zip / 폴더명_01.json)
            zip_filename = f"{group_dir.name}_{i+1:02d}.zip"
            json_filename = f"{group_dir.name}_{i+1:02d}.json"
            zip_path = group_dir / zip_filename
            json_path = group_dir / json_filename

            # --- 1) YOLO 감지 + COCO JSON 생성(두 모델을 번갈아 사용하여 병렬처리) ---
            run_yolo_and_create_json_parallel(batch_files, json_path, model0, model1)

            # --- 2) ZIP 압축 생성 ---
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for img_path in batch_files:
                    # ZIP 내부에는 파일명만 넣어 경로 단순화(arcname=파일명)
                    zipf.write(img_path, arcname=img_path.name)
            print(f"[Batch] {zip_filename} created with {len(batch_files)} images")

            # --- 3) CVAT Task 생성 + ZIP 업로드 ---
            task_name = zip_path.stem  # 확장자 제외(= ZIP 이름)
            try:
                task_id = create_task_with_zip(
                    task_name, project_id, zip_path, headers, org_slug=org_slug
                )
            except Exception as e:
                print(f"❌ Task 생성/ZIP 업로드 실패: {task_name} | 에러: {e}")
                # 업로드 실패 → 디버깅을 위해 파일 보존
                continue

            # --- 4) Task 준비(프레임 인덱싱) 대기 ---
            if not wait_until_task_ready(task_id, headers, org_slug):
                print(f"[CVAT] Task {task_name} 초기화 실패(프레임 인덱싱 미완료)")
                # 디버깅을 위해 파일 보존
                continue

            # --- 5) COCO 1.0 어노테이션 업로드 ---
            ok = upload_annotations(task_id, json_path, headers, org_slug)
            if not ok:
                print(f"[CVAT] Task {task_name} 어노 업로드 실패")
                # 디버깅을 위해 파일 보존
                continue

            # --- 6) 서버 메타 리프레시/조회(요약값/카운트 반영 확인용) ---
            refresh_and_check_counts(task_id, headers, org_slug)
            print(f"[CVAT] Task {task_name} 등록 및 어노테이션 완료")

            # --- 7) 작업자(Job) 할당: 라운드로빈으로 한 명씩 ---
            try:
                jobs = get_jobs(task_id, headers, org_slug)
                assignee_name = next(assignee_cycle)
                if assignee_name:
                    assign_jobs_to_one_user(jobs, headers, assignee_name, org_slug)
                    log_assignment(
                        task_name, task_id, assignee_name, len(jobs),
                        project_name, organization
                    )
            except Exception as e:
                # 할당 단계에서 문제가 생겨도, 그 이전 단계(업로드/어노)는 성공했으므로
                # 파일 삭제는 진행해도 무방합니다. 다만 필요 시 보존하고 싶다면 여기서 return/continue 로 바꿔도 됩니다.
                print(f"⚠️ 작업자 할당 중 오류 발생: {e}")

            # --- 8) ✅ 모든 핵심 단계 성공 시 산출물(.json / .zip) 삭제 ---
            try:
                if json_path.exists():
                    os.remove(json_path)
                    print(f"🗑️ Deleted JSON: {json_path}")
                if zip_path.exists():
                    os.remove(zip_path)
                    print(f"🗑️ Deleted ZIP: {zip_path}")
            except Exception as e:
                # 삭제 자체는 필수 단계가 아니므로 경고만 남김
                print(f"⚠️ 파일 삭제 중 오류 발생: {e}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLO 병렬 추론 + COCO JSON + CVAT 자동 업로드")
    parser.add_argument("--org_name", required=True)
    parser.add_argument("--image_dir", required=True)
    parser.add_argument("--project_name", required=True)
    parser.add_argument("--labels", type=str, nargs="+", required=True)
    parser.add_argument("--assignees", type=str, nargs="+", required=True)
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
    compress_and_upload_all(
        image_dir, project_id, headers,
        assignees=args.assignees, project_name=args.project_name,
        organization=ORGANIZATION, batch_size=100, org_slug=org_slug
    )
