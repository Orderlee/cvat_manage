# import os
# import csv
# import requests
# import subprocess
# import zipfile
# from datetime import datetime
# from dotenv import load_dotenv
# from pathlib import Path

# # === 환경 변수 로드 ===
# env_path = Path(__file__).resolve().parent.parent / ".env"
# load_dotenv(dotenv_path=env_path)

# CVAT_URL = os.getenv("CVAT_URL_2")
# TOKEN = os.getenv("TOKEN_2")
# CVAT_USERNAME = os.getenv("CVAT_USERNAME")
# CVAT_PASSWORD = os.getenv("CVAT_PASSWORD")
# CVAT_EXPORT_FORMAT = os.getenv("CVAT_EXPORT_FORMAT")
# CVAT_EXPORT_FORMAT_4 = os.getenv("CVAT_EXPORT_FORMAT_4")  # skeleton 용 포맷
# WITH_IMAGES = os.getenv("WITH_IMAGES")
# ORGANIZATIONS = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",")]
# RESULT_DIR = os.getenv("RESULT_DIR", "/tmp/cvat_exports")

# HEADERS = {
#     "Authorization": f"Token {TOKEN}",
#     "Content-Type": "application/json"
# }

# # === JSON만 포함된 zip으로 덮어쓰기 ===
# def extract_json_and_only_json(zip_path: Path):
#     with zipfile.ZipFile(zip_path, "r") as zip_ref:
#         json_files = [f for f in zip_ref.namelist() if f.endswith(".json") and f.startswith("annotations/")]
#         if not json_files:
#             print(f"⚠️ JSON 파일 없음: {zip_path}")
#             return

#         json_internal_path = json_files[0]
#         json_data = zip_ref.read(json_internal_path)
#         json_filename = zip_path.stem + ".json"

#     with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as new_zip:
#         new_zip.writestr(json_filename, json_data)

#     print(f"📦 JSON만 포함된 zip으로 덮어쓰기 완료: {zip_path.name}")

# # === 유틸 함수 ===
# def get_all_jobs():
#     jobs = []
#     page = 1
#     while True:
#         r = requests.get(f"{CVAT_URL}/api/jobs?page={page}", headers=HEADERS)
#         r.raise_for_status()
#         data = r.json()
#         jobs.extend(data["results"])
#         if not data["next"]:
#             break
#         page += 1
#     return jobs

# def get_task_info(task_id):
#     r = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=HEADERS)
#     r.raise_for_status()
#     return r.json()

# def get_annotations(job_id):
#     r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
#     r.raise_for_status()
#     return r.json()

# def get_organization_name(org_id):
#     if not org_id:
#         return "(None)"
#     r = requests.get(f"{CVAT_URL}/api/organizations/{org_id}", headers=HEADERS)
#     if r.status_code == 404:
#         return "(Not found)"
#     r.raise_for_status()
#     return r.json().get("slug", f"(org-{org_id})")

# def load_assignee_map_from_env():
#     assignee_map = {}
#     for key, value in os.environ.items():
#         if key.startswith("USERMAP_"):
#             username = key.replace("USERMAP_", "")
#             assignee_map[username] = value
#     return assignee_map

# def get_label_types_from_annotations(job_id):
#     label_types = set()
#     try:
#         annotations = get_annotations(job_id)
#         for shape in annotations.get("shapes", []):
#             shape_type = shape.get("shape_type") or shape.get("type")
#             if shape_type:
#                 label_types.add(shape_type)
#     except requests.RequestException as e:
#         print(f"⚠️ 어노테이션 정보 가져오기 실패 (job_id={job_id}): {e}")
#         return set()

#     print(f"📌 Job ID {job_id} → 사용된 라벨 타입: {label_types}")
#     return label_types

# def run_cvat_cli_export(task_id: int, task_name: str, assignee: str, result_dir: Path, export_log_path: Path, assignee_map: dict, export_format: str, log_name_override: str = None):
#     safe_name = task_name#.replace(" ", "-")
#     exported_date = datetime.today().strftime("%Y-%m-%d")


#     # export_format 값에 따라 결정
#     if export_format == CVAT_EXPORT_FORMAT:
#         suffix = "_boundingbox"
#     elif export_format == CVAT_EXPORT_FORMAT_4:
#         suffix = "_keypoint"
#     else:
#         suffix = ""
    
#     output_path = result_dir / f"{safe_name}{suffix}.zip"

#     command = (
#         f'cvat-cli --server-host {CVAT_URL} '
#         f'--auth {CVAT_USERNAME}:{CVAT_PASSWORD} '
#         f'task export-dataset {task_id} "{output_path}" '
#         f'--format "{export_format}" '
#         f'--with-images {WITH_IMAGES}'
#     )

#     try:
#         print(f"🚀 Exporting: Task {task_id} → {output_path.name}")
#         subprocess.run(command, shell=True, check=True)
#         print(f"✅ Export 성공 → {output_path}")

#         mapped_assignee = assignee_map.get(assignee, assignee)

#         # 로그에 기록할 이름을 직접 지정 (override 없으면 원래 task_name)
#         log_name = log_name_override if log_name_override else task_name

#         with open(export_log_path, "a", newline='') as f:
#             writer = csv.writer(f)
#             writer.writerow([task_id, log_name, mapped_assignee, exported_date])

#         extract_json_and_only_json(output_path)

#     except subprocess.CalledProcessError as e:
#         print(f"❌ Export 실패: Task {task_id} - {e}")

# # === 메인 ===
# def main():
#     jobs = get_all_jobs()

#     today_str = datetime.today().strftime("%Y-%m-%d")
    
#     base_result_dir = Path(RESULT_DIR)

#     export_log_path = Path("/home/pia/work_p/dfn/omission/result/export_log.csv")

#     if not export_log_path.exists():
#         with open(export_log_path, "w", newline='') as f:
#             writer = csv.writer(f)
#             writer.writerow(["task_id", "task_name", "assignee", "exported_date"])

#     exported_task_ids = set()
#     with open(export_log_path, "r", newline='') as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             exported_task_ids.add(row["task_id"])

#     assignee_map = load_assignee_map_from_env()

#     for job in jobs:
#         task_id = str(job["task_id"])
#         state = job.get("state")
#         stage = job.get("stage")

#         if not (stage == "acceptance" and state == "completed"):
#             continue

#         if task_id in exported_task_ids:
#             print(f"⏩ 이미 export됨 → Task {task_id}, 건너뜀")
#             continue

#         task_info = get_task_info(int(task_id))
#         task_name = task_info.get("name", f"task_{task_id}")
#         org_slug = get_organization_name(task_info.get("organization"))

#         if org_slug not in ORGANIZATIONS:
#             continue

        
#         assignee_info = job.get("assignee")
#         assignee = assignee_info.get("username", "(unassigned)") if assignee_info else "(unassigned)"

#         label_types = get_label_types_from_annotations(int(job["id"]))

#         result_dir = base_result_dir / today_str / org_slug / task_name#.replace(" ", "_")
#         result_dir.mkdir(parents=True, exist_ok=True)

#         exported = False

#         if label_types == {"rectangle"}:
#             run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, CVAT_EXPORT_FORMAT)
#             exported = True
#         elif label_types == {"skeleton"}:
#             run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, os.getenv("CVAT_EXPORT_FORMAT_4"), 
#                                 log_name_override=task_name + "_k")
#             exported = True
#         elif {"rectangle", "skeleton"}.issubset(label_types):
#             run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, CVAT_EXPORT_FORMAT,
#                                 log_name_override=task_name)
#             run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, os.getenv("CVAT_EXPORT_FORMAT_4"),
#                                 log_name_override=task_name + "_k")

# if __name__ == "__main__":
#     main()

"""
export.py — CVAT 멀티 조직 Export (slug/id 자동 폴백, 403/406 안전화 - org 맵 호출 제거판)
- 목적: thailabeling, vietnamlabeling, piaspace 등 여러 조직을 한 번에 처리
- 핵심 변경 (406 대응):
  1) 더 이상 `/api/organizations` 로 조직 맵을 조회하지 않습니다. (일부 서버에서 406 발생)
  2) .env에서 받은 조직 슬러그만으로 바로 호출하고, 가능하면 선택적으로 제공한 org_id도 함께 사용합니다.
  3) 모든 GET은 `Accept`만 설정하고 `Content-Type`은 제거해 406을 피합니다. (기존 코드들은 GET에 Content-Type을 넣고 있었습니다)
  4) 모든 요청에 헤더(X-Organization, X-Organization-ID)와 쿼리(org, org_id)를 조합해 3단계 자동 폴백.

환경변수(.env) 예시:
  CVAT_URL_2=http://34.64.195.111:8080
  TOKEN_2=xxxxx
  CVAT_USERNAME=your_id
  CVAT_PASSWORD=your_pw
  CVAT_EXPORT_FORMAT=COCO              # bbox 등
  CVAT_EXPORT_FORMAT_4=COCO Keypoint   # skeleton 등
  WITH_IMAGES=false
  RESULT_DIR=/tmp/cvat_exports
  ORGANIZATIONS=thailabeling,vietnamlabeling,piaspace   # 여러 조직 루프 실행
  # 단일 조직만 실행하고 싶을 때: CVAT_ORG_SLUG=piaspace (설정 시 ORGANIZATIONS 무시)
  # (선택) org_id 매핑이 필요한 서버의 경우:
  CVAT_ORG_ID_MAP=thailabeling:12,vietnamlabeling:13,piaspace:14
"""

import os
import csv
import requests
import subprocess
import zipfile
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Dict, Any, List, Set

# =========================
# 0) 환경 변수 로드
# =========================
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = (os.getenv("CVAT_URL_2") or "").rstrip("/")
TOKEN = os.getenv("TOKEN_2", "")
CVAT_USERNAME = os.getenv("CVAT_USERNAME", "")
CVAT_PASSWORD = os.getenv("CVAT_PASSWORD", "")

# Export 포맷
CVAT_EXPORT_FORMAT = os.getenv("CVAT_EXPORT_FORMAT", "")
CVAT_EXPORT_FORMAT_4 = os.getenv("CVAT_EXPORT_FORMAT_4", "")  # skeleton 용
WITH_IMAGES = os.getenv("WITH_IMAGES", "false")

# 멀티 조직: 쉼표로 구분된 슬러그 목록 (예: thailabeling,vietnamlabeling,piaspace)
ORGANIZATIONS: List[str] = [
    org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()
]

# (선택) 단일 조직 강제 실행용 — 설정되어 있으면 이 값 하나만 사용
CVAT_ORG_SLUG = (os.getenv("CVAT_ORG_SLUG") or "").strip()

# (선택) org_id 매핑 (예: thailabeling:12,vietnamlabeling:13,piaspace:14)
_RAW_MAP = os.getenv("CVAT_ORG_ID_MAP", "").strip()
CVAT_ORG_ID_MAP: Dict[str, int] = {}
if _RAW_MAP:
    for pair in _RAW_MAP.split(","):
        if ":" in pair:
            slug, sid = pair.split(":", 1)
            slug = slug.strip()
            try:
                CVAT_ORG_ID_MAP[slug] = int(sid.strip())
            except ValueError:
                pass

RESULT_DIR = os.getenv("RESULT_DIR", "/tmp/cvat_exports")

# =========================
# 1) 조직/요청 공통 유틸 (폴백 지원)
# =========================

def build_session(base_headers: Dict[str, str]) -> requests.Session:
    """Authorization/Accept 등을 포함한 Session 생성."""
    sess = requests.Session()
    sess.headers.update(base_headers)
    return sess


def make_base_headers(org_slug: str = "", org_id: Optional[int] = None, accept_variant: int = 0) -> Dict[str, str]:
    """
    GET 기본 헤더 구성 (Accept 네고 지원)
    - accept_variant:
        0: (권장) **Accept 헤더를 아예 넣지 않음** → 일부 프록시의 406 회피
        1: Accept: */*
        2: Accept: application/json
    - GET에는 Content-Type을 넣지 않는다(일부 서버에서 406 방지)
    - 조직 헤더는 slug와 id를 붙여 호환성 확보
    """
    headers = {
        "Authorization": f"Token {TOKEN}",
    }
    if accept_variant == 1:
        headers["Accept"] = "*/*"
    elif accept_variant == 2:
        headers["Accept"] = "application/json"

    if org_slug:
        headers["X-Organization"] = org_slug
    if org_id is not None:
        headers["X-Organization-ID"] = str(org_id)
    return headers


def with_org_params(params: Optional[Dict[str, Any]], org_slug: str = "", org_id: Optional[int] = None) -> Dict[str, Any]:
    """
    쿼리에도 org, org_id를 동시에 부착 (중복 무해, 호환성↑)
    """
    params = dict(params or {})
    if org_slug:
        params.setdefault("org", org_slug)
    if org_id is not None:
        params.setdefault("org_id", org_id)
    return params


def get_json_with_fallback(
    path: str,
    org_slug: str,
    org_id: Optional[int],
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    """
    폴백 로직으로 조직 컨텍스트/Accept 호환성 보장:
      A) Accept 네고: [ no Accept header(0) → */*(1) → application/json(2) ]
      B) 조직 네고:   [ header(org+id)+query(org+id) → header(id)+query(id) → header(org)+query(org) ]
    총 3 x 3 조합으로 재시도. 최종 실패 시 마지막 응답과 함께 에러.
    """
    if not CVAT_URL:
        raise RuntimeError("환경변수 CVAT_URL_2가 설정되지 않았습니다.")

    url = f"{CVAT_URL}{path}"

    # (use_slug, use_id)
    org_attempts = [(True, True), (False, True), (True, False)]
    # 0: Accept 헤더 없음, 1: */*, 2: application/json
    accept_attempts = [0, 1, 2]

    last_status: Optional[int] = None
    last_text: Optional[str] = None

    for accept_variant in accept_attempts:
        for use_slug, use_id in org_attempts:
            # 헤더/파라미터 조합 구성
            hdr = make_base_headers(
                org_slug if use_slug else "",
                org_id if (use_id and org_id is not None) else None,
                accept_variant=accept_variant,
            )
            sess = build_session(hdr)
            prms = with_org_params(
                params,
                org_slug if use_slug else "",
                org_id if (use_id and org_id is not None) else None,
            )

            # 호출
            resp = sess.get(url, params=prms, timeout=timeout)

            # 성공
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    # JSON 디코드 실패 시 본문 일부를 포함해 에러
                    snippet = resp.text[:300] if resp.text else ""
                    raise requests.HTTPError(
                        f"JSON 파싱 실패: {url}\n본문: {snippet}"
                    )

            # 실패 → 다음 조합으로 폴백
            last_status = resp.status_code
            last_text = (resp.text or "")[:300]

    # 모든 조합 실패
    raise requests.HTTPError(
        f"조직/Accept 조합으로도 실패: {url} (org={org_slug}, org_id={org_id})\n"
        f"마지막 응답: {last_status} {last_text}"
    )



# =========================
# 2) CVAT API 헬퍼
# =========================

def get_all_jobs_for_org(org_slug: str, org_id: Optional[int]) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    page = 1
    while True:
        data = get_json_with_fallback("/api/jobs", org_slug, org_id, params={"page": page})
        jobs.extend(data.get("results", []))
        if not data.get("next"):
            break
        page += 1
    return jobs


def get_task_info_for_org(task_id: int, org_slug: str, org_id: Optional[int]) -> Dict[str, Any]:
    return get_json_with_fallback(f"/api/tasks/{task_id}", org_slug, org_id)


def get_annotations_for_org(job_id: int, org_slug: str, org_id: Optional[int]) -> Dict[str, Any]:
    return get_json_with_fallback(f"/api/jobs/{job_id}/annotations", org_slug, org_id)


# =========================
# 3) cvat-cli export 래퍼 & 후처리
# =========================

def extract_json_only(zip_path: Path):
    """export zip에서 annotations/*.json 하나만 남기는 후처리"""
    with zipfile.ZipFile(zip_path, "r") as zf:
        json_files = [f for f in zf.namelist() if f.endswith(".json") and f.startswith("annotations/")]
        if not json_files:
            print(f"⚠️ JSON 파일 없음: {zip_path}")
            return
        json_internal = json_files[0]
        json_bytes = zf.read(json_internal)
        json_name = zip_path.stem + ".json"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as new_zip:
        new_zip.writestr(json_name, json_bytes)

    print(f"📦 JSON만 포함된 zip으로 재작성: {zip_path.name}")


def run_cvat_cli_export(task_id: int, task_name: str, assignee: str, result_dir: Path,
                        export_log_path: Path, assignee_map: Dict[str, str],
                        export_format: str, with_images: str, log_name_override: str = None):
    """
    cvat-cli를 호출해 dataset export. (서버/계정은 환경변수 사용)
    - 참고: cvat-cli에 조직 옵션이 별도로 있다면 추가해야 하지만, 보통 계정 로그인 컨텍스트에 따릅니다.
    """
    safe_name = task_name  # 필요시 파일명 치환 추가
    exported_date = datetime.today().strftime("%Y-%m-%d")

    # 포맷에 따른 파일명 접미사
    if export_format == CVAT_EXPORT_FORMAT:
        suffix = "_boundingbox"
    elif export_format == CVAT_EXPORT_FORMAT_4:
        suffix = "_keypoint"
    else:
        suffix = ""

    output_path = result_dir / f"{safe_name}{suffix}.zip"

    cmd = (
        f'cvat-cli --server-host {CVAT_URL} '
        f'--auth {CVAT_USERNAME}:{CVAT_PASSWORD} '
        f'task export-dataset {task_id} "{output_path}" '
        f'--format "{export_format}" '
        f'--with-images {with_images}'
    )

    try:
        print(f"🚀 Exporting: Task {task_id} → {output_path.name}")
        subprocess.run(cmd, shell=True, check=True)
        print(f"✅ Export 성공: {output_path}")

        mapped = assignee_map.get(assignee, assignee)
        log_name = log_name_override if log_name_override else task_name
        with open(export_log_path, "a", newline="") as f:
            csv.writer(f).writerow([task_id, log_name, mapped, exported_date])

        extract_json_only(output_path)
    except subprocess.CalledProcessError as e:
        print(f"❌ Export 실패: Task {task_id} - {e}")


# =========================
# 4) 메인: 조직 루프
# =========================

def load_assignee_map_from_env() -> Dict[str, str]:
    m: Dict[str, str] = {}
    for k, v in os.environ.items():
        if k.startswith("USERMAP_"):
            m[k.replace("USERMAP_", "")] = v
    return m


def main():
    # 0) 실행 조직 목록 결정
    if CVAT_ORG_SLUG:
        # ⚠️ CVAT_ORG_SLUG는 단일 값이어야 합니다. 콤마로 여러 개를 넣지 마세요.
        if "," in CVAT_ORG_SLUG:
            raise RuntimeError("CVAT_ORG_SLUG에는 하나의 슬러그만 설정하세요. 여러 조직은 ORGANIZATIONS를 사용합니다.")
        org_list = [CVAT_ORG_SLUG]  # 단일 조직 강제 실행 모드
    else:
        org_list = ORGANIZATIONS     # 다중 조직 실행 모드

    if not org_list:
        raise RuntimeError("실행할 조직이 없습니다. CVAT_ORG_SLUG 또는 ORGANIZATIONS를 설정하세요.")

    # 1) export 로그 준비
    export_log_path = Path("/home/pia/work_p/dfn/omission/result/export_log.csv")
    export_log_path.parent.mkdir(parents=True, exist_ok=True)
    if not export_log_path.exists():
        with open(export_log_path, "w", newline="") as f:
            csv.writer(f).writerow(["task_id", "task_name", "assignee", "exported_date"])

    exported: Set[str] = set()
    with open(export_log_path, "r", newline="") as f:
        for row in csv.DictReader(f):
            exported.add(row["task_id"])  # 이미 export된 Task는 스킵

    assignee_map = load_assignee_map_from_env()
    base_result_dir = Path(RESULT_DIR)
    today = datetime.today().strftime("%Y-%m-%d")

    # 2) 조직별 실행 루프
    for org_slug in org_list:
        # (선택) org_id 매핑이 있으면 사용
        org_id = CVAT_ORG_ID_MAP.get(org_slug)

        print("==============================")
        print(f"🏢 조직 컨텍스트 시작: {org_slug}")
        print("==============================")

        # (A) 이 조직에서 보이는 Job만 조회
        try:
            jobs = get_all_jobs_for_org(org_slug, org_id)
        except requests.RequestException as e:
            print(f"❌ /api/jobs 조회 실패 (org={org_slug}): {e}")
            continue

        # (B) Job 순회
        for job in jobs:
            task_id = str(job.get("task_id"))
            job_id = int(job.get("id"))
            stage = job.get("stage")
            state = job.get("state")

            # 완료된 acceptance만 대상
            if not (stage == "acceptance" and state == "completed"):
                continue

            # 이미 export 한 Task 스킵
            if task_id in exported:
                print(f"⏩ 이미 export됨 → Task {task_id}, 건너뜀")
                continue

            # Task 상세 조회 (조직 컨텍스트가 맞으므로 403/406 확률 낮음)
            try:
                task_info = get_task_info_for_org(int(task_id), org_slug, org_id)
            except requests.RequestException as e:
                print(f"⚠️ Task 상세 조회 실패 (ID={task_id}, org={org_slug}): {e}")
                continue

            task_name = task_info.get("name", f"task_{task_id}")

            # 로그용 담당자
            assignee_info = job.get("assignee")
            assignee = assignee_info.get("username", "(unassigned)") if assignee_info else "(unassigned)"

            # 라벨 타입 결정
            label_types = set()
            try:
                anns = get_annotations_for_org(job_id, org_slug, org_id)
                for shape in anns.get("shapes", []):
                    t = shape.get("shape_type") or shape.get("type")
                    if t:
                        label_types.add(t)
            except requests.RequestException as e:
                print(f"⚠️ 어노테이션 정보 실패 (job_id={job_id}, org={org_slug}): {e}")
                label_types = set()

            # 결과 폴더: /RESULT_DIR/날짜/조직/태스크명
            result_dir = base_result_dir / today / org_slug / task_name
            result_dir.mkdir(parents=True, exist_ok=True)

            # Export 분기
            if label_types == {"rectangle"}:
                run_cvat_cli_export(
                    int(task_id), task_name, assignee, result_dir,
                    export_log_path, assignee_map, CVAT_EXPORT_FORMAT, WITH_IMAGES
                )
            elif label_types == {"skeleton"}:
                run_cvat_cli_export(
                    int(task_id), task_name, assignee, result_dir,
                    export_log_path, assignee_map, CVAT_EXPORT_FORMAT_4, WITH_IMAGES,
                    log_name_override=task_name + "_k"
                )
            elif {"rectangle", "skeleton"}.issubset(label_types):
                # 혼합이면 두 번
                run_cvat_cli_export(
                    int(task_id), task_name, assignee, result_dir,
                    export_log_path, assignee_map, CVAT_EXPORT_FORMAT, WITH_IMAGES,
                    log_name_override=task_name
                )
                run_cvat_cli_export(
                    int(task_id), task_name, assignee, result_dir,
                    export_log_path, assignee_map, CVAT_EXPORT_FORMAT_4, WITH_IMAGES,
                    log_name_override=task_name + "_k"
                )
            else:
                print(f"ℹ️ Task {task_id}({task_name}) → 지원 외 라벨 타입 {label_types}, 스킵")


if __name__ == "__main__":
    main()
