import os
import csv
import requests
import subprocess
import zipfile
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# === 환경 변수 로드 ===
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
CVAT_USERNAME = os.getenv("CVAT_USERNAME")
CVAT_PASSWORD = os.getenv("CVAT_PASSWORD")
CVAT_EXPORT_FORMAT = os.getenv("CVAT_EXPORT_FORMAT")
CVAT_EXPORT_FORMAT_4 = os.getenv("CVAT_EXPORT_FORMAT_4")  # skeleton 용 포맷
WITH_IMAGES = os.getenv("WITH_IMAGES")
# 주의: 사용자가 제공한 키를 그대로 사용
ORGANIZATIONS = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",")]  
RESULT_DIR = os.getenv("RESULT_DIR", "/tmp/cvat_exports")

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json"
}

# === 특정 프로젝트 강제 export 모드 트리거 ===
# 아래 문자열이 task_name / project_name / org_slug 중 하나에 (대소문자 무시) 포함되면
# export_log.csv 중복 체크를 무시하고 강제 export 수행
PROJECT_NAME = ""   # 비활성화하려면 "" 로 두세요.

# === JSON만 포함된 zip으로 덮어쓰기 ===
def extract_json_and_only_json(zip_path: Path):
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        json_files = [f for f in zip_ref.namelist() if f.endswith(".json") and f.startswith("annotations/")]
        if not json_files:
            print(f"⚠️ JSON 파일 없음: {zip_path}")
            return
        json_internal_path = json_files[0]
        json_data = zip_ref.read(json_internal_path)
        json_filename = zip_path.stem + ".json"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as new_zip:
        new_zip.writestr(json_filename, json_data)

    print(f"📦 JSON만 포함된 zip으로 덮어쓰기 완료: {zip_path.name}")

# === 유틸 함수 ===
def get_all_jobs():
    jobs = []
    page = 1
    while True:
        r = requests.get(f"{CVAT_URL}/api/jobs?page={page}", headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        jobs.extend(data["results"])
        if not data["next"]:
            break
        page += 1
    return jobs

def get_task_info(task_id: int):
    r = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def get_project_name(project_id: int | None) -> str:
    if not project_id:
        return ""
    r = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=HEADERS)
    if r.status_code == 404:
        return ""
    r.raise_for_status()
    data = r.json()
    # CVAT 버전에 따라 name/title 필드가 다를 수 있어 넉넉하게 처리
    return data.get("name") or data.get("title") or ""

def get_annotations(job_id):
    r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def get_organization_name(org_id):
    if not org_id:
        return ""
    r = requests.get(f"{CVAT_URL}/api/organizations/{org_id}", headers=HEADERS)
    if r.status_code == 404:
        return ""
    r.raise_for_status()
    return r.json().get("slug", "")  # slug 기준 필터

def load_assignee_map_from_env():
    assignee_map = {}
    for key, value in os.environ.items():
        if key.startswith("USERMAP_"):
            username = key.replace("USERMAP_", "")
            assignee_map[username] = value
    return assignee_map

def get_label_types_from_annotations(job_id):
    label_types = set()
    try:
        annotations = get_annotations(job_id)
        for shape in annotations.get("shapes", []):
            shape_type = shape.get("shape_type") or shape.get("type")
            if shape_type:
                label_types.add(shape_type)
    except requests.RequestException as e:
        print(f"⚠️ 어노테이션 정보 가져오기 실패 (job_id={job_id}): {e}")
        return set()
    print(f"📌 Job ID {job_id} → 사용된 라벨 타입: {label_types}")
    return label_types

def run_cvat_cli_export(task_id: int, task_name: str, assignee: str, result_dir: Path,
                        export_log_path: Path, assignee_map: dict, export_format: str,
                        log_name_override: str | None = None):
    safe_name = task_name
    exported_date = datetime.today().strftime("%Y-%m-%d")

    # export_format 값에 따라 파일명 접미사 결정
    if export_format == CVAT_EXPORT_FORMAT:
        suffix = "_boundingbox"
    elif export_format == CVAT_EXPORT_FORMAT_4:
        suffix = "_keypoint"
    else:
        suffix = ""
    output_path = result_dir / f"{safe_name}{suffix}.zip"

    command = (
        f'cvat-cli --server-host {CVAT_URL} '
        f'--auth {CVAT_USERNAME}:{CVAT_PASSWORD} '
        f'task export-dataset {task_id} "{output_path}" '
        f'--format "{export_format}" '
        f'--with-images {WITH_IMAGES}'
    )
    try:
        print(f"🚀 Exporting: Task {task_id} → {output_path.name}")
        subprocess.run(command, shell=True, check=True)
        print(f"✅ Export 성공 → {output_path}")

        mapped_assignee = assignee_map.get(assignee, assignee)
        log_name = log_name_override if log_name_override else task_name

        with open(export_log_path, "a", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([task_id, log_name, mapped_assignee, exported_date])

        extract_json_and_only_json(output_path)

    except subprocess.CalledProcessError as e:
        print(f"❌ Export 실패: Task {task_id} - {e}")

def ci_contains(needle: str, hay: str) -> bool:
    return needle.lower() in hay.lower()

# === 메인 ===
def main():
    jobs = get_all_jobs()
    today_str = datetime.today().strftime("%Y-%m-%d")
    base_result_dir = Path(RESULT_DIR)

    export_log_path = Path("/home/pia/work_p/dfn/omission/result/export_log.csv")
    if not export_log_path.exists():
        with open(export_log_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["task_id", "task_name", "assignee", "exported_date"])

    # 로그 기반 중복 방지용 집합
    exported_task_ids = set()
    with open(export_log_path, "r", newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            exported_task_ids.add(row["task_id"])

    assignee_map = load_assignee_map_from_env()
    pn = (PROJECT_NAME or "").strip()

    for job in jobs:
        task_id = str(job["task_id"])
        state = job.get("state")
        stage = job.get("stage")

        # 완료 + 검수통과만 대상으로 필터
        if not (stage == "acceptance" and state == "completed"):
            continue

        task_info = get_task_info(int(task_id))
        task_name = task_info.get("name", f"task_{task_id}")
        org_slug = get_organization_name(task_info.get("organization"))
        project_name = get_project_name(task_info.get("project_id") or task_info.get("project"))

        # 조직 화이트리스트 필터 (환경변수 비어있을 경우 전체 허용)
        if ORGANIZATIONS and any(ORGANIZATIONS):
            if org_slug not in ORGANIZATIONS:
                continue

        assignee_info = job.get("assignee")
        assignee = assignee_info.get("username", "(unassigned)") if assignee_info else "(unassigned)"
        label_types = get_label_types_from_annotations(int(job["id"]))

        result_dir = base_result_dir / today_str / (org_slug or "no_org") / task_name
        result_dir.mkdir(parents=True, exist_ok=True)

        # === 자동 무시 모드 판단 (task_name / project_name / org_slug 모두 검사)
        ignore_mode = False
        matched_field = ""
        if pn:
            if ci_contains(pn, task_name):
                ignore_mode, matched_field = True, "task_name"
            elif project_name and ci_contains(pn, project_name):
                ignore_mode, matched_field = True, "project_name"
            elif org_slug and ci_contains(pn, org_slug):
                ignore_mode, matched_field = True, "org_slug"

        if ignore_mode:
            print(f"⚡ [무시 모드] '{PROJECT_NAME}' 매칭({matched_field}) → export_log.csv 무시하고 강제 export: task={task_name}, project={project_name}, org={org_slug}")
        else:
            # === 일반 모드: export_log.csv 기반 중복 방지 ===
            if task_id in exported_task_ids:
                print(f"⏩ 이미 export됨 → Task {task_id}, 건너뜀")
                continue

        # === 실제 export 실행 ===
        if label_types == {"rectangle"}:
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path,
                                assignee_map, CVAT_EXPORT_FORMAT)
        elif label_types == {"skeleton"}:
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path,
                                assignee_map, CVAT_EXPORT_FORMAT_4, log_name_override=task_name + "_k")
        elif {"rectangle", "skeleton"}.issubset(label_types):
            # 둘 다 있으면 두 번 export
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path,
                                assignee_map, CVAT_EXPORT_FORMAT, log_name_override=task_name)
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path,
                                assignee_map, CVAT_EXPORT_FORMAT_4, log_name_override=task_name + "_k")

if __name__ == "__main__":
    main()
