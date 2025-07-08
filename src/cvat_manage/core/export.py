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
ORG_FILTER = os.getenv("ORGANIZATION_FILTER")
RESULT_DIR = os.getenv("RESULT_DIR", "/tmp/cvat_exports")

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json"
}

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

def get_task_info(task_id):
    r = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def get_annotations(job_id):
    r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
    r.raise_for_status()
    return r.json()

def get_organization_name(org_id):
    if not org_id:
        return "(None)"
    r = requests.get(f"{CVAT_URL}/api/organizations/{org_id}", headers=HEADERS)
    if r.status_code == 404:
        return "(Not found)"
    r.raise_for_status()
    return r.json().get("slug", f"(org-{org_id})")

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

def run_cvat_cli_export(task_id: int, task_name: str, assignee: str, result_dir: Path, export_log_path: Path, assignee_map: dict, export_format: str):
    safe_name = task_name.replace(" ", "-")
    # output_path = result_dir / f"{safe_name}_{export_format}.zip"
    exported_date = datetime.today().strftime("%Y-%m-%d")

    # export_format 값에 따라 결정
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

        with open(export_log_path, "a", newline='') as f:
            writer = csv.writer(f)
            writer.writerow([task_id, task_name, mapped_assignee, exported_date])

        extract_json_and_only_json(output_path)

    except subprocess.CalledProcessError as e:
        print(f"❌ Export 실패: Task {task_id} - {e}")

# === 메인 ===
def main():
    jobs = get_all_jobs()

    today_str = datetime.today().strftime("%Y-%m-%d")
    # result_dir = Path(f"{RESULT_DIR}/{today_str}")
    base_result_dir = Path(RESULT_DIR)
    # result_dir.mkdir(parents=True, exist_ok=True)

    export_log_path = Path("/home/pia/work_p/dfn/omission/result/export_log.csv")

    if not export_log_path.exists():
        with open(export_log_path, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["task_id", "task_name", "assignee", "exported_date"])

    exported_task_ids = set()
    with open(export_log_path, "r", newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            exported_task_ids.add(row["task_id"])

    assignee_map = load_assignee_map_from_env()

    for job in jobs:
        task_id = str(job["task_id"])
        state = job.get("state")
        stage = job.get("stage")

        if not (stage == "acceptance" and state == "completed"):
            continue

        if task_id in exported_task_ids:
            print(f"⏩ 이미 export됨 → Task {task_id}, 건너뜀")
            continue

        task_info = get_task_info(int(task_id))
        task_name = task_info.get("name", f"task_{task_id}")
        task_org_id = task_info.get("organization")
        task_org_slug = get_organization_name(task_org_id)

        if ORG_FILTER and task_org_slug != ORG_FILTER:
            continue

        assignee_info = job.get("assignee")
        assignee = assignee_info.get("username", "(unassigned)") if assignee_info else "(unassigned)"

        label_types = get_label_types_from_annotations(int(job["id"]))

        result_dir = base_result_dir / today_str / task_org_slug / task_name.replace(" ", "_")
        result_dir.mkdir(parents=True, exist_ok=True)

        exported = False

        if label_types == {"rectangle"}:
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, CVAT_EXPORT_FORMAT)
            exported = True
        elif label_types == {"skeleton"}:
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, os.getenv("CVAT_EXPORT_FORMAT_4"))
            exported = True
        elif {"rectangle", "skeleton"}.issubset(label_types):
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, CVAT_EXPORT_FORMAT)
            run_cvat_cli_export(int(task_id), task_name, assignee, result_dir, export_log_path, assignee_map, os.getenv("CVAT_EXPORT_FORMAT_4"))

if __name__ == "__main__":
    main()