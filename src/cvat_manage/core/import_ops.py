import os
import requests
from pathlib import Path
import time
import colorsys
import argparse
import csv
from datetime import datetime
from dotenv import load_dotenv

# Load .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
today_str = datetime.today().strftime("%Y-%m-%d")

log_dir = Path(os.getenv("ASSIGN_LOG_DIR","/home/pia/work_p/dfn/omission/logs"))
ASSIGN_LOG_PATH = log_dir / f"assignments_log_{today_str}.csv"

def get_or_create_organization(name):
    headers = {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json"
    }
    res = requests.get(f"{CVAT_URL}/api/organizations", headers=headers)
    res.raise_for_status()
    orgs = res.json()["results"]

    for org in orgs:
        if org["slug"] == name or org["name"] == name:
            return org["id"], org["slug"]
        
    slug = name.lower().replace(" ", "-")
    data = {"name": name, "slug": slug}
    res = requests.post(f"{CVAT_URL}/api/organizations", headers=headers, json=data)
    res.raise_for_status()
    return res.json()["id"], slug

def build_headers(org_slug):
    return {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json",
        "X-Organization": org_slug
    }

def hsv_to_hex(h, s, v):
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return '#{:02X}{:02X}{:02X}'.format(int(r * 255), int(g * 255), int(b * 255))

def create_project(name, labels, headers):
    num_labels = len(labels)
    label_defs = []
    for i, label in enumerate(labels):
        hue = i / num_labels
        color = hsv_to_hex(hue, 0.7, 0.95)
        label_defs.append({"name": label, "color": color})

    data = {"name": name, "labels": label_defs}
    res = requests.post(f"{CVAT_URL}/api/projects", headers=headers, json=data)
    res.raise_for_status()
    return res.json()["id"]

def create_task_with_zip(name, project_id, zip_path, headers):
    task_data = {
        "name": name,
        "project_id": project_id,
        "image_quality": 70,
        "segment_size": 100
    }
    res = requests.post(f"{CVAT_URL}/api/tasks", headers=headers, json=task_data)
    res.raise_for_status()
    task_id = res.json()["id"]

    upload_headers = headers.copy()
    upload_headers.pop("Content-Type", None)

    with open(zip_path, "rb") as zip_file:
        files = {
            "client_files[0]": (os.path.basename(zip_path), zip_file, "application/zip")
        }
        data = {
            "image_quality": 70,
            "use_zip_chunks": "false",
            "use_cache": "false",
            "sorting_method": "lexicographical",
            "upload_format": "zip"
        }
        upload_url = f"{CVAT_URL}/api/tasks/{task_id}/data"
        res = requests.post(upload_url, headers=upload_headers, files=files, data=data)
        res.raise_for_status()

    return task_id

def wait_until_task_ready(task_id, headers, timeout=60):
    print(f"⏳ 태스크 데이터 로딩 대기 중...")
    start = time.time()
    while time.time() - start < timeout:
        res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=headers)
        res.raise_for_status()
        task = res.json()
        if task["size"] > 0:
            print(f"✅ 태스크 데이터 준비 완료 (총 이미지 수: {task['size']})")
            return True
        time.sleep(2)
    print("❌ 태스크 데이터 로딩 시간 초과")
    return False

def get_jobs(task_id, headers):
    res = requests.get(f"{CVAT_URL}/api/jobs?task_id={task_id}", headers=headers)
    res.raise_for_status()
    return res.json()["results"]

def get_user_id(username, headers):
    res = requests.get(f"{CVAT_URL}/api/users", headers=headers, params={"search": username})
    res.raise_for_status()
    users = res.json()["results"]
    for user in users:
        if user["username"] == username:
            return user["id"]
    return None

def assign_jobs_to_one_user(jobs, headers, assignee_name):
    user_id = get_user_id(assignee_name, headers)
    if not user_id:
        print(f"❌ 사용자 '{assignee_name}'를 찾을 수 없습니다.")
        return

    print(f"👥 이 태스크의 모든 Job을 '{assignee_name}'에게 할당합니다")

    for job in jobs:
        job_id = job["id"]
        current_assignee = job.get("assignee")
        if current_assignee:
            print(f"👤 Job {job_id}은 이미 '{current_assignee['username']}'에게 할당되어 있습니다. 건너뜁니다.")
            continue

        data = {"assignee": user_id}
        try:
            res = requests.patch(f"{CVAT_URL}/api/jobs/{job_id}", headers=headers, json=data)
            res.raise_for_status()
            print(f"✅ Job {job_id} → '{assignee_name}'에게 할당 완료")
        except requests.HTTPError as e:
            print(f"⚠️ Job {job_id} → '{assignee_name}' 할당 실패: {e.response.status_code} - {e.response.text}")

def review_jobs(jobs, headers):
    for job in jobs:
        job_id = job["id"]
        ann_url = f"{CVAT_URL}/api/jobs/{job_id}/annotations"
        res = requests.get(ann_url, headers=headers)
        res.raise_for_status()
        ann = res.json()
        if len(ann.get("shapes", [])) > 0:
            patch_data = {
                "stage": "validation",
                "state": "completed"
            }
            res = requests.patch(f"{CVAT_URL}/api/jobs/{job_id}", headers=headers, json=patch_data)
            res.raise_for_status()
            print(f"🔍 Job {job_id} → 검수 완료 전환")

def get_user_display_name(username):
    return os.getenv(f"USERMAP_{username}", username)

def log_assignment(task_name, task_id, assignee, num_jobs):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    display_name = get_user_display_name(assignee)
    log_entry = [now, task_name, task_id, display_name, num_jobs]
    write_header = not ASSIGN_LOG_PATH.exists()

    with open(ASSIGN_LOG_PATH, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["timestamp", "task_name", "task_id", "assignee", "num_jobs"])
        writer.writerow(log_entry)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload ZIPs to CVAT and assign jobs")
    parser.add_argument("--org_name", type=str, required=True)
    parser.add_argument("--zip_dir", type=str, required=True)
    parser.add_argument("--assignees", type=str, nargs="+", required=True)
    parser.add_argument("--project_name", type=str, required=True)
    parser.add_argument("--labels", type=str, nargs="+", required=True)
    args = parser.parse_args()

    org_id, org_slug = get_or_create_organization(args.org_name)
    headers = build_headers(org_slug)
    print(f"📦 Organization: {org_slug} (ID: {org_id})")

    project_id = create_project(args.project_name, labels=args.labels, headers=headers)
    print(f"📁 프로젝트 생성: {project_id}")


    zip_files = sorted(Path(args.zip_dir).rglob("*.zip"))
    assignees = args.assignees
    num_users = len(assignees)

    for i, zip_file in enumerate(zip_files):
        task_name = zip_file.stem
        print(f"\n🆕 처리 중: {task_name} ({zip_file.name})")

        task_id = create_task_with_zip(task_name, project_id, str(zip_file), headers=headers)
        print(f"🗂️ 태스크 생성 및 ZIP 업로드 완료: {task_id}")

        if not wait_until_task_ready(task_id, headers=headers):
            print("🚫 어노테이션을 실행하지 않습니다.")
            continue

        time.sleep(2)
        jobs = get_jobs(task_id, headers=headers)

        # ⬇️ 태스크 단위로 사용자 한 명 할당
        assignee_name = assignees[i % num_users]
        assign_jobs_to_one_user(jobs, headers=headers, assignee_name=assignee_name)

        # 로그 기록 추가
        log_assignment(task_name, task_id, assignee_name, len(jobs))

        time.sleep(2)
        review_jobs(jobs, headers=headers)
