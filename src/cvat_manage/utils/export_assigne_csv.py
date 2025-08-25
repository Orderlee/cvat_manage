import os
import csv
import requests
from dotenv import load_dotenv
from pathlib import Path

# 환경 변수 로드
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")

def build_headers(org_slug):
    return {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json",
        "X-Organization": org_slug
    }

def get_user_id(username, headers):
    url = f"{CVAT_URL}/api/users?search={username}"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    users = res.json().get("results", [])
    for user in users:
        if user["username"] == username:
            return user["id"]
    return None

def assign_job_to_user(job_id, user_id, headers, dry_run=True):
    if dry_run:
        print(f"🔎 (DRY RUN) Job {job_id} → 사용자 ID {user_id}에게 할당 예정")
        return
    url = f"{CVAT_URL}/api/jobs/{job_id}"
    res = requests.patch(url, headers=headers, json={"assignee": user_id})
    if res.status_code == 200:
        print(f"✅ Job {job_id} → 사용자 할당 완료")
    else:
        print(f"❌ Job {job_id} 할당 실패: {res.status_code} - {res.text}")

def assign_jobs_from_csv(csv_path, username, org_slug, dry_run=True):
    headers = build_headers(org_slug)
    user_id = get_user_id(username, headers)
    if not user_id:
        print(f"❌ 사용자 '{username}'를 찾을 수 없습니다.")
        return
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            job_id = row.get("job_id")
            if job_id:
                assign_job_to_user(job_id, user_id, headers, dry_run=dry_run)

if __name__ == "__main__":
    csv_file = " "
    username = " "
    org_slug = " "

    # 🔧 dry_run=True → 실제 할당은 하지 않고 출력만
    assign_jobs_from_csv(csv_file, username, org_slug, dry_run=False)
