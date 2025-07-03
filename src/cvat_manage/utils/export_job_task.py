import requests
import csv
import os
import re
from dotenv import load_dotenv
from pathlib import Path

# .env 파일 경로 설정 (상위 폴더를 가리키도록 수정됨)
dotenv_path = Path(__file__).resolve().parent.parent / ".env"

# === .env 파일 로드 ===
load_dotenv(dotenv_path=dotenv_path)

def load_usermap_from_env(path):
    mappings = {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                
                if line.startswith('USERMAP_'):
                    key, value = line.split('=', 1)
                    mappings[key.strip()] = value.strip()
    except FileNotFoundError:
        print(f"⚠️ [오류] .env 파일을 다음 경로에서 찾을 수 없습니다: {path}")
    return mappings

# === CVAT API 설정 ===
CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORGANIZATION = os.getenv("ORGANIZATION")

# === 사용자 매핑 정보 로드 ===
USER_MAPPINGS = load_usermap_from_env(dotenv_path)
print("✅ 다음 사용자 매핑을 로드했습니다:")
print(USER_MAPPINGS)


# === 사용자 이름 → display name 매핑 ===
def get_user_display_name(username):
    if username is None:
        return "Unassigned"
    
    clean_username = username.strip()
    lookup_key = f"USERMAP_{clean_username}"
    
    display_name = USER_MAPPINGS.get(lookup_key, clean_username)

    if display_name != clean_username:
        print(f"🔁 사용자 매핑: '{clean_username}' → '{display_name}'")
    else:
        print(f"⚠️ 매핑 없음: '{clean_username}' (딕셔너리에서 '{lookup_key}' 키를 찾지 못함)")
        
    return display_name

# === 요청 헤더 ===
headers = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json",
    "X-Organization": ORGANIZATION
}

# === 결과 저장 경로 및 ID 목록 ===
output_csv_path = Path("./cvat_task_job_assignees_adlib.csv")
task_ids_to_fetch = list(range(6648, 6532, -1))
job_ids_to_fetch = list(range(8632, 8790)) # ⭐️ 신규: 8632 ~ 8789 까지의 Job ID 목록

rows = []
project_cache = {} # ⭐️ 신규: 프로젝트 정보 저장을 위한 캐시

# === 1. Task ID 순회 ===
print("\n--- Task ID 범위 순회 시작 ---")
for task_id in task_ids_to_fetch:
    try:
        task_res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=headers)
        if task_res.status_code != 200: continue
        task_data = task_res.json()
        project_id = task_data.get("project_id")
        
        # 프로젝트 정보 가져오기 (캐시 활용)
        if project_id in project_cache:
            project_name = project_cache[project_id]
        else:
            project_res = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=headers)
            project_name = project_res.json().get("name", "").strip() if project_res.status_code == 200 else ""
            if project_name: project_cache[project_id] = project_name

        if "ad_lib" not in project_name.lower(): continue
        print(f"[{task_id}] 📌 Task: {task_data.get('name', '')}, Project: {project_name}")

        job_res = requests.get(f"{CVAT_URL}/api/jobs?task_id={task_id}", headers=headers)
        if job_res.status_code != 200: continue

        jobs = job_res.json().get("results", [])
        for job in jobs:
            job_id = job.get("id")
            assignee = job.get("assignee")
            username = assignee.get("username") if assignee else None
            display_name = get_user_display_name(username)
            rows.append([project_name, task_id, job_id, display_name])

        print(f"✅ Task {task_id} 완료 (Job {len(jobs)}건)")
    except Exception as e:
        print(f"⚠️ 오류(Task {task_id}): {e}")

# === 2. Job ID 순회 (신규 추가된 로직) ===
print("\n--- 지정된 Job ID 범위 순회 시작 ---")
for job_id in job_ids_to_fetch:
    try:
        job_res = requests.get(f"{CVAT_URL}/api/jobs/{job_id}", headers=headers)
        if job_res.status_code != 200:
            print(f"[{job_id}] ❌ Job 조회 실패")
            continue

        job_data = job_res.json()
        task_id = job_data.get("task_id")

        # 프로젝트 정보 가져오기 (캐시 활용)
        task_res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=headers)
        if task_res.status_code != 200: continue
        project_id = task_res.json().get("project_id")
        
        if project_id in project_cache:
            project_name = project_cache[project_id]
        else:
            project_res = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=headers)
            project_name = project_res.json().get("name", "").strip() if project_res.status_code == 200 else ""
            if project_name: project_cache[project_id] = project_name

        if "ad_lib" not in project_name.lower(): continue

        assignee = job_data.get("assignee")
        username = assignee.get("username") if assignee else None
        display_name = get_user_display_name(username)
        rows.append([project_name, task_id, job_id, display_name])
        print(f"✅ Job {job_id} 완료 (Project: {project_name})")
    except Exception as e:
        print(f"⚠️ 오류(Job {job_id}): {e}")

# === CSV 저장 ===
with open(output_csv_path, mode="w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    writer.writerow(["project_name", "task_id", "job_id", "assignee_display_name"])
    writer.writerows(rows)

print(f"\n📄 결과 저장 완료: {output_csv_path.resolve()}")
print(f"📊 총 저장된 행 수: {len(rows)}")