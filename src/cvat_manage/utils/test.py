import os
import csv
import requests
from collections import defaultdict
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

def get_project_name(project_id, headers, cache):
    if project_id in cache:
        return cache[project_id]
    
    res = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=headers)
    if res.status_code == 200:
        project_name = res.json().get("name", f"(ID:{project_id})")
    else:
        project_name = f"(ID:{project_id})"
    cache[project_id] = project_name
    return project_name

def get_task_info(task_id, headers, task_cache):
    if task_id in task_cache:
        return task_cache[task_id]
    
    res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=headers)
    if res.status_code == 200:
        task_data = res.json()
        task_cache[task_id] = task_data
        return task_data
    return {}

def get_jobs_assigned_to_user(username, org_slug, headers):
    jobs = []
    page = 1
    while True:
        url = f"{CVAT_URL}/api/jobs?assignee={username}&page={page}"
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        data = res.json()
        jobs.extend(data["results"])  
        if not data["next"]:
            break
        page += 1
    return jobs

def save_jobs_to_csv(jobs_data, output_path):
    fieldnames = ["project_name", "task_id", "job_id", "job_stage", "job_state"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in jobs_data:
            writer.writerow(row)
    print(f"📁 CSV 저장 완료: {output_path}")

if __name__== "__main__":
    username = "user02"
    org_slug = "vietnamlabeling"
    output_csv = "assigned_jobs_user02.csv"

    headers = build_headers(org_slug)
    jobs = get_jobs_assigned_to_user(username, org_slug, headers)

    if not jobs:
        print(f"사용자 '{username}'에게 할당된 Job이 없습니다.")
    else:
        project_name_cache = {}
        task_cache = {}
        result_rows = []

        for job in jobs:
            task_id = job.get("task_id")
            job_id = job.get("id")
            stage = job.get("stage")
            state = job.get("state")
            if not task_id:
                continue

            task_info = get_task_info(task_id, headers, task_cache)
            project_id = task_info.get("project_id")

            if project_id:
                project_name = get_project_name(project_id, headers, project_name_cache)
            else:
                project_name = "(No Project)"

            row = {
                "project_name": project_name,
                "task_id": task_id,
                "job_id": job_id,
                "job_stage": stage,
                "job_state": state
            }
            result_rows.append(row)

        # CSV 저장
        save_jobs_to_csv(result_rows, output_csv)
