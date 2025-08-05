import os
import csv
import argparse
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict, Counter
import pandas as pd

# Load environment variables
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORGANIZATION_LIST = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()]

DATE_FROM = os.getenv("DATE_FROM")
DATE_TO = os.getenv("DATE_TO")
DATE_FROM = datetime.strptime(DATE_FROM, "%Y-%m-%d") if DATE_FROM else None
DATE_TO = datetime.strptime(DATE_TO, "%Y-%m-%d") if DATE_TO else None

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json"
}

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

def get_task_name(task_id):
    try:
        r = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=HEADERS)
        if r.status_code == 404:
            print(f"⚠️ Task ID {task_id} 없음 (404)")
            return f"(Unknown Task ID {task_id})"
        r.raise_for_status()
        return r.json().get("name", f"(No name, ID {task_id})")
    except requests.exceptions.RequestException as e:
        print(f"❌ Task 정보 요청 실패 (ID {task_id}): {e}")
        return f"(Error Task ID {task_id})"

def get_project_name(project_id):
    r = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json().get("name", f"(No name, ID {project_id})")

def get_organization_name(org_id):
    if not org_id:
        return "(None)"
    r = requests.get(f"{CVAT_URL}/api/organizations/{org_id}", headers=HEADERS)
    if r.status_code == 404:
        return "(Not found)"
    r.raise_for_status()
    return r.json().get("slug", f"(No name, ID {org_id})")

def get_user_display_name(user_id):
    return os.getenv(f"USERMAP_{user_id}", user_id)

def get_job_labels(job_id):
    r = requests.get(f"{CVAT_URL}/api/labels?job_id={job_id}", headers=HEADERS)
    r.raise_for_status()
    return [l["name"] for l in r.json().get("results", [])]

def get_job_issues(job_id):
    r = requests.get(f"{CVAT_URL}/api/issues?job_id={job_id}", headers=HEADERS)
    r.raise_for_status()
    return [
        (i.get("frame", -1), i.get("message", "(no message)"))
        for i in r.json().get("results", [])
        if "frame" in i
    ]

def get_annotations(job_id):
    r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
    r.raise_for_status()
    return r.json().get("shapes", [])

def main(quiet=False):
    jobs = get_all_jobs()
    task_cache, project_cache, org_cache = {}, {}, {}
    org_proj_user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"total_jobs": 0, "completed_jobs": 0})))
    status_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    results = []

    for job in jobs:
        task_id = job["task_id"]
        project_id = job["project_id"]
        org_id = job.get("organization")
        created_date = job.get("created_date")

        if DATE_FROM or DATE_TO:
            dt = datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            if DATE_FROM and dt < DATE_FROM:
                continue
            if DATE_TO and dt > DATE_TO:
                continue

        if task_id not in task_cache:
            task_cache[task_id] = get_task_name(task_id)
        if project_id not in project_cache:
            project_cache[project_id] = get_project_name(project_id)
        if org_id not in org_cache:
            org_cache[org_id] = get_organization_name(org_id)

        task_name = task_cache[task_id]
        project_name = project_cache[project_id]
        org_name = org_cache[org_id]

        if ORGANIZATION_LIST and org_name not in ORGANIZATION_LIST:
            continue

        assignee = job.get("assignee")
        assignee_username = assignee["username"] if assignee else "(Unassigned)"
        assignee_display = get_user_display_name(assignee_username)

        stage = job.get("stage")
        state = job.get("state")

        org_proj_user_stats[org_name][project_name][assignee_display]["total_jobs"] += 1
        status_stats[org_name][project_name][f"{stage} {state}"] += 1

        job_id = job["id"]
        annotations = get_annotations(job_id)
        label_count = len(annotations)
        issues = get_job_issues(job_id)
        labels = get_job_labels(job_id)

        if (stage == "annotation" and state == "completed") or (stage == "acceptance" and state == "completed"):
            org_proj_user_stats[org_name][project_name][assignee_display]["completed_jobs"] += 1

        frame_range = f"{job.get('start_frame', 0)}~{job.get('stop_frame', 0)}"

        total_frames = job.get("stop_frame", 0) - job.get("start_frame", 0) + 1
        annotated_frames = set(shape["frame"] for shape in annotations)
        missing_frames = [f for f in range(job.get("start_frame", 0), job.get("stop_frame", 0) + 1) if f not in annotated_frames]
        missing_count = len(missing_frames)
        missing_rate = round(missing_count / total_frames * 100, 2) if total_frames else 0

        results.append({
            "organization": org_name,
            "project": project_name,
            "task": task_name,
            "task_id": task_id,
            "assignee": assignee_display,
            "created": created_date,
            "state": state,
            "stage": stage,
            "labels": ", ".join(labels),
            "label_count": label_count,
            "issue_count": len(issues),
            "issues": "; ".join([f"Frame {f}: {m}" for f, m in issues]),
            "missing_count": missing_count,
            "missing_rate": missing_rate,
            "missing_frames": ", ".join(map(str, missing_frames)),
            "frame_range": frame_range
        })

    today_str = datetime.today().strftime("%Y-%m-%d")
    csv_dir = Path(__file__).resolve().parent / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_filename = csv_dir / f"cvat_job_report_{today_str}.csv"

    with open(csv_filename, "w", newline="") as f:
        fieldnames = [
            "organization", "project", "task", "task_id", "assignee", "created",
            "state", "stage", "labels", "label_count",
            "issue_count", "issues", "missing_count", "missing_rate",
            "missing_frames", "frame_range"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    if not quiet:
        print(f"\n📄 CSV 저장 완료: {csv_filename}")

        print("\n📌 Organization +  Project별 작업자 Completion Rate 요약:")
        for org, projects in org_proj_user_stats.items():
            print(f"\n🏢 [Organization: {org}]")
            for proj, users in projects.items():
                print(f"📂 [Project: {proj}]")
                for user, stats in users.items():
                    total = stats["total_jobs"]
                    completed = stats["completed_jobs"]
                    rate = round(completed / total * 100, 2) if total else 0
                    print(f" - {user} → Job: {total}개 | Completed: {rate}% ({completed} / {total})")

        print("\n📌 Organization + Project별 Annotation Status Statistics:")
        for org, projects in status_stats.items():
            print(f"\n🏢 [Organization: {org}]")
            for proj, states in projects.items():
                total_count = sum(states.values())
                print(f"📂 [Project: {proj}] Annotation Status Statistics: (총 {total_count}개)")
                for state_label, count in states.items():
                    percentage = round(count / total_count * 100, 2) if total_count else 0
                    print(f" - {state_label}: {count}개 ({percentage}%)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", action="store_true", help="콘솔 출력 생략 (crontab용)")
    args = parser.parse_args()
    main(quiet=args.quiet)

