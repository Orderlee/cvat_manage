import os
import csv
import argparse
# import smtplib
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
# from email.mime.multipart import MIMEMultipart
# from email.mime.base import MIMEBase 
# from email.mime.text import MIMEText
# from email.utils import COMMASPACE
# from email import encoders
from collections import defaultdict, Counter
import matplotlib.pyplot as plt
import koreanize_matplotlib
import seaborn as sns
import pandas as pd
# from msal import ConfidentialClientApplication
import base64


# Load .env
# env_path = Path.cwd().parent / ".env"
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORGANIZATION_LIST = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()]


# ORGANIZATION = os.getenv("ORGANIZATION")
# ORG_FILTER = os.getenv("ORGANIZATION_FILTER")
# USER_FILTER = os.getenv("USER_FILTER")

# Date filtering
DATE_FROM = os.getenv("DATE_FROM")
DATE_TO = os.getenv("DATE_TO")
DATE_FROM = datetime.strptime(DATE_FROM, "%Y-%m-%d") if DATE_FROM else None
DATE_TO = datetime.strptime(DATE_TO, "%Y-%m-%d") if DATE_TO else None

HEADERS = {
    "Authorization": f"Token {TOKEN}",
    "Content-Type": "application/json"
}

# === API 요청 함수 ===
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
    r = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=HEADERS)
    r.raise_for_status()
    return r.json().get("name", f"(No name, ID {task_id})")

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

def get_job_issues(job_id):
    r = requests.get(f"{CVAT_URL}/api/issues?job_id={job_id}", headers=HEADERS)
    r.raise_for_status()
    return [(i["frame"], i["message"]) for i in r.json().get("results", [])]

def get_job_labels(job_id):
    r = requests.get(f"{CVAT_URL}/api/labels?job_id={job_id}", headers=HEADERS)
    r.raise_for_status()
    return [l["name"] for l in r.json().get("results", [])]

def get_missing_annotated_frames(job_id, start_frame, stop_frame):
    r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    annotated = set(s["frame"] for s in data.get("shapes", []))
    return [f for f in range(start_frame, stop_frame + 1) if f not in annotated]

def get_labeled_shape_count(job_id):
    r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    return len(data.get("shapes", []))

def is_within_date_range(created_date):
    if not DATE_FROM and not DATE_TO:
        return True
    dt = datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%S.%fZ")
    if DATE_FROM and dt < DATE_FROM:
        return False
    if DATE_TO and dt > DATE_TO:
        return False
    return True

def get_user_display_name(user_id):
    return os.getenv(f"USERMAP_{user_id}", user_id)

# === 메인 ===
def main(quiet=False):
    jobs = get_all_jobs()
    task_cache, project_cache, org_cache = {}, {}, {}
    results = []
    user_stats = defaultdict(lambda: {"jobs": 0, "missing": 0, "frames": 0})
    completed_frame_stats = defaultdict(int)


    for job in jobs:
        job_id = job["id"]
        task_id, project_id, org_id = job["task_id"], job["project_id"], job.get("organization")
        created_date = job.get("created_date")
        if not is_within_date_range(created_date):
            continue
        
        stage = job.get("stage")
        status = job.get("status")
        

        # 완료된 어노테이션 작업 프레임 수 기록
        if stage == "annotation" and status == "completed":
            assignee = job.get("assignee")
            username = assignee["usename"] if assignee else "(Unassigned)"
            start_frame = job.get("start_frame", 0)
            stop_frame = job.get("stop_frame", 0)
            frame_count = stop_frame - start_frame + 1 if stop_frame >= start_frame else 0
            completed_frame_stats[username] += frame_count

        # assignee = job.get("assignee")
        # assignee_username = assignee["username"] if assignee else "(Unassigned)"
        # assignee_display = get_user_display_name(assignee_username)

        if task_id not in task_cache:
            task_cache[task_id] = get_task_name(task_id)
        if project_id not in project_cache:
            project_cache[project_id] = get_project_name(project_id)
        if org_id not in org_cache:
            org_cache[org_id] = get_organization_name(org_id)

        task_name = task_cache[task_id]
        project_name = project_cache[project_id]
        org_name = org_cache[org_id]

        # if ORG_FILTER and ORG_FILTER != org_name:
        #     continue

        # 필터링 시 여러 개 중 하나라도 해당하는 경우 통과
        if ORGANIZATION_LIST and org_name not in ORGANIZATION_LIST:
            continue

        # if ORGANIZATION and ORGANIZATION != org_name:
        #     continue

        assignee = job.get("assignee")
        assignee_username = assignee["username"] if assignee else "(Unassigned)"
        assignee_display = get_user_display_name(assignee_username)
        # if USER_FILTER and USER_FILTER != assignee_str:
        #     continue

        # state, status, stage = job.get("state"), job.get("status"), job.get("stage")
        # start_frame, stop_frame = job.get("start_frame", 0), job.get("stop_frame", 0)
        state = job.get("state")
        start_frame = job.get("start_frame", 0)
        stop_frame = job.get("stop_frame", 0)
        total_frames = stop_frame - start_frame + 1

        labels = get_job_labels(job_id)
        issues = get_job_issues(job_id)
        missing_frames = get_missing_annotated_frames(job_id, start_frame, stop_frame)
        missing_rate = round(len(missing_frames) / total_frames * 100, 2) if total_frames else 0
        label_count = get_labeled_shape_count(job_id)

        results.append({
            "organization": org_name,
            "project": project_name,
            "task": task_name,
            "task_id": task_id,
            "assignee": assignee_display,
            "created": created_date,
            "state": state,
            "status": status,
            "stage": stage,
            "labels": ', '.join(labels),
            "label_count": label_count,
            "issue_count": len(issues),
            "issues": '; '.join([f"Frame {f}: {m}" for f, m in issues]),
            # "total_frames": total_frames,
            "missing_count": len(missing_frames),
            "missing_rate": missing_rate,
            "missing_frames": ', '.join(map(str, missing_frames)),
            "frame_range": f"{start_frame}~{stop_frame}"            
        })

        user_stats[assignee_display]["jobs"] += 1
        user_stats[assignee_display]["missing"] += len(missing_frames)
        user_stats[assignee_display]["frames"] += total_frames

    results.sort(key=lambda x: x["stage"], reverse=True)

    csv_dir = Path(__file__).resolve().parent / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    today_str = datetime.today().strftime("%Y-%m-%d")
    csv_filename = csv_dir / f"cvat_job_report_{today_str}.csv"

    with open(csv_filename, "w", newline="") as f:
        fieldnames = [
            "organization", "project", "task", "task_id", "assignee", "created",
            "state", "status", "stage", "labels", "label_count", "issue_count", "issues",
            "missing_count", "missing_rate", "missing_frames", "frame_range"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    if not quiet:
        print(f"📄 CSV 저장 완료: {csv_filename}")

    # 📌 프로젝트별 작업자 누락률 요약
    print("\n📌 Organization +  Project별 작업자 Omission Rate 요약:")
    org_project_user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict (lambda: {"jobs": 0, "missing": 0, "frames": 0})))
    

    for row in results:
        org = row["organization"]
        project = row["project"]
        user = row["assignee"]
        total_frames = row["missing_count"] + row["label_count"]
        org_project_user_stats[org][project][user]["jobs"] += 1
        org_project_user_stats[org][project][user]["missing"] += row["missing_count"]
        org_project_user_stats[org][project][user]["frames"] += total_frames

    for org, projects in org_project_user_stats.items():
        print(f"\n🏢 [Organization: {org}]")
        for project, users in projects.items():
            print(f"📂 [Project: {project}]")
            for user, stat in users.items():
                frames = stat["frames"]
                missing = stat["missing"]
                jobs = stat["jobs"]
                rate = round(missing / frames * 100, 2) if frames else 0
                print(f" - {user} → Job: {jobs}개 | Omission Rate: {rate}% ({missing} / {frames})")

    # 📌 Organization + Project별 Annotation 상태 통계
    print("\n📌 Organization + Project별 Annotation Status Statistics:")

    df = pd.DataFrame(results)
    # state + stage 조합을 통한 상태명 매핑
    grouped = df.groupby(["organization", "project", "stage", "state"]).size().reset_index(name="count")

    # 출력용 매핑
    state_mapping = {
        "new": "new",
        "in progress": "progress",
        "completed": "completed",
    }

    for org_name in grouped["organization"].unique():
        print(f"\n🏢 [Organization: {org_name}]")
        org_df = grouped[grouped["organization"] == org_name]
        for project_name in org_df["project"].unique():
            print(f"📂 [Project: {project_name}] Annotation Status Statistics:")
            sub = org_df[org_df["project"] == project_name]
            for _, row in sub.iterrows():
                stage = row["stage"]
                state = row["state"]
                count = row["count"]

                # stage가 None이면 건너뜀
                if not stage:
                    continue

                stage_label = stage
                state_label = state_mapping.get(state, state)
                print(f" - {stage_label} {state_label}: {count}개")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--quiet", action="store_true", help="콘솔 출력 생략 (crontab용)")
    args = parser.parse_args()
    main(quiet=args.quiet)