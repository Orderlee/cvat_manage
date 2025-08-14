"""
프로젝트 내 '완료(completed)' 상태의 Job만 찾아서
사람이 보기 좋은 job name을 출력하고 CSV로 저장하는 스크립트
- Job 이름 포맷: "{task_name} · Job {index}"
- 프레임 범위([start-stop])는 제거
"""

import os
import csv
from typing import Dict, List, Tuple, Optional
import requests
from dotenv import load_dotenv

# ===================== 사용자 설정 =====================
ORG = "vietnamlabeling"       # 조회할 조직
PROJECT_NAME = "ad_lib_weapon"    # 조회할 프로젝트 이름
OUTPUT_CSV = "completed_job_names_weapon.csv"  # 저장 파일명
REQUEST_TIMEOUT = 30
# ======================================================

# --- .env 로드 (CVAT_URL_2, TOKEN_2 사용) ---
load_dotenv()
CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")

# -------------------- 공통 헤더 --------------------
def hdr_json(x_org: Optional[str] = None) -> dict:
    """JSON API 호출용 헤더"""
    h = {
        "Authorization": f"Token {TOKEN}",
        "Accept": "application/vnd.cvat+json",
        "Content-Type": "application/json",
    }
    if x_org:
        h["X-Organization"] = x_org
    return h

# -------------------- 유틸 함수 --------------------
def get_project_id(session: requests.Session, org: str, name: str) -> int:
    r = session.get(
        f"{CVAT_URL}/api/projects",
        headers=hdr_json(org),
        params={"name": name, "page_size": 100},
        timeout=REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    for p in r.json().get("results", []):
        if p.get("name") == name:
            return p["id"]
    raise RuntimeError(f"프로젝트를 찾지 못했습니다: org='{org}', name='{name}'")

def list_tasks_in_project(session: requests.Session, org: str, project_id: int) -> List[dict]:
    tasks: List[dict] = []
    page = 1
    while True:
        r = session.get(
            f"{CVAT_URL}/api/tasks",
            headers=hdr_json(org),
            params={"project_id": project_id, "page": page, "page_size": 100},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        tasks.extend(data.get("results", []))
        if not data.get("next"):
            break
        page += 1
    return tasks

def list_jobs_in_task(session: requests.Session, org: str, task_id: int) -> List[dict]:
    jobs: List[dict] = []
    page = 1
    while True:
        r = session.get(
            f"{CVAT_URL}/api/jobs",
            headers=hdr_json(org),
            params={"task_id": task_id, "page": page, "page_size": 100},
            timeout=REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        jobs.extend(data.get("results", []))
        if not data.get("next"):
            break
        page += 1
    return jobs

def build_job_name_like(task_name: str,
                        index: Optional[int],
                        job_id: int) -> str:
    """
    사람이 보기 좋은 job 이름(가명)을 생성합니다.
    - index가 있으면 index를, 없으면 job_id를 사용합니다.
    - 프레임 범위는 제거
    """
    return f"{task_name}" if index is not None else f"{task_name}"

# -------------------- 메인 로직 --------------------
def main():
    if not CVAT_URL or not TOKEN:
        raise RuntimeError("환경변수 CVAT_URL_2 / TOKEN_2가 필요합니다.")

    with requests.Session() as s:
        project_id = get_project_id(s, ORG, PROJECT_NAME)
        print(f"프로젝트 '{PROJECT_NAME}' (id={project_id}) 조회 성공")

        tasks = list_tasks_in_project(s, ORG, project_id)
        if not tasks:
            print("해당 프로젝트에 task가 없습니다.")
            return

        rows_for_csv: List[List[str]] = []
        total_completed = 0

        print(f"=== Completed Jobs in project '{PROJECT_NAME}' ===")
        for t in tasks:
            task_id = t["id"]
            task_name = t.get("name") or f"task_{task_id}"

            all_jobs = list_jobs_in_task(s, ORG, task_id)
            if not all_jobs:
                continue

            sorted_jobs = sorted(all_jobs, key=lambda j: (j.get("start_frame", 0), j.get("id", 0)))
            index_map: Dict[int, int] = {int(job["id"]): idx for idx, job in enumerate(sorted_jobs, start=1)}

            completed_jobs = [j for j in all_jobs if (j.get("state") or "").lower() == "new"]
            if not completed_jobs:
                continue

            print(f"- task_id={task_id} '{task_name}': {len(completed_jobs)} completed")
            for j in completed_jobs:
                jid = int(j["id"])
                state = j.get("state") or ""
                idx = index_map.get(jid)
                job_name_like = build_job_name_like(task_name, idx, jid)

                print(f"    · job_id={jid} → {job_name_like}")
                rows_for_csv.append([PROJECT_NAME, str(task_id), str(jid), state, job_name_like])
                total_completed += 1

        if rows_for_csv:
            with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["project_name", "task_id", "job_id", "state", "job_name"])
                w.writerows(rows_for_csv)
            print(f"\n총 {total_completed}개의 completed job을 '{OUTPUT_CSV}'에 저장했습니다.")
        else:
            print("completed 상태의 job을 찾지 못했습니다.")

if __name__ == "__main__":
    main()
