"""
CSV를 읽어 (project_name, task_id, job_id) 별 'job name-like' 문자열을 출력하고 CSV로 저장하는 스크립트
- 이동/백업/복원 없음
- job 가명 포맷: "{task_name} · Job {index} [start-stop]"
  * index는 해당 task의 jobs를 start_frame 오름차순으로 정렬했을 때의 1-based 순번
  * start/stop_frame이 없으면 범위는 생략됨

필요 패키지: pip install requests python-dotenv
환경변수: .env 파일에 CVAT_URL_2, TOKEN_2
"""

import os
import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests
from dotenv import load_dotenv

# ===================== 사용자 설정 =====================
ORG =   # CSV의 org (보통 소스 org)
PROJECT_NAME = 
EXCLUSION_CSV =   # (project_name, task_id, job_id)
OUTPUT_CSV =   # 저장할 파일 이름
REQUEST_TIMEOUT = 30
# ======================================================

# --- .env 로드 (CVAT_URL_2, TOKEN_2 사용) ---
load_dotenv()
CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")

def hdr_json(x_org: Optional[str] = None) -> dict:
    h = {
        "Authorization": f"Token {TOKEN}",
        "Accept": "application/vnd.cvat+json",
        "Content-Type": "application/json",
    }
    if x_org:
        h["X-Organization"] = x_org
    return h

def hdr_org(x_org: Optional[str] = None) -> dict:
    h = {
        "Authorization": f"Token {TOKEN}",
        "Accept": "application/vnd.cvat+json",
    }
    if x_org:
        h["X-Organization"] = x_org
    return h

# ---------- CSV 로드 ----------
def load_rows(csv_path: str, target_project: str) -> List[Tuple[int, int]]:
    out: List[Tuple[int, int]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if not row:
                continue
            if row[0].strip().startswith("#"):
                continue
            cols = [c.strip() for c in row if c is not None]
            if len(cols) < 3:
                continue
            proj, t_str, j_str = cols[0], cols[1], cols[2]
            if proj != target_project:
                continue
            if not (t_str.isdigit() and j_str.isdigit()):
                continue
            out.append((int(t_str), int(j_str)))
    return out

# ---------- CVAT 조회 ----------
def fetch_task_name(session: requests.Session, org: str, task_id: int) -> str:
    r = session.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=hdr_json(org), timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return data.get("name") or f"task_{task_id}"

def fetch_jobs_in_task(session: requests.Session, org: str, task_id: int) -> List[Dict]:
    jobs: List[Dict] = []
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

# ---------- 가명 생성 ----------
def build_job_name_like(task_name: str, index: Optional[int], start_frame, stop_frame, job_id: int) -> str:
    if start_frame is not None and stop_frame is not None:
        range_str = f"[{start_frame}-{stop_frame}]"
    else:
        range_str = ""

    if index is not None:
        base = f"{task_name} · Job {index}"
    else:
        base = f"{task_name} · Job {job_id}"

    return f"{base} {range_str}".strip()

# ---------- 메인 ----------
def main():
    if not CVAT_URL or not TOKEN:
        raise RuntimeError("환경변수 CVAT_URL_2 / TOKEN_2가 필요합니다.")

    rows = load_rows(EXCLUSION_CSV, PROJECT_NAME)
    if not rows:
        print(f"(info) CSV '{EXCLUSION_CSV}'에서 프로젝트 '{PROJECT_NAME}' 행을 찾지 못했습니다.")
        return

    jobs_by_task: Dict[int, List[int]] = defaultdict(list)
    for tid, jid in rows:
        jobs_by_task[tid].append(jid)

    output_data = []  # CSV 저장용 데이터

    print(f"=== Job names for project '{PROJECT_NAME}' (from CSV: {EXCLUSION_CSV}) ===")
    with requests.Session() as s:
        for task_id, job_ids in jobs_by_task.items():
            try:
                task_name = fetch_task_name(s, ORG, task_id)
            except requests.HTTPError as e:
                print(f"[warn] task {task_id} name 조회 실패: {e}")
                task_name = f"task_{task_id}"

            try:
                all_jobs = fetch_jobs_in_task(s, ORG, task_id)
            except requests.HTTPError as e:
                print(f"[warn] task {task_id} jobs 조회 실패: {e}")
                all_jobs = []

            sorted_jobs = sorted(all_jobs, key=lambda j: (j.get("start_frame", 0), j.get("id", 0)))
            idx_map: Dict[int, Tuple[int, Optional[int], Optional[int]]] = {}
            for i, j in enumerate(sorted_jobs, start=1):
                idx_map[int(j["id"])] = (i, j.get("start_frame"), j.get("stop_frame"))

            for jid in job_ids:
                if jid in idx_map:
                    idx, s_frame, e_frame = idx_map[jid]
                    name_like = build_job_name_like(task_name, idx, s_frame, e_frame, jid)
                else:
                    name_like = build_job_name_like(task_name, None, None, None, jid)

                print(f"- task_id={task_id}, job_id={jid} → {name_like}")
                output_data.append([task_id, jid, name_like])

    # CSV 저장
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["task_id", "job_id", "job_name_like"])
        writer.writerows(output_data)

    print(f"=== done ===\n결과가 '{OUTPUT_CSV}' 파일로 저장되었습니다.")

if __name__ == "__main__":
    main()