import os
import sys
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Iterable, List, Optional

# Load .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORG_SLUG = ""
PROJECT_NAME = ""

FILTER_STAGE = "acceptance"
FILTER_STATE = None

# 변경할 값
NEW_STAGE = "annotation"   # <-- 요청사항 반영
NEW_STATE = "new"          # <-- 요청사항 반영

# dry_run
DRY_RUN = False 

def headers() -> Dict[str, str]:
    if not TOKEN or TOKEN.startswith("<PUT_"):
        print("ERROR: CVAT_TOKEN 미설정. Personal Access Token을 넣어주세요.", file=sys.stderr)
        sys.exit(1)
    return {
        "Authorization": f"Token {TOKEN}",
        "Accept": "application/vnd.cvat+json",
        "Content-Type": "application/json",
        "X-Organization": ORG_SLUG,  # 조직 컨텍스트
    }

def get_json(url: str, params: Optional[Dict]=None) -> Dict:
    r = requests.get(url, headers=headers(), params=params, timeout=30)
    if not r.ok:
        raise RuntimeError(f"GET {url} failed: {r.status_code} {r.text}")
    return r.json()

def patch_json(url: str, payload:Dict) -> Dict:
    r = requests.patch(url, headers=headers(), json=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"PATCH {url} failed: {r.status_code} {r.text}")
    return r.json()

def get_project_id(base_url: str, project_name: str) -> int:
    url = f"{base_url.rstrip('/')}/api/projects"
    page = 1
    while True:
        data = get_json(url, params={"page": page, "page_size": 100, "name": project_name})
        for proj in data.get("results", []):
            if proj.get("name") == project_name:
                return proj["id"]
        if not data.get("next"):
            break
        page += 1
    raise ValueError(f"프로젝트를 찾지 못했습니다: name='{project_name}', org='{ORG_SLUG}'")

def iter_jobs_in_project(
    base_url: str, project_id: int, stage: Optional[str]=None, state: Optional[str]=None
) -> Iterable[Dict]:
    url = f"{base_url.rstrip('/')}/api/jobs"
    page = 1
    while True:
        params = {"page": page, "page_size": 100, "project_id": project_id}
        if stage: params["stage"] = stage
        if state: params["state"] = state

        data = get_json(url, params=params)
        for job in data.get("results", []):
            if job.get("project_id") != project_id:
                continue
            if stage and job.get("stage") != stage:
                continue
            if state and job.get("state") != state:  # <- 버그 수정 (state 비교)
                continue
            yield job

        if not data.get("next"):
            break
        page += 1

def patch_job_state(base_url: str, job_id: int, new_stage: str, new_state: str) -> Dict:
    url = f"{base_url.rstrip('/')}/api/jobs/{job_id}"
    payload = {"stage": new_stage, "state": new_state}
    if DRY_RUN:
        print(f"[DRY_RUN] PATCH {url} payload={payload}")
        return {}
    return patch_json(url, payload)

def main():
    print(f"Target: org='{ORG_SLUG}', project='{PROJECT_NAME}', filter=({FILTER_STAGE=}, {FILTER_STATE=}) -> ({NEW_STAGE=}, {NEW_STATE=})")
    proj_id = get_project_id(CVAT_URL, PROJECT_NAME)
    print(f"Project ID: {proj_id}")

    targets: List[Dict] = list(iter_jobs_in_project(CVAT_URL, proj_id, FILTER_STAGE, FILTER_STATE))
    if not targets:
        print("조건에 맞는 잡이 없습니다. (아무 것도 변경하지 않음)")
        return
    
    print(f"총 대상 잡 수: {len(targets)}")
    changed = 0
    for job in targets:
        jid = job["id"]
        print(f"- Job #{jid}: stage={job['stage']} state={job['state']} -> stage={NEW_STAGE}, state={NEW_STATE}")
        try:
            patch_job_state(CVAT_URL, jid, NEW_STAGE, NEW_STATE)
            changed += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"  ! 업데이트 실패 (job {jid}): {e}")

    if DRY_RUN:
        print(f"[DRY-RUN] 실제 변경 없음. 변경 예정 건수: {changed}/{len(targets)}")
    else:
        print(f"완료: 변경된 잡 수 {changed}/{len(targets)}")

if __name__ == "__main__":
    main()