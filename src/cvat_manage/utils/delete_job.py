import os
import csv
import argparse
import requests
from dotenv import load_dotenv
from typing import Optional, List, Tuple


# ===================== 사용자 설정 =====================
ORG = ""       # 어떤 ORG에서 지울지
PROJECT_NAME = ""     # 어떤 프로젝트의 job만 대상으로 할지
CSV_PATH = ""
REQUEST_TIMEOUT = 30

DRY_RUN = False

# ======================================================

# --- .env 로드 ---
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

# ---------- 유틸 ----------
def get_project_id(session: requests.Session, org: str, name: str) -> int:
    params = {"name": name, "page_size": 100}
    r = session.get(f"{CVAT_URL}/api/projects", headers=hdr_json(org),
                    params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code == 404:
        print(f"[hint] 404 from /api/projects with org='{org}'. ORG 이름 확인 필요.")
    r.raise_for_status()
    for p in r.json().get("results", []):
        if p.get("name") == name:
            return p["id"]
    raise RuntimeError(f"프로젝트를 찾지 못했습니다: org='{org}', name='{name}'")

def get_job_detail(session: requests.Session, org: str, job_id: int) -> dict:
    r = session.get(f"{CVAT_URL}/api/jobs/{job_id}",
                    headers=hdr_json(org), timeout=REQUEST_TIMEOUT)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()

# ---------- 동작(삭제/비우기) ----------
def delete_job(session: requests.Session, org: str, job_id: int, dry_run: bool = True):
    if dry_run:
        print(f"[DRY RUN] (하드삭제) job_id={job_id}")
        return
    url = f"{CVAT_URL}/api/jobs/{job_id}"
    r = session.delete(url, headers=hdr_json(org), timeout=REQUEST_TIMEOUT)
    if r.status_code in (200, 204):
        print(f"[OK] (하드삭제) job_id={job_id} 완료")
    elif r.status_code == 404:
        print(f"[WARN] job_id={job_id} 존재하지 않음 (이미 삭제됨?)")
    else:
        print(f"[ERROR] job_id={job_id} 삭제 실패 → {r.status_code} {r.text}")
        r.raise_for_status()

def clear_job_annotations(session: requests.Session, org: str, job_id: int, dry_run: bool = True):
    """
    어노테이션 전부 삭제 (소프트 삭제).
    CVAT는 DELETE /api/jobs/{id}/annotations 지원.
    """
    if dry_run:
        print(f"[DRY RUN] (소프트삭제: 어노테이션 비우기) job_id={job_id}")
        return
    url = f"{CVAT_URL}/api/jobs/{job_id}/annotations"
    r = session.delete(url, headers=hdr_json(org), timeout=REQUEST_TIMEOUT)
    if r.status_code in (200, 204):
        print(f"[OK] (소프트삭제) job_id={job_id} 어노테이션 삭제 완료")
    elif r.status_code == 404:
        print(f"[WARN] job_id={job_id} 또는 annotations 리소스 없음 (이미 비었을 수 있음)")
    else:
        print(f"[ERROR] job_id={job_id} 어노테이션 삭제 실패 → {r.status_code} {r.text}")
        r.raise_for_status()

# ---------- CSV 로드 ----------
def load_csv_targets(csv_path: str, target_project: str) -> List[Tuple[int, str]]:
    """
    CSV에서 target_project 행만 골라 (job_id, job_name) 목록 반환
    헤더: project_name, task_id, job_id, state, job_name
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV 파일을 찾을 수 없습니다: {csv_path}")
    out: List[Tuple[int, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        required = {"project_name", "task_id", "job_id", "state", "job_name"}
        if not required.issubset(rdr.fieldnames or {}):
            raise ValueError(f"CSV 헤더에 다음 컬럼이 필요합니다: {sorted(required)}")
        for row in rdr:
            if (row.get("project_name") or "").strip() != target_project:
                continue
            job_id_str = (row.get("job_id") or "").strip()
            if not job_id_str.isdigit():
                print(f"[SKIP] 잘못된 job_id: '{job_id_str}' (job_name='{row.get('job_name','').strip()}')")
                continue
            out.append((int(job_id_str), (row.get("job_name") or "").strip()))
    return out

# ---------- 메인 ----------
def run(csv_path: str, org: str, project_name: str, dry_run: bool):
    if not CVAT_URL or not TOKEN:
        raise RuntimeError("환경변수 CVAT_URL_2 / TOKEN_2가 필요합니다.")

    with requests.Session() as s:
        # ORG/PROJECT 유효성 체크
        try:
            _ = get_project_id(s, org, project_name)
        except Exception as e:
            print(f"[fatal] 프로젝트 확인 실패: {e}")
            return

        targets = load_csv_targets(csv_path, project_name)
        if not targets:
            print(f"(info) '{project_name}'에 해당하는 대상이 CSV에 없습니다.")
            return

        hard_del, soft_del, missing, blocked = 0, 0, 0, 0

        print(f"=== {'DRY RUN' if dry_run else 'APPLY'} 모드 | 대상 {len(targets)}건 ===")
        for job_id, job_name in targets:
            detail = get_job_detail(s, org, job_id)
            if not detail:
                print(f"[MISS] job_id={job_id}, name='{job_name}' → 존재하지 않음(404)")
                missing += 1
                continue

            jtype = (detail.get("type") or "").lower()
            if jtype == "ground_truth":
                # 하드 삭제
                delete_job(s, org, job_id, dry_run=dry_run)
                hard_del += 1
            else:
                # 타입 변경은 불가능 → 어노테이션만 비우는 소프트 삭제
                print(f"[INFO] job_id={job_id} type='{jtype}' → job 자체 삭제 불가. 어노테이션만 제거합니다.")
                clear_job_annotations(s, org, job_id, dry_run=dry_run)
                soft_del += 1

        print("\n=== 요약 ===")
        print(f"- 하드 삭제(ground_truth): {hard_del}건")
        print(f"- 소프트 삭제(annotations 삭제): {soft_del}건")
        print(f"- 미존재(404): {missing}건")
        if blocked:
            print(f"- 기타 차단: {blocked}건")

def parse_args():
    p = argparse.ArgumentParser(description="Delete jobs (ground_truth) or clear annotations for others")
    p.add_argument("--csv", default=CSV_PATH, help=f"입력 CSV 경로 (기본: {CSV_PATH})")
    p.add_argument("--org", default=ORG, help=f"ORG 지정 (기본: {ORG})")
    p.add_argument("--project", default=PROJECT_NAME, help=f"프로젝트명 지정 (기본: {PROJECT_NAME})")
    p.add_argument("--apply", action="store_true", help="실제 수행(기본은 DRY RUN)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    effective_dry_run = DRY_RUN and (not args.apply)
    run(args.csv, args.org, args.project, effective_dry_run)