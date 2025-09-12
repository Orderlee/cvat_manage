#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CVAT 프로젝트 내 '이미 할당된 Job'을 새 작업자 집합으로 균등 재배분하는 전용 스크립트

기능 개요
- 대상: 이미 assignee가 있는 Job 중 (stage=annotation AND state=new)
- 옵션:
  * --unassign_first : 재할당 전에 기존 assignee 해제
  * --use_all_users  : 조직의 모든 유저를 재배분 대상으로 자동 사용
  * --dry_run        : 계획만 출력 (PATCH 미수행)
  * --snapshot_only  : 현 분배 스냅샷만 출력
- 분배 로직:
  * 모든 유저가 최소 1개씩 갖도록 강제 분배
    - 잡 수 m >= 유저 수 k → 각 1개 선지급 후 나머지는 라운드로빈
    - 잡 수 m <  k        → 경고 후 앞의 m명만 1개
"""

import os
import argparse
from pathlib import Path
from collections import Counter, defaultdict
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import requests

# =========================
# 0) ENV 로드 (.env는 상위 폴더에 있다고 가정)
# =========================
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORGANIZATIONS = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()]

if not CVAT_URL or not TOKEN:
    raise RuntimeError("CVAT_URL_2 / TOKEN_2 환경변수(.env) 설정을 확인하세요.")

# =========================
# 1) 공통 유틸
# =========================
def _debug_http_error(prefix, res):
    print(f"[{prefix}] status={res.status_code}")
    try:
        print(f"[{prefix}] body.json=", res.json())
    except Exception:
        print(f"[{prefix}] body.text=", res.text)

def build_headers(org_slug: str):
    """CVAT 인증/조직 헤더 구성"""
    return {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json",
        "X-Organization": org_slug,
    }

def get_or_create_organization(name: str):
    """조직 조회(없으면 생성)"""
    headers = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}
    res = requests.get(f"{CVAT_URL}/api/organizations", headers=headers)
    res.raise_for_status()
    for org in res.json().get("results", []):
        if org["slug"] == name or org["name"] == name:
            return org["id"], org["slug"]
    slug = name.lower().replace(" ", "-")
    res = requests.post(f"{CVAT_URL}/api/organizations", headers=headers, json={"name": name, "slug": slug})
    res.raise_for_status()
    return res.json()["id"], slug

def preflight_check(headers, org_slug) -> bool:
    """동일 컨텍스트로 /api/tasks 접근 가능한지 사전 확인"""
    url = f"{CVAT_URL}/api/tasks?org={org_slug}"
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            print("✅ Preflight OK: /api/tasks GET authorized with org context")
            return True
        _debug_http_error("Preflight /api/tasks", res)
        return False
    except Exception as e:
        print("❌ Preflight exception:", e)
        return False

def fetch_all_list_api(base_url: str, headers, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    CVAT 리스트 API의 모든 페이지를 끝까지 수집하는 공용 헬퍼
    - 표준 응답: { "count": N, "next": URL or null, "previous": URL or null, "results": [...] }
    - 첫 요청은 (base_url, params)로 시작하고, 이후에는 'next' 절대/상대 URL을 그대로 따라감
    """
    items: List[Dict[str, Any]] = []
    url = base_url
    first_params = dict(params) if params else None

    while url:
        res = requests.get(url, headers=headers, params=first_params)
        res.raise_for_status()
        data = res.json()
        batch = data.get("results", []) or []
        items.extend(batch)

        # 다음 루프부터는 next URL을 그대로 사용하므로 params는 비움
        url = data.get("next")
        first_params = None

    return items

# =========================
# 2) 조회 함수
# =========================
def get_project_id_by_name(project_name: str, headers, org_slug: str):
    """프로젝트 이름으로 프로젝트 ID를 찾음 (동일명 다수면 최신 1개)"""
    url = f"{CVAT_URL}/api/projects?search={project_name}&org={org_slug}"
    res = requests.get(url, headers=headers)
    res.raise_for_status()
    results = res.json().get("results", [])
    if not results:
        raise ValueError(f"프로젝트를 찾을 수 없습니다: {project_name}")
    results.sort(key=lambda r: r.get("created_date", ""), reverse=True)
    return results[0]["id"], results[0]["name"]

def get_tasks_by_project(project_id: int, headers, org_slug: str, page_size: int = 100):
    """
    프로젝트 내 Task 목록 '전체 페이지' 수집
    """
    base_url = f"{CVAT_URL}/api/tasks"
    params = {
        "project_id": project_id,
        "org": org_slug,
        "page": 1,
        "page_size": page_size,
    }
    return fetch_all_list_api(base_url, headers, params)

def get_jobs(task_id: int, headers, org_slug: str, page_size: int = 100):
    """
    Task에 속한 Job 목록 '전체 페이지' 수집
    """
    base_url = f"{CVAT_URL}/api/jobs"
    params = {
        "task_id": task_id,
        "org": org_slug,
        "page": 1,
        "page_size": page_size,
    }
    return fetch_all_list_api(base_url, headers, params)

def get_all_users(headers, org_slug: str, page_size: int = 100):
    """
    조직 컨텍스트의 모든 유저를 '끝 페이지까지' 전부 수집
    - CVAT 응답: { "count": N, "next": URL or null, "previous": URL or null, "results": [...] }
    - next 가 존재하는 동안 계속 따라가며 누적
    """
    base_url = f"{CVAT_URL}/api/users"
    params = {
        "org": org_slug,
        "page": 1,
        "page_size": page_size,
    }
    return fetch_all_list_api(base_url, headers, params)

def map_usernames_to_ids(usernames, headers, org_slug: str):
    """새 작업자 username → user_id 매핑 (없으면 에러)"""
    all_users = get_all_users(headers, org_slug)
    id_map = {}
    for u in all_users:
        un = u.get("username")
        if un in usernames:
            id_map[un] = u.get("id")
    missing = [u for u in usernames if u not in id_map]
    if missing:
        raise ValueError(f"다음 username을 CVAT에서 찾지 못했습니다: {missing}")
    return id_map

# =========================
# 3) 스냅샷/재배분
# =========================
def print_project_assignment_snapshot(project_id: int, headers, org_slug: str):
    """현재 '이미 할당된 Job' 현황 요약 (사용자별 개수 및 예시 ID)"""
    tasks = get_tasks_by_project(project_id, headers, org_slug)
    user_counts = Counter()
    user_jobs = defaultdict(list)
    total_assigned = 0

    for t in tasks:
        jobs = get_jobs(t["id"], headers, org_slug)
        for j in jobs:
            if j.get("assignee"):
                uname = j["assignee"]["username"]
                user_counts[uname] += 1
                user_jobs[uname].append(j["id"])
                total_assigned += 1

    print(f"📊 프로젝트 {project_id} - 이미 할당된 Job 수: {total_assigned}")
    for uname, cnt in user_counts.most_common():
        ids = user_jobs[uname]
        preview = ", ".join(map(str, ids[:10])) + (" ..." if len(ids) > 10 else "")
        print(f" - {uname}: {cnt}개 (예: {preview})")

def redistribute_assigned_jobs_in_project(
    project_id: int,
    headers,
    org_slug: str,
    new_assignees: list,
    dry_run: bool = True,
    sort_key="id",
    unassign_first: bool = False,
):
    """
    1) 프로젝트 내 모든 Task의 Job 조회
    2) 대상: 이미 할당 + stage=annotation + state=new
    3) (옵션) 기존 할당 해제 → '모든 유저 최소 1개' 보장 분배
    """
    if not new_assignees:
        raise ValueError("new_assignees가 비었습니다. 최소 1명의 username을 지정하세요.")

    # (A) 대상 잡 수집
    tasks = get_tasks_by_project(project_id, headers, org_slug)
    assigned_jobs = []
    for t in tasks:
        jobs = get_jobs(t["id"], headers, org_slug)
        for j in jobs:
            if (
                j.get("assignee")
                and j.get("stage") == "annotation"
                and j.get("state") == "new"
            ):
                assigned_jobs.append(j)

    if not assigned_jobs:
        print("ℹ️ 조건(stage=annotation, state=new)을 만족하는 '이미 할당된 Job'을 찾지 못했습니다.")
        return

    # (A-1) (선택) 기존 할당 해제
    if unassign_first:
        print("🔓 기존 assignee 해제 단계 시작...")
        if dry_run:
            preview_ids = ", ".join(str(j["id"]) for j in assigned_jobs[:20])
            print(f"🧪 Dry-run: 아래 Job들의 assignee를 해제할 예정 (총 {len(assigned_jobs)}개) 예시: {preview_ids}{' ...' if len(assigned_jobs)>20 else ''}")
        else:
            for j in assigned_jobs:
                job_id = j["id"]
                try:
                    res = requests.patch(
                        f"{CVAT_URL}/api/jobs/{job_id}?org={org_slug}",
                        headers=headers,
                        json={"assignee": None},  # ← unassign
                    )
                    res.raise_for_status()
                    print(f"🔁 Job {job_id}: assignee 해제 완료")
                except requests.HTTPError as e:
                    print(f"⚠️ Job {job_id} assignee 해제 실패: {e.response.status_code} - {e.response.text}")

    # (B) 정렬 → 최소 1개 강제 분배 + 라운드로빈
    assigned_jobs.sort(key=lambda x: x.get(sort_key, 0))
    k = len(new_assignees)
    m = len(assigned_jobs)

    # username → id 매핑
    user_id_map = map_usernames_to_ids(new_assignees, headers, org_slug)

    # 초기 버킷 준비(유저명 → 잡 리스트)
    buckets = {uname: [] for uname in new_assignees}

    if m == 0:
        print("ℹ️ 분배할 Job이 없습니다. (필터 조건에 부합하는 '이미 할당된 Job' 0개)")
        return

    if m >= k:
        # 1) 모든 유저에게 1개씩 먼저 배분 (보장 분배)
        for i, uname in enumerate(new_assignees):
            buckets[uname].append(assigned_jobs[i])
        # 2) 남은 잡을 라운드로빈으로 배분
        remaining = assigned_jobs[k:]
        for idx, job in enumerate(remaining):
            uname = new_assignees[idx % k]
            buckets[uname].append(job)
    else:
        # m < k: 전원 1개씩 배분이 수학적으로 불가능
        print(f"⚠️ 잡 개수({m}) < 유저 수({k}) 이므로 전원에게 1개씩 배분할 수 없습니다.")
        print("   → 앞에서부터 순서대로 각 1개씩만 배분하고, 나머지 유저는 0개가 됩니다.")
        for i in range(m):
            uname = new_assignees[i]
            buckets[uname].append(assigned_jobs[i])

    # (D) 계획 출력
    print("====== 재배분 계획 (최소 1개 강제 분배 로직 적용) ======")
    for uname in new_assignees:
        job_ids = [j["id"] for j in buckets[uname]]
        preview = ", ".join(map(str, job_ids[:12])) + (" ..." if len(job_ids) > 12 else "")
        print(f" - {uname} ← {len(job_ids)} jobs: {preview}")
    print("=====================================================")

    if dry_run:
        print("🧪 Dry-run 모드: 실제 해제/재할당 PATCH는 수행하지 않았습니다.")
        return

    # (E) 실제 재할당 PATCH
    for uname in new_assignees:
        uid = user_id_map[uname]
        for job in buckets[uname]:
            job_id = job["id"]
            try:
                res = requests.patch(
                    f"{CVAT_URL}/api/jobs/{job_id}?org={org_slug}",
                    headers=headers,
                    json={"assignee": uid},
                )
                res.raise_for_status()
                print(f"✅ Job {job_id}: → {uname} (재할당 완료)")
            except requests.HTTPError as e:
                print(f"⚠️ Job {job_id} 재할당 실패: {e.response.status_code} - {e.response.text}")

# =========================
# 4) CLI
# =========================
def main():
    parser = argparse.ArgumentParser(description="CVAT 프로젝트 내 '이미 할당된 Job'을 새 작업자 집합으로 균등 재배분")
    parser.add_argument("--org_name", required=True, help="조직 이름 (예: YOUR_ORG)")
    gid = parser.add_mutually_exclusive_group(required=True)
    gid.add_argument("--project_id", type=int, help="대상 프로젝트 ID")
    gid.add_argument("--project_name", type=str, help="대상 프로젝트 이름")
    parser.add_argument("--use_all_users", action="store_true", help="조직 내 모든 유저를 재배분 대상으로 사용")
    # 여러 번 전달된 --new_assignees를 모두 수집하도록 (append)
    parser.add_argument(
        "--new_assignees",
        nargs="+",
        action="append",
        default=[],
        help="재배분 대상 작업자 username 목록 (공백 구분, 옵션 여러 번 사용 가능)"
    )
    parser.add_argument("--unassign_first", action="store_true", help="재할당 전에 기존 assignee를 해제")
    parser.add_argument("--dry_run", action="store_true", help="시뮬레이션만 수행 (기본 권장)")
    parser.add_argument("--snapshot_only", action="store_true", help="현재 분배 스냅샷만 출력하고 종료")
    args = parser.parse_args()

    # 조직 유효성(.env) 간단 체크 (있으면)
    if ORGANIZATIONS and args.org_name not in ORGANIZATIONS:
        raise ValueError(f"지정 조직({args.org_name})이 .env ORGANIZATIONS에 없습니다: {ORGANIZATIONS}")

    # 조직 컨텍스트 준비
    org_id, org_slug = get_or_create_organization(args.org_name)
    headers = build_headers(org_slug)

    # org 정보 출력
    print(f"🏢 조직 이름: {args.org_name}")
    print(f"🔑 org_slug: {org_slug}")
    print(f"🆔 org_id: {org_id}")

    preflight_check(headers, org_slug)

    # 프로젝트 식별
    if args.project_id:
        pid = int(args.project_id)
        pname = None
    else:
        pid, pname = get_project_id_by_name(args.project_name, headers, org_slug)

    if args.snapshot_only:
        print_project_assignment_snapshot(pid, headers, org_slug)
        return

    # 재배분 대상 사용자 구성
    if args.use_all_users:
        all_users = get_all_users(headers, org_slug)
        new_assignees = [u["username"] for u in all_users]
        print(f"ℹ️ 조직 내 전체 유저 {len(new_assignees)}명을 재배분 대상으로 사용합니다.")
    else:
        if not args.new_assignees:
            raise ValueError("--new_assignees 또는 --use_all_users 중 하나는 지정해야 합니다.")
        # [[u1,u2],[u3,u4], ...] → [u1,u2,u3,u4,...]
        new_assignees = [u for group in args.new_assignees for u in group]

    redistribute_assigned_jobs_in_project(
        project_id=pid,
        headers=headers,
        org_slug=org_slug,
        new_assignees=new_assignees,
        dry_run=args.dry_run,
        sort_key="id",                # 재현 가능한 분배를 위해 Job ID 기준 정렬
        unassign_first=args.unassign_first,  # 기존 할당 해제 여부
    )

if __name__ == "__main__":
    main()
