# # import os
# # import csv
# # import argparse
# # import requests
# # from datetime import datetime
# # from pathlib import Path
# # from dotenv import load_dotenv
# # from collections import defaultdict, Counter
# # import pandas as pd

# # # Load environment variables
# # env_path = Path(__file__).resolve().parent.parent / ".env"
# # load_dotenv(dotenv_path=env_path)

# # CVAT_URL = os.getenv("CVAT_URL_2")
# # TOKEN = os.getenv("TOKEN_2")
# # ORGANIZATION_LIST = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()]

# # DATE_FROM = os.getenv("DATE_FROM")
# # DATE_TO = os.getenv("DATE_TO")
# # DATE_FROM = datetime.strptime(DATE_FROM, "%Y-%m-%d") if DATE_FROM else None
# # DATE_TO = datetime.strptime(DATE_TO, "%Y-%m-%d") if DATE_TO else None

# # HEADERS = {
# #     "Authorization": f"Token {TOKEN}",
# #     "Content-Type": "application/json"
# # }

# # # ------------------------------------------------------------------------------------
# # # 유틸 함수들 - CVAT REST API 래퍼
# # # 초보자 Tip: 네트워크 요청은 항상 예외/404/None 입력을 방어하세요!
# # # ------------------------------------------------------------------------------------
# # def get_all_jobs():
# #     """모든 잡 목록을 페이지네이션으로 수집"""
# #     jobs = []
# #     page = 1
# #     while True:
# #         r = requests.get(f"{CVAT_URL}/api/jobs?page={page}", headers=HEADERS)
# #         r.raise_for_status()
# #         data = r.json()
# #         jobs.extend(data["results"])
# #         if not data["next"]:
# #             break
# #         page += 1
# #     return jobs

# # def get_task_name(task_id):
# #     """task_id로 태스크 이름 조회 (404/네트워크 예외 안전 처리)"""
# #     try:
# #         r = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=HEADERS)
# #         if r.status_code == 404:
# #             print(f"⚠️ Task ID {task_id} 없음 (404)")
# #             return f"(Unknown Task ID {task_id})"
# #         r.raise_for_status()
# #         return r.json().get("name", f"(No name, ID {task_id})")
# #     except requests.exceptions.RequestException as e:
# #         print(f"❌ Task 정보 요청 실패 (ID {task_id}): {e}")
# #         return f"(Error Task ID {task_id})"

# # # def get_project_name(project_id):
# # #     r = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=HEADERS)
# # #     r.raise_for_status()
# # #     return r.json().get("name", f"(No name, ID {project_id})")

# # def get_project_name(project_id):
# #     """
# #     project_id로 프로젝트 이름 조회.
# #     ▶ 핵심 수정:
# #       - project_id가 None/빈 값이면 즉시 표시용 문자열 반환 → /api/projects/None 호출 방지
# #       - 404/네트워크 예외를 캐치하여 파이프라인이 죽지 않도록 함
# #     """
# #     if not project_id:
# #         return "(None)"
    
# #     try:
# #         r = requests.get(f"{CVAT_URL}/api/projects/{project_id}", headers=HEADERS)
# #         if r.status_code == 404:
# #             print(f"⚠️ Project ID {project_id} 없음 (404)")
# #             return f"(Unknown Project ID {project_id})"
# #         r.raise_for_status()
# #         return r.json().get("name", f"(No name, ID {project_id})")
# #     except requests.exceptions.RequestException as e:
# #         print(f"❌ Project 정보 요청 실패 (ID {project_id}): {e}")
# #         return f"(Error Project ID {project_id})"



# # def get_organization_name(org_id):
# #     """organization slug 조회 (원래 코드도 None 가드가 있었음)"""
# #     if not org_id:
# #         return "(None)"
# #     r = requests.get(f"{CVAT_URL}/api/organizations/{org_id}", headers=HEADERS)
# #     if r.status_code == 404:
# #         return "(Not found)"
# #     r.raise_for_status()
# #     return r.json().get("slug", f"(No name, ID {org_id})")

# # def get_user_display_name(user_id):
# #     return os.getenv(f"USERMAP_{user_id}", user_id)

# # def get_job_labels(job_id):
# #     r = requests.get(f"{CVAT_URL}/api/labels?job_id={job_id}", headers=HEADERS)
# #     r.raise_for_status()
# #     return [l["name"] for l in r.json().get("results", [])]

# # def get_job_issues(job_id):
# #     r = requests.get(f"{CVAT_URL}/api/issues?job_id={job_id}", headers=HEADERS)
# #     r.raise_for_status()
# #     return [
# #         (i.get("frame", -1), i.get("message", "(no message)"))
# #         for i in r.json().get("results", [])
# #         if "frame" in i
# #     ]

# # def get_annotations(job_id):
# #     r = requests.get(f"{CVAT_URL}/api/jobs/{job_id}/annotations", headers=HEADERS)
# #     r.raise_for_status()
# #     return r.json().get("shapes", [])

# # def main(quiet=False):
# #     jobs = get_all_jobs()
    
# #     # 간단 캐시: 동일 ID 다회 조회 시 API 트래픽 절약
# #     task_cache, project_cache, org_cache = {}, {}, {}

# #     # 통계 구조
# #     org_proj_user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"total_jobs": 0, "completed_jobs": 0})))
# #     status_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
# #     results = []

# #     for job in jobs:
# #         # ▶ 안전하게 .get() 사용 (키 누락/None 모두 방어)
# #         task_id = job["task_id"]
# #         project_id = job["project_id"]
# #         org_id = job.get("organization")
# #         created_date = job.get("created_date")

# #         # 날짜 필터 (환경 변수로 기간 지정 가능)
# #         if DATE_FROM or DATE_TO:
# #             # CVAT의 created 형식 예: "2025-08-08T09:33:33.123Z"
# #             dt = datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%S.%fZ")
# #             if DATE_FROM and dt < DATE_FROM:
# #                 continue
# #             if DATE_TO and dt > DATE_TO:
# #                 continue
        
# #         # ----- 이름 캐싱 -----
# #         if task_id not in task_cache:
# #             task_cache[task_id] = get_task_name(task_id)
# #         if project_id not in project_cache:
# #             project_cache[project_id] = get_project_name(project_id) # ✅ None 안전
# #         if org_id not in org_cache:
# #             org_cache[org_id] = get_organization_name(org_id)

# #         task_name = task_cache[task_id]
# #         project_name = project_cache[project_id]
# #         org_name = org_cache[org_id]

# #         # 조직 필터링 (선택적)
# #         if ORGANIZATION_LIST and org_name not in ORGANIZATION_LIST:
# #             continue
        
# #         # 담당자 표시명
# #         assignee = job.get("assignee")
# #         assignee_username = assignee["username"] if assignee else "(Unassigned)"
# #         assignee_display = get_user_display_name(assignee_username)

# #         # 상태/스테이지
# #         stage = job.get("stage")
# #         state = job.get("state")

# #         # 통계 집계
# #         org_proj_user_stats[org_name][project_name][assignee_display]["total_jobs"] += 1
# #         status_stats[org_name][project_name][f"{stage} {state}"] += 1

# #         # 디테일 수집
# #         job_id = job["id"]  # id는 필수라 [] 사용
# #         annotations = get_annotations(job_id)
# #         label_count = len(annotations)
# #         issues = get_job_issues(job_id)
# #         labels = get_job_labels(job_id)

# #         # 완료 판단 (annotation/acceptance 스테이지에서 completed)
# #         if (stage == "annotation" and state == "completed") or (stage == "acceptance" and state == "completed"):
# #             org_proj_user_stats[org_name][project_name][assignee_display]["completed_jobs"] += 1

# #         # 프레임 범위/누락 프레임 계산
# #         frame_range = f"{job.get('start_frame', 0)}~{job.get('stop_frame', 0)}"

# #         total_frames = job.get("stop_frame", 0) - job.get("start_frame", 0) + 1
# #         annotated_frames = set(shape["frame"] for shape in annotations)
# #         missing_frames = [f for f in range(job.get("start_frame", 0), job.get("stop_frame", 0) + 1) if f not in annotated_frames]
# #         missing_count = len(missing_frames)
# #         missing_rate = round(missing_count / total_frames * 100, 2) if total_frames else 0

# #         results.append({
# #             "organization": org_name,
# #             "project": project_name,
# #             "task": task_name,
# #             "task_id": task_id,
# #             "assignee": assignee_display,
# #             "created": created_date,
# #             "state": state,
# #             "stage": stage,
# #             "labels": ", ".join(labels),
# #             "label_count": label_count,
# #             "issue_count": len(issues),
# #             "issues": "; ".join([f"Frame {f}: {m}" for f, m in issues]),
# #             "missing_count": missing_count,
# #             "missing_rate": missing_rate,
# #             "missing_frames": ", ".join(map(str, missing_frames)),
# #             "frame_range": frame_range
# #         })

# #     today_str = datetime.today().strftime("%Y-%m-%d")
# #     csv_dir = Path(__file__).resolve().parent / "csv"
# #     csv_dir.mkdir(parents=True, exist_ok=True)
# #     csv_filename = csv_dir / f"cvat_job_report_{today_str}.csv"

# #     with open(csv_filename, "w", newline="") as f:
# #         fieldnames = [
# #             "organization", "project", "task", "task_id", "assignee", "created",
# #             "state", "stage", "labels", "label_count",
# #             "issue_count", "issues", "missing_count", "missing_rate",
# #             "missing_frames", "frame_range"
# #         ]
# #         writer = csv.DictWriter(f, fieldnames=fieldnames)
# #         writer.writeheader()
# #         writer.writerows(results)

# #     if not quiet:
# #         print(f"\n📄 CSV 저장 완료: {csv_filename}")

# #     print("\n📌 Organization +  Project별 작업자 Completion Rate 요약:")
# #     for org, projects in org_proj_user_stats.items():
# #         print(f"\n🏢 [Organization: {org}]")
# #         for proj, users in projects.items():
# #             print("\n")
# #             print(f"📂 [Project: {proj}]")
# #             for user, stats in users.items():
# #                 total = stats["total_jobs"]
# #                 completed = stats["completed_jobs"]
# #                 rate = round(completed / total * 100, 2) if total else 0
# #                 print(f" - {user} → Job: {total}개 | Completed: {rate}% ({completed} / {total})")

# #     print("\n📌 Organization + Project별 Annotation Status Statistics:")
# #     for org, projects in status_stats.items():
# #         print(f"\n🏢 [Organization: {org}]")
# #         for proj, states in projects.items():
# #             total_count = sum(states.values())
# #             print("\n")
# #             print(f"📂 [Project: {proj}] Annotation Status Statistics: (총 {total_count}개)")
# #             for state_label, count in states.items():
# #                 percentage = round(count / total_count * 100, 2) if total_count else 0
# #                 print(f" - {state_label}: {count}개 ({percentage}%)")

# # if __name__ == "__main__":
# #     parser = argparse.ArgumentParser()
# #     parser.add_argument("--quiet", action="store_true", help="콘솔 출력 생략 (crontab용)")
# #     args = parser.parse_args()
# #     main(quiet=args.quiet)

# """
# omission.py — 지정된 Organization만 접근하도록 개선 (멀티 조직 루프 + 안전한 GET)
# - 목표: .env에 설정한 조직들에 대해서만 /api 호출을 수행하고, 그 외 조직의 리소스는 아예 조회하지 않음
# - 핵심 변경:
#   1) .env → ORGANIZATIONS=thailabeling,vietnamlabeling,piaspace (또는 CVAT_ORG_SLUG=단일)
#   2) 모든 GET은 Accept 협상(무/ */* / application/json) 폴백 적용, GET에 Content-Type 제거해 406 회피
#   3) 조직 컨텍스트를 헤더(X-Organization, X-Organization-ID) + 쿼리(org, org_id)에 동시 부착, slug/id 모두 시도
#   4) 조직별로 /api/jobs를 조회 → 해당 조직 Job만 순회 → Task/Project/Annotations도 동일 컨텍스트로 호출
#   5) 날짜 필터/통계/CSV 출력은 기존 동작 유지

# .env 예시
#   CVAT_URL_2=http://34.64.195.111:8080
#   TOKEN_2=xxxxx
#   ORGANIZATIONS=thailabeling,vietnamlabeling,piaspace  # 여러 조직 처리 (권장)
#   # 단일 조직만 처리하려면 (둘 중 하나만 사용)
#   # CVAT_ORG_SLUG=piaspace
#   # (선택) org_id가 필요한 서버용 매핑
#   # CVAT_ORG_ID_MAP=thailabeling:12,vietnamlabeling:13,piaspace:14
# """

# import os
# import csv
# import argparse
# import requests
# from datetime import datetime
# from pathlib import Path
# from dotenv import load_dotenv
# from collections import defaultdict
# from typing import Optional, Dict, Any, List

# # ============================
# # 0) 환경 변수 로딩
# # ============================
# env_path = Path(__file__).resolve().parent.parent / ".env"
# load_dotenv(dotenv_path=env_path)

# CVAT_URL = (os.getenv("CVAT_URL_2") or "").rstrip("/")
# TOKEN = os.getenv("TOKEN_2", "")

# # 멀티 조직: 쉼표 구분 슬러그 목록
# ORGANIZATION_LIST: List[str] = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()]
# # 단일 조직 강제 실행(선택)
# CVAT_ORG_SLUG = (os.getenv("CVAT_ORG_SLUG") or "").strip()
# # (선택) org_id 매핑: thailabeling:12,vietnamlabeling:13,piaspace:14
# _RAW_ID_MAP = (os.getenv("CVAT_ORG_ID_MAP") or "").strip()
# CVAT_ORG_ID_MAP: Dict[str, int] = {}
# if _RAW_ID_MAP:
#     for pair in _RAW_ID_MAP.split(","):
#         if ":" in pair:
#             slug, sid = pair.split(":", 1)
#             slug = slug.strip()
#             try:
#                 CVAT_ORG_ID_MAP[slug] = int(sid.strip())
#             except ValueError:
#                 pass

# DATE_FROM = os.getenv("DATE_FROM")
# DATE_TO = os.getenv("DATE_TO")
# DATE_FROM = datetime.strptime(DATE_FROM, "%Y-%m-%d") if DATE_FROM else None
# DATE_TO = datetime.strptime(DATE_TO, "%Y-%m-%d") if DATE_TO else None

# # ============================
# # 1) 공용 요청 유틸 (Accept/조직 폴백)
# # ============================

# def build_session(base_headers: Dict[str, str]) -> requests.Session:
#     sess = requests.Session()
#     sess.headers.update(base_headers)
#     return sess


# def make_base_headers(org_slug: str = "", org_id: Optional[int] = None, accept_variant: int = 0) -> Dict[str, str]:
#     """GET 기본 헤더 구성
#     - accept_variant: 0(없음) / 1("*/*") / 2("application/json")
#     - GET에는 Content-Type을 넣지 않음 (일부 서버 406 회피)
#     - 조직 헤더는 slug와 id를 함께 붙여 호환성 확보
#     """
#     headers = {"Authorization": f"Token {TOKEN}"}
#     if accept_variant == 1:
#         headers["Accept"] = "*/*"
#     elif accept_variant == 2:
#         headers["Accept"] = "application/json"
#     if org_slug:
#         headers["X-Organization"] = org_slug
#     if org_id is not None:
#         headers["X-Organization-ID"] = str(org_id)
#     return headers


# def with_org_params(params: Optional[Dict[str, Any]] = None, org_slug: str = "", org_id: Optional[int] = None) -> Dict[str, Any]:
#     params = dict(params or {})
#     if org_slug:
#         params.setdefault("org", org_slug)
#     if org_id is not None:
#         params.setdefault("org_id", org_id)
#     return params


# def get_json_with_fallback(path: str, org_slug: str, org_id: Optional[int], params: Optional[Dict[str, Any]] = None, timeout: float = 30.0) -> Dict[str, Any]:
#     """
#     폴백 로직으로 조직 컨텍스트/Accept 호환성 보장:
#       A) Accept: [없음 → */* → application/json]
#       B) 조직:  [header(org+id)+query(org+id) → header(id)+query(id) → header(org)+query(org)]
#     총 3x3 조합 시도. 마지막 실패 응답으로 에러 발생.
#     """
#     if not CVAT_URL:
#         raise RuntimeError("환경변수 CVAT_URL_2가 설정되지 않았습니다.")
#     url = f"{CVAT_URL}{path}"

#     org_attempts = [(True, True), (False, True), (True, False)]  # (use_slug, use_id)
#     accept_attempts = [0, 1, 2]  # 0:none, 1:*/*, 2:application/json

#     last_status = None
#     last_text = None

#     for accept_variant in accept_attempts:
#         for use_slug, use_id in org_attempts:
#             hdr = make_base_headers(
#                 org_slug if use_slug else "",
#                 org_id if (use_id and org_id is not None) else None,
#                 accept_variant=accept_variant,
#             )
#             sess = build_session(hdr)
#             prms = with_org_params(
#                 params,
#                 org_slug if use_slug else "",
#                 org_id if (use_id and org_id is not None) else None,
#             )
#             resp = sess.get(url, params=prms, timeout=timeout)
#             if resp.status_code == 200:
#                 try:
#                     return resp.json()
#                 except Exception:
#                     snippet = resp.text[:300] if resp.text else ""
#                     raise requests.HTTPError(f"JSON 파싱 실패: {url}\n본문: {snippet}")
#             last_status = resp.status_code
#             last_text = (resp.text or "")[:300]

#     raise requests.HTTPError(
#         f"조직/Accept 조합으로도 실패: {url} (org={org_slug}, org_id={org_id})\n마지막 응답: {last_status} {last_text}"
#     )

# # ============================
# # 2) CVAT API 헬퍼 (조직 컨텍스트 버전)
# # ============================

# def api_jobs(org_slug: str, org_id: Optional[int]) -> List[Dict[str, Any]]:
#     jobs: List[Dict[str, Any]] = []
#     page = 1
#     while True:
#         data = get_json_with_fallback("/api/jobs", org_slug, org_id, params={"page": page})
#         jobs.extend(data.get("results", []))
#         if not data.get("next"):
#             break
#         page += 1
#     return jobs


# def api_task(task_id: int, org_slug: str, org_id: Optional[int]) -> Dict[str, Any]:
#     return get_json_with_fallback(f"/api/tasks/{task_id}", org_slug, org_id)


# def api_project(project_id: Optional[int], org_slug: str, org_id: Optional[int]) -> str:
#     if not project_id:
#         return "(None)"
#     data = get_json_with_fallback(f"/api/projects/{project_id}", org_slug, org_id)
#     return data.get("name", f"(No name, ID {project_id})")


# def api_org_slug_from_id(org_id_val: Optional[int], org_slug: str, current_org_id: Optional[int]) -> str:
#     """참고용: 조직 ID → slug (동일 컨텍스트에서 조회). 실패 시 표시 문자열 반환."""
#     if not org_id_val:
#         return "(None)"
#     try:
#         data = get_json_with_fallback(f"/api/organizations/{org_id_val}", org_slug, current_org_id)
#         return data.get("slug", f"(No name, ID {org_id_val})")
#     except requests.RequestException:
#         return f"(org-{org_id_val})"


# def api_labels(job_id: int, org_slug: str, org_id: Optional[int]) -> List[str]:
#     data = get_json_with_fallback("/api/labels", org_slug, org_id, params={"job_id": job_id})
#     return [l.get("name") for l in data.get("results", [])]


# def api_issues(job_id: int, org_slug: str, org_id: Optional[int]) -> List[Dict[str, Any]]:
#     data = get_json_with_fallback("/api/issues", org_slug, org_id, params={"job_id": job_id})
#     return data.get("results", [])


# def api_annotations(job_id: int, org_slug: str, org_id: Optional[int]) -> Dict[str, Any]:
#     return get_json_with_fallback(f"/api/jobs/{job_id}/annotations", org_slug, org_id)

# # ============================
# # 3) 메인 로직 (조직별 루프)
# # ============================

# def main(quiet: bool = False):
#     # 실행 조직 목록 결정
#     if CVAT_ORG_SLUG:
#         if "," in CVAT_ORG_SLUG:
#             raise RuntimeError("CVAT_ORG_SLUG에는 하나의 슬러그만 설정하세요. 여러 조직은 ORGANIZATIONS를 사용합니다.")
#         org_list = [CVAT_ORG_SLUG]
#     else:
#         org_list = ORGANIZATION_LIST

#     if not org_list:
#         raise RuntimeError("실행할 조직이 없습니다. CVAT_ORG_SLUG 또는 ORGANIZATIONS를 설정하세요.")

#     # 통계 구조
#     org_proj_user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"total_jobs": 0, "completed_jobs": 0})))
#     status_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
#     results = []

#     for org_slug in org_list:
#         org_id = CVAT_ORG_ID_MAP.get(org_slug)  # (선택) 있으면 사용

#         if not quiet:
#             print("\n==============================")
#             print(f"🏢 조직 컨텍스트 시작: {org_slug}")
#             print("==============================\n")

#         # (A) 해당 조직의 Job만 조회
#         try:
#             jobs = api_jobs(org_slug, org_id)
#         except requests.RequestException as e:
#             print(f"❌ /api/jobs 조회 실패 (org={org_slug}): {e}")
#             continue

#         # (B) Job 순회 (이 시점부터 모든 상세 호출도 동일 조직 컨텍스트)
#         for job in jobs:
#             task_id = job.get("task_id")
#             project_id = job.get("project_id")
#             org_id_field = job.get("organization")  # 숫자 ID일 수 있음
#             created_date = job.get("created_date")

#             # 날짜 필터
#             if DATE_FROM or DATE_TO:
#                 # ISO 포맷 가변 대응
#                 dt = None
#                 raw = created_date or ""
#                 for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
#                     try:
#                         dt = datetime.strptime(raw, fmt)
#                         break
#                     except ValueError:
#                         pass
#                 if dt:
#                     if DATE_FROM and dt < DATE_FROM: 
#                         continue
#                     if DATE_TO and dt > DATE_TO:
#                         continue

#             # 이름/캐시
#             try:
#                 task_info = api_task(int(task_id), org_slug, org_id)
#                 task_name = task_info.get("name", f"(No name, ID {task_id})")
#             except requests.RequestException as e:
#                 print(f"⚠️ Task 조회 실패 (ID={task_id}, org={org_slug}): {e}")
#                 task_name = f"(Error Task ID {task_id})"

#             project_name = api_project(project_id, org_slug, org_id)
#             org_name = api_org_slug_from_id(org_id_field, org_slug, org_id)

#             # 지정된 조직 외는 출력에서도 배제 (이중 안전장치)
#             if org_name not in org_list:
#                 continue

#             # 담당자
#             assignee = job.get("assignee")
#             assignee_username = assignee.get("username") if assignee else "(Unassigned)"
#             assignee_display = os.getenv(f"USERMAP_{assignee_username}", assignee_username)

#             stage = job.get("stage")
#             state = job.get("state")

#             # 통계 집계
#             org_proj_user_stats[org_name][project_name][assignee_display]["total_jobs"] += 1
#             status_stats[org_name][project_name][f"{stage} {state}"] += 1

#             # 상세 수집 안전화
#             job_id = job.get("id")
#             try:
#                 ann = api_annotations(int(job_id), org_slug, org_id)
#                 shapes = ann.get("shapes", [])
#             except requests.RequestException as e:
#                 print(f"⚠️ Annotations 조회 실패 (job_id={job_id}, org={org_slug}): {e}")
#                 shapes = []

#             try:
#                 labels = api_labels(int(job_id), org_slug, org_id)
#             except requests.RequestException as e:
#                 print(f"⚠️ Labels 조회 실패 (job_id={job_id}, org={org_slug}): {e}")
#                 labels = []

#             try:
#                 issues = api_issues(int(job_id), org_slug, org_id)
#             except requests.RequestException as e:
#                 print(f"⚠️ Issues 조회 실패 (job_id={job_id}, org={org_slug}): {e}")
#                 issues = []

#             # 완료 판단
#             if (stage == "annotation" and state == "completed") or (stage == "acceptance" and state == "completed"):
#                 org_proj_user_stats[org_name][project_name][assignee_display]["completed_jobs"] += 1

#             # 누락 프레임 계산
#             start_f = job.get("start_frame", 0)
#             stop_f = job.get("stop_frame", 0)
#             total_frames = stop_f - start_f + 1 if stop_f >= start_f else 0

#             annotated_frames = {s.get("frame") for s in shapes if "frame" in s}
#             missing_frames = [f for f in range(start_f, stop_f + 1) if f not in annotated_frames] if total_frames else []
#             missing_count = len(missing_frames)
#             missing_rate = round(missing_count / total_frames * 100, 2) if total_frames else 0.0

#             results.append({
#                 "organization": org_name,
#                 "project": project_name,
#                 "task": task_name,
#                 "task_id": task_id,
#                 "assignee": assignee_display,
#                 "created": created_date,
#                 "state": state,
#                 "stage": stage,
#                 "labels": ", ".join(labels),
#                 "label_count": len(shapes),
#                 "issue_count": len(issues),
#                 "issues": "; ".join([f"Frame {i.get('frame', -1)}: {i.get('message', '(no message)')}" for i in issues]),
#                 "missing_count": missing_count,
#                 "missing_rate": missing_rate,
#                 "missing_frames": ", ".join(map(str, missing_frames)),
#                 "frame_range": f"{start_f}~{stop_f}",
#             })

#     # CSV 저장
#     today_str = datetime.today().strftime("%Y-%m-%d")
#     csv_dir = Path(__file__).resolve().parent / "csv"
#     csv_dir.mkdir(parents=True, exist_ok=True)
#     csv_filename = csv_dir / f"cvat_job_report_{today_str}1.csv"

#     with open(csv_filename, "w", newline="") as f:
#         fieldnames = [
#             "organization", "project", "task", "task_id", "assignee", "created",
#             "state", "stage", "labels", "label_count",
#             "issue_count", "issues", "missing_count", "missing_rate",
#             "missing_frames", "frame_range"
#         ]
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(results)

#     if not quiet:
#         print(f"\n📄 CSV 저장 완료: {csv_filename}")

#     # 요약 출력
#     print("\n📌 Organization +  Project별 작업자 Completion Rate 요약:")
#     for org, projects in org_proj_user_stats.items():
#         print(f"\n🏢 [Organization: {org}]")
#         for proj, users in projects.items():
#             print("\n")
#             print(f"📂 [Project: {proj}]")
#             for user, stats in users.items():
#                 total = stats["total_jobs"]
#                 completed = stats["completed_jobs"]
#                 rate = round(completed / total * 100, 2) if total else 0
#                 print(f" - {user} → Job: {total}개 | Completed: {rate}% ({completed} / {total})")

#     print("\n📌 Organization + Project별 Annotation Status Statistics:")
#     for org, projects in status_stats.items():
#         print(f"\n🏢 [Organization: {org}]")
#         for proj, states in projects.items():
#             total_count = sum(states.values())
#             print("\n")
#             print(f"📂 [Project: {proj}] Annotation Status Statistics: (총 {total_count}개)")
#             for state_label, count in states.items():
#                 percentage = round(count / total_count * 100, 2) if total_count else 0
#                 print(f" - {state_label}: {count}개 ({percentage}%)")


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--quiet", action="store_true", help="콘솔 출력 생략 (crontab용)")
#     args = parser.parse_args()
#     main(quiet=args.quiet)

"""
omission.py — 보고서 생성 속도 + 네트워크 트래픽 최적화 버전 (org slug 기반)
- labels / issues 상세 제거
- requests.Session() 전역 재사용
- ThreadPoolExecutor 로 Job 상세 병렬 처리
- 캐시(task, project, org) 적용
- missing_frames 전체 제거 → count, rate만 저장
- 조직 접근은 org(slug)만 사용 (org_id 제거, 404 방지)
"""

import os
import csv
import argparse
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================
# 0) 환경 변수 로딩
# ============================
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CVAT_URL = (os.getenv("CVAT_URL_2") or "").rstrip("/")
TOKEN = os.getenv("TOKEN_2", "")

# 여러 조직 지원
ORGANIZATION_LIST: List[str] = [org.strip() for org in os.getenv("ORGANIZATIONS", "").split(",") if org.strip()]
# 단일 조직 (선택)
CVAT_ORG_SLUG = (os.getenv("CVAT_ORG_SLUG") or "").strip()

DATE_FROM = os.getenv("DATE_FROM")
DATE_TO = os.getenv("DATE_TO")
DATE_FROM = datetime.strptime(DATE_FROM, "%Y-%m-%d") if DATE_FROM else None
DATE_TO = datetime.strptime(DATE_TO, "%Y-%m-%d") if DATE_TO else None

# ============================
# 1) 전역 세션 (재사용)
# ============================
SESSION = requests.Session()
SESSION.headers.update({"Authorization": f"Token {TOKEN}"})


def with_org_params(params: Optional[Dict[str, Any]] = None,
                    org_slug: str = "") -> Dict[str, Any]:
    """조직 파라미터: slug만 사용"""
    params = dict(params or {})
    if org_slug:
        params.setdefault("org", org_slug)
    return params


def build_headers(org_slug: str = "") -> Dict[str, str]:
    """조직 헤더: slug만 사용"""
    headers = {"Authorization": f"Token {TOKEN}"}
    if org_slug:
        headers["X-Organization"] = org_slug
    return headers


def get_json(path: str, org_slug: str,
             params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{CVAT_URL}{path}"
    headers = build_headers(org_slug)
    resp = SESSION.get(url, headers=headers,
                       params=with_org_params(params, org_slug),
                       timeout=20)
    resp.raise_for_status()
    return resp.json()

# ============================
# 2) API 호출
# ============================

def api_jobs(org_slug: str) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    page = 1
    while True:
        data = get_json("/api/jobs", org_slug,
                        params={"page": page, "page_size": 100})
        jobs.extend(data.get("results", []))
        if not data.get("next"):
            break
        page += 1
    return jobs


def api_task(task_id: int, org_slug: str) -> Dict[str, Any]:
    return get_json(f"/api/tasks/{task_id}", org_slug)


def api_project(project_id: Optional[int], org_slug: str) -> str:
    if not project_id:
        return "(None)"
    data = get_json(f"/api/projects/{project_id}", org_slug)
    return data.get("name", f"(No name, ID {project_id})")


def api_org_slug_from_id(org_id_val: Optional[int], org_slug: str) -> str:
    if not org_id_val:
        return "(None)"
    try:
        data = get_json(f"/api/organizations/{org_id_val}", org_slug)
        return data.get("slug", f"(No name, ID {org_id_val})")
    except requests.RequestException:
        return f"(org-{org_id_val})"


def api_annotations(job_id: int, org_slug: str) -> Dict[str, Any]:
    return get_json(f"/api/jobs/{job_id}/annotations", org_slug)

# ============================
# 3) 캐시
# ============================
task_cache, project_cache, org_cache = {}, {}, {}

# ============================
# 4) Job 상세 처리
# ============================
def fetch_job_details(job, org_slug):
    task_id, project_id, org_id_field = job.get("task_id"), job.get("project_id"), job.get("organization")

    # Task 캐시
    if task_id not in task_cache:
        try:
            task_cache[task_id] = api_task(int(task_id), org_slug).get("name", f"(No name {task_id})")
        except:
            task_cache[task_id] = f"(Error Task {task_id})"

    # Project 캐시
    if project_id not in project_cache:
        project_cache[project_id] = api_project(project_id, org_slug)

    # Org 캐시
    if org_id_field not in org_cache:
        org_cache[org_id_field] = api_org_slug_from_id(org_id_field, org_slug)

    task_name, project_name, org_name = task_cache[task_id], project_cache[project_id], org_cache[org_id_field]
    assignee = job.get("assignee")
    assignee_display = assignee.get("username") if assignee else "(Unassigned)"

    stage, state = job.get("stage"), job.get("state")

    # Annotation (frame 기반 통계만)
    try:
        ann = api_annotations(int(job["id"]), org_slug)
        shapes = ann.get("shapes", [])
    except:
        shapes = []

    start_f, stop_f = job.get("start_frame", 0), job.get("stop_frame", 0)
    total_frames = stop_f - start_f + 1 if stop_f >= start_f else 0
    annotated_frames = {s.get("frame") for s in shapes if "frame" in s}
    missing_count = total_frames - len(annotated_frames)
    missing_rate = round(missing_count / total_frames * 100, 2) if total_frames else 0

    return {
        "organization": org_name,
        "project": project_name,
        "task": task_name,
        "task_id": task_id,
        "assignee": assignee_display,
        "created": job.get("created_date"),
        "state": state,
        "stage": stage,
        "label_count": len(shapes),   # Annotation shape 개수
        "missing_count": missing_count,
        "missing_rate": missing_rate,
        "frame_range": f"{start_f}~{stop_f}",
    }

# ============================
# 5) 메인
# ============================
def main(quiet: bool = False):
    if CVAT_ORG_SLUG:
        if "," in CVAT_ORG_SLUG:
            raise RuntimeError("CVAT_ORG_SLUG에는 하나만 설정하세요. 여러 조직은 ORGANIZATIONS 사용")
        org_list = [CVAT_ORG_SLUG]
    else:
        org_list = ORGANIZATION_LIST
    if not org_list:
        raise RuntimeError("실행할 조직 없음. CVAT_ORG_SLUG 또는 ORGANIZATIONS 설정 필요")

    org_proj_user_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"total_jobs": 0, "completed_jobs": 0})))
    status_stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    results = []

    for org_slug in org_list:
        if not quiet:
            print(f"\n🏢 조직 컨텍스트 시작: {org_slug}")

        try:
            jobs = api_jobs(org_slug)
        except requests.RequestException as e:
            print(f"❌ /api/jobs 실패 (org={org_slug}): {e}")
            continue

        # 병렬 처리
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(fetch_job_details, job, org_slug) for job in jobs]
            for future, job in zip(as_completed(futures), jobs):
                data = future.result()
                results.append(data)

                # 통계 집계
                org_proj_user_stats[data["organization"]][data["project"]][data["assignee"]]["total_jobs"] += 1
                status_stats[data["organization"]][data["project"]][f"{data['stage']} {data['state']}"] += 1
                if (data["stage"] == "annotation" and data["state"] == "completed") or \
                   (data["stage"] == "acceptance" and data["state"] == "completed"):
                    org_proj_user_stats[data["organization"]][data["project"]][data["assignee"]]["completed_jobs"] += 1

    # CSV 저장
    today_str = datetime.today().strftime("%Y-%m-%d")
    csv_dir = Path(__file__).resolve().parent / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_filename = csv_dir / f"cvat_job_report_{today_str}.csv"

    with open(csv_filename, "w", newline="") as f:
        fieldnames = [
            "organization", "project", "task", "task_id", "assignee", "created",
            "state", "stage", "label_count",
            "missing_count", "missing_rate", "frame_range"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    if not quiet:
        print(f"\n📄 CSV 저장 완료: {csv_filename}")

    # 요약 출력
    print("\n📌 Organization + Project별 작업자 Completion Rate 요약:")
    for org, projects in org_proj_user_stats.items():
        print(f"\n🏢 [Organization: {org}]")
        for proj, users in projects.items():
            print(f"📂 [Project: {proj}]")
            for user, stats in users.items():
                total, completed = stats["total_jobs"], stats["completed_jobs"]
                rate = round(completed / total * 100, 2) if total else 0
                print(f" - {user} → Job: {total}개 | Completed: {rate}% ({completed}/{total})")

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
