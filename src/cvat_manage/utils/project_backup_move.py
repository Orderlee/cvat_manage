"""
CVAT 프로젝트 백업 → 다른 조직으로 복원 자동화 스크립트 (서버/버전 호환형)

핵심 포인트
- .env 안전 로드(경로 빗나감 방지) + 인증 부트스트랩(401 원인 즉시 진단)
- 최신 플로우: POST /api/projects/{id}/backup/export → GET /api/requests/{rq_id} 폴링
             → GET /api/projects/{id}/backup?action=download&location=local&filename=...
- 레거시 플로우(롱폴링): GET /api/projects/{id}/backup  (202 → 준비중, 200/201 → ZIP 응답)
- 일부 서버/프록시에서 406/410이 날 수 있어 Accept/쿼리파라미터/폴백 순서로 안전하게 처리
"""

import os
import time
import glob
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

import requests
from dotenv import load_dotenv, find_dotenv

# ===================== 사용자 설정 =====================
# 원본 조직(SRC_ORG)에서 프로젝트를 백업 → 대상 조직(DST_ORG)에 복원합니다.
SRC_ORG = 
DST_ORG = 

# 이동(백업)할 프로젝트 이름들 (정확한 "프로젝트 이름"과 일치해야 합니다)
PROJECTS_TO_MOVE: List[str] = []

# 백업 ZIP 보관 폴더
BACKUP_DIR = Path.cwd() / "cvat_backups"
BACKUP_DIR.mkdir(exist_ok=True)

# 로그 파일(날짜별 파일 생성)
LOG_FILE = BACKUP_DIR / f"{datetime.now().strftime('%Y%m%d')}_result.log"

# 요청/폴링 타임아웃 설정
REQUEST_TIMEOUT = 30           # HTTP 요청 타임아웃(초)
RQ_POLL_INTERVAL = 2.0         # 비동기 요청 상태 폴링 간격(초)
RQ_POLL_TIMEOUT = 900          # 비동기 요청 최대 대기시간(초) = 15분
# ======================================================


# -------------------- 공통 유틸 --------------------
def log(msg: str) -> None:
    """터미널과 파일에 동시에 로그를 남깁니다."""
    print(msg, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} {msg}\n")


def mask(s: Optional[str], keep: int = 4) -> str:
    """민감정보(토큰)를 로그에 찍을 때 앞/뒤 일부만 남기고 마스킹합니다."""
    if not s:
        return "<EMPTY>"
    if len(s) <= keep * 2:
        return s[0] + "*" * (len(s) - 2) + s[-1]
    return s[:keep] + "*" * (len(s) - keep * 2) + s[-keep:]


def load_env_safely() -> tuple[Optional[str], Optional[str]]:
    """
    .env를 안전하게 로드합니다.
    - 1차: 현재 파일의 부모 폴더 상단에 있는 .env 시도
    - 2차: 작업 디렉터리 기준으로 .env 자동 탐색(find_dotenv)
    환경변수:
      CVAT_URL_2 = "http://<host>:<port>"
      TOKEN_2    = "<your-token>"
    """
    # 1) 프로젝트 구조에 따라 상위에 .env가 있는 경우
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)

    # 2) 현재 작업경로 기준 자동 탐색 (백업 플랜)
    found = find_dotenv(usecwd=True)
    if found:
        load_dotenv(found, override=True)

    url = os.getenv("CVAT_URL_2")
    token = os.getenv("TOKEN_2")
    return url, token


CVAT_URL, TOKEN = load_env_safely()


def sanity_log_env() -> None:
    """
    환경변수 로드 결과를 점검하고, 필수 값이 없으면 즉시 종료합니다.
    - 401의 최빈 원인은 .env 로드 실패로 TOKEN이 비어있는 경우입니다.
    """
    log(f"[SANITY] CVAT_URL={CVAT_URL}")
    log(f"[SANITY] SRC_ORG={SRC_ORG}, DST_ORG={DST_ORG}")
    log(f"[SANITY] TOKEN(len)={len(TOKEN) if TOKEN else 0}, TOKEN(mask)={mask(TOKEN)}")
    if not CVAT_URL or not TOKEN:
        raise SystemExit("❌ CVAT_URL_2 또는 TOKEN_2 가 비어 있습니다. .env 경로/변수명을 확인하세요.")


def auth_headers(org: Optional[str] = None, is_json: bool = True) -> Dict[str, str]:
    """
    CVAT API 공통 헤더 생성
    - Authorization: Token <TOKEN>
    - X-Organization: <org> (조직 격리 기능 사용 시 필요)
    - is_json=True이면 'Content-Type: application/json' 추가
    """
    headers = {
        "Authorization": f"Token {TOKEN}",
    }
    if org:
        headers["X-Organization"] = org
    if is_json:
        headers["Content-Type"] = "application/json"
    return headers


# -------------------- 인증 부트스트랩 (401 빠른 진단) --------------------
def whoami(session: requests.Session, org: str | None = None):
    """
    현재 토큰으로 로그인 사용자 정보 확인 (200이어야 정상)
    ✅ 올바른 경로: /api/users/self  (예전처럼 /api/auth/users/self 아님)
    """
    headers = {"Authorization": f"Token {TOKEN}"}
    if org:
        headers["X-Organization"] = org
    # 엔드포인트 수정!
    r = session.get(f"{CVAT_URL}/api/users/self", headers=headers, timeout=REQUEST_TIMEOUT)
    return r


def list_orgs(session: requests.Session) -> requests.Response:
    """현재 토큰이 접근 가능한 조직 목록 반환"""
    r = session.get(f"{CVAT_URL}/api/organizations",
                    headers={"Authorization": f"Token {TOKEN}"},
                    timeout=REQUEST_TIMEOUT)
    return r


def bootstrap_auth_check(session: requests.Session) -> None:
    """
    1) .env 로드 점검
    2) whoami(org 포함) 확인 → 실패 시 whoami(org 미포함)로 토큰 자체 검증
    3) organizations 조회하여 SRC_ORG가 실제로 접근 가능한 조직인지 확인
    4) 프록시가 Authorization 헤더를 제거하는 경우 사용자에게 안내
    """
    sanity_log_env()

    # ORG 포함 whoami (조직 격리 케이스에서 권장)
    r = whoami(session, SRC_ORG)
    if r.status_code == 200:
        data = r.json()
        log(f"👤 whoami OK (ORG={SRC_ORG}): id={data.get('id')}, username={data.get('username')}")
    else:
        log(f"⚠ whoami with ORG failed: {r.status_code} {r.text[:300]}")
        wa = r.headers.get("WWW-Authenticate")
        if wa:
            log(f"[Header] WWW-Authenticate: {wa}")

        # ORG 미포함으로 토큰 자체 검증
        r2 = whoami(session, org=None)
        if r2.status_code != 200:
            log(f"❌ whoami without ORG also failed: {r2.status_code} {r2.text[:300]}")
            raise SystemExit(
                "인증 실패: 토큰이 유효하지 않거나, 프록시가 Authorization 헤더를 차단 중입니다.\n"
                " - 토큰이 해당 CVAT_URL 에서 발급된 것이 맞는지 확인하세요.\n"
                " - Nginx 등 프록시를 사용 중이면 'proxy_set_header Authorization $http_authorization;' 설정이 필요할 수 있습니다."
            )
        else:
            data = r2.json()
            log(f"👤 whoami (no ORG) OK: id={data.get('id')}, username={data.get('username')}")

    # 접근 가능한 조직 확인 (슬러그/이름이 무엇으로 오는지 로그)
    r_orgs = list_orgs(session)
    if r_orgs.status_code == 200:
        orgs = r_orgs.json().get("results", [])
        slugs = [o.get("slug") or o.get("name") for o in orgs]
        log(f"🏷 접근 가능한 조직(SLUG/NAME): {slugs}")
        if SRC_ORG not in slugs:
            raise SystemExit(f"SRC_ORG='{SRC_ORG}' 조직에 속해있지 않습니다. 위 목록 중 하나로 바꾸세요.")
        if DST_ORG not in slugs:
            log(f"ℹ️ 참고: DST_ORG='{DST_ORG}'는 현재 토큰 사용자 소속이 아닐 수 있습니다(복원 단계에서 권한 오류 발생 가능).")
    else:
        log(f"⚠ organizations 조회 실패: {r_orgs.status_code} {r_orgs.text[:200]}")
        log("조직 목록을 불러오지 못했습니다. ORG 슬러그 철자/권한을 다시 확인하세요.")


# -------------------- 비동기 요청 폴링 --------------------
def poll_request(session: requests.Session, rq_id: str, org: Optional[str]) -> None:
    """
    CVAT 비동기 요청 상태를 폴링합니다.
    - 최신 API 경로: GET /api/requests/{rq_id}
    - 응답이 버전에 따라 'status' 또는 'state' 를 사용할 수 있어 모두 대응
    - 'finished' → 성공, 'failed' → 예외
    """
    start_time = time.time()
    while True:
        r = session.get(f"{CVAT_URL}/api/requests/{rq_id}",
                        headers=auth_headers(org),
                        timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        status = (data.get("status") or data.get("state") or "").lower()

        if status == "finished":
            return
        if status == "failed":
            raise RuntimeError(f"요청 실패: {data}")

        if time.time() - start_time > RQ_POLL_TIMEOUT:
            raise TimeoutError(f"요청 타임아웃: {rq_id}")

        time.sleep(RQ_POLL_INTERVAL)

def _safe_json_get(session: requests.Session, url: str, org: str, params: dict | None = None) -> dict:
    """
    JSON 응답을 받기 위한 안전한 GET 호출 헬퍼.
    - 일부 서버/프록시에서 Accept 헤더가 까다로워 406(Not Acceptable)이 발생할 수 있음.
    - 1) 헤더 최소화(Authorization + X-Organization만)로 시도
    - 2) 실패 시 Accept='*/*'로 1회 재시도
    - 3) 그래도 안 되면 Accept='application/json, text/plain, */*'로 최종 재시도
    - 성공하면 r.json() 반환, 아니면 raise_for_status()

    왜 이렇게?
    - 406은 보통 '원하는 형식으로 줄 수 없다'는 뜻인데, 어떤 배포본/Nginx 설정은
      Accept가 특정 값일 때만 JSON을 내주거나, 오히려 특정 값을 싫어합니다.
      그래서 '가능한 한 비우고' → '가장 느슨하게' → '명시적으로 JSON' 순으로 갑니다.
    """
    # 1) 가장 느슨한 형태: Accept/Content-Type 없이 (is_json=False)
    headers_min = auth_headers(org, is_json=False)  # Authorization + X-Organization 만 보냄
    r = session.get(url, headers=headers_min, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code == 200:
        return r.json()
    if r.status_code != 406:
        r.raise_for_status()
    r.close()

    # 2) Accept='*/*' 로 재시도
    headers_star = dict(headers_min)
    headers_star["Accept"] = "*/*"
    r2 = session.get(url, headers=headers_star, params=params, timeout=REQUEST_TIMEOUT)
    if r2.status_code == 200:
        return r2.json()
    if r2.status_code != 406:
        r2.raise_for_status()
    r2.close()

    # 3) 최종: JSON 선호를 명시
    headers_json = dict(headers_min)
    headers_json["Accept"] = "application/json, text/plain, */*"
    r3 = session.get(url, headers=headers_json, params=params, timeout=REQUEST_TIMEOUT)
    r3.raise_for_status()  # 여기서도 실패면 진짜 에러로 본다
    return r3.json()

# -------------------- 프로젝트 목록/백업/복원 --------------------
def list_projects(session: requests.Session, org: str) -> list[dict]:
    """
    프로젝트 목록을 페이지네이션으로 모두 가져옵니다.
    ✅ 포인트: GET에 굳이 'Content-Type'이나 특정 'Accept'를 고정하지 않도록 _safe_json_get 사용
    """
    results: list[dict] = []
    page = 1
    while True:
        data = _safe_json_get(
            session,
            f"{CVAT_URL}/api/projects",
            org,
            params={"page": page, "page_size": 100}
        )
        results.extend(data.get("results", []))
        if not data.get("next"):
            break
        page += 1
    return results


def _stream_to_file(resp: requests.Response, zip_path: Path) -> None:
    """
    스트리밍 응답을 안전하게 파일로 저장합니다.
    - 큰 파일도 메모리 과점 없이 chunk 단위로 기록
    """
    with open(zip_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1MB
            if chunk:
                f.write(chunk)


def export_project(session: requests.Session, project_id: int, org: str, zip_path: Path) -> None:
    """
    단일 프로젝트 ZIP 백업 (서버/버전 호환형)
    1) (권장/최신) export 트리거:
        POST /api/projects/{id}/backup/export  → rq_id 수신
        → GET /api/requests/{rq_id} 폴링(완료까지 대기)
    2) 완료 후 다운로드:
        GET /api/projects/{id}/backup?action=download&location=local&filename=...
        - 일부 서버는 위 파라미터가 없으면 410(Gone)을 반환할 수 있음
        - 202면 준비중 → 재시도(롱폴링), 200/201 이면 ZIP 스트리밍 저장
    3) (폴백) 레거시 GET 롱폴링:
        파라미터 없이 GET /api/projects/{id}/backup  (202→대기, 200/201→ZIP)
    """
    url_get = f"{CVAT_URL}/api/projects/{project_id}/backup"
    url_export = f"{CVAT_URL}/api/projects/{project_id}/backup/export"

    log(f"📦 백업 시작: Project ID={project_id}")

    # (0) 다운로드 시 406 회피를 위해 Accept를 명시하지 않습니다(*/* 기본).
    #     Content-Type 도 굳이 보낼 필요 없어 is_json=False 로 둡니다.
    headers_bin = auth_headers(org, is_json=False)

    # (1) 백업 생성 요청 (export 트리거)
    r_init = session.post(
        url_export,
        headers=auth_headers(org),  # JSON 헤더
        json={"filename": zip_path.name},  # 서버가 파일명을 기억하는 배포본 호환성 ↑ (없어도 동작)
        timeout=REQUEST_TIMEOUT,
    )

    # 어떤 구버전은 /backup/export 자체가 없을 수 있음(404/405)
    if r_init.status_code in (404, 405):
        log(f"ℹ️ export 엔드포인트 미지원({r_init.status_code}) → 레거시 GET 롱폴링으로 진행")
        return _export_project_legacy_get(session, project_id, org, zip_path)

    r_init.raise_for_status()
    data = r_init.json()
    rq_id = data.get("rq_id")
    if not rq_id:
        raise RuntimeError(f"rq_id를 받지 못했습니다: {data}")

    # (2) 백그라운드 작업 완료까지 대기
    poll_request(session, rq_id, org)

    # (3) 결과 다운로드 (action=download & location=local & filename=...)
    params = {
        "action": "download",
        "location": "local",
        "filename": zip_path.name,
    }
    start_dl = time.time()
    while True:
        r_file = session.get(url_get, headers=headers_bin, params=params,
                             timeout=REQUEST_TIMEOUT, stream=True)

        if r_file.status_code in (200, 201):
            _stream_to_file(r_file, zip_path)
            log(f"✅ 백업 완료: {zip_path}")
            return

        if r_file.status_code == 202:
            # 아직 파일 준비중 → 조금 기다렸다가 다시 요청
            r_file.close()
            if time.time() - start_dl > RQ_POLL_TIMEOUT:
                raise TimeoutError(f"백업 파일 생성 지연: project={project_id}")
            time.sleep(RQ_POLL_INTERVAL)
            continue

        if r_file.status_code == 410:
            # 일부 서버는 파라미터가 맞지 않거나 파일이 일시 소멸된 상태에서 410을 반환할 수 있음
            r_file.close()
            log("↻ 410 응답 → 레거시 GET 롱폴링으로 재시도")
            return _export_project_legacy_get(session, project_id, org, zip_path)

        if r_file.status_code == 406:
            # 콘텐츠 협상 실패: Accept 문제로 간주하고 1회만 application/zip로 재시도
            r_file.close()
            log("↻ 406 응답 → Accept=application/zip 로 재시도")
            headers_zip = dict(headers_bin)
            headers_zip["Accept"] = "application/zip"
            r_zip = session.get(url_get, headers=headers_zip, params=params,
                                timeout=REQUEST_TIMEOUT, stream=True)
            r_zip.raise_for_status()
            _stream_to_file(r_zip, zip_path)
            log(f"✅ 백업 완료(406 재시도): {zip_path}")
            return

        # 그 외 에러는 그대로 예외
        r_file.raise_for_status()


def _export_project_legacy_get(session: requests.Session, project_id: int, org: str, zip_path: Path) -> None:
    """
    레거시 서버용: GET /api/projects/{id}/backup 롱폴링 + 410 재생성 루프
    - 파라미터를 명시해야 하는 배포본이 있어 action/location/filename을 넣습니다.
    - 202: 준비중 → 재시도
    - 410: 준비/캐시 파일 소멸 → 같은 요청으로 다시 '생성 트리거'가 걸리므로 재시도
    - 200/201: ZIP 본문 스트리밍 저장
    참고:
      - 백업 GET 규약(202→201/200): CVAT 매뉴얼 백업 문서. 
      - 쿼리 파라미터(action/location/filename): SDK 인터페이스 정의. 
    """
    url_get = f"{CVAT_URL}/api/projects/{project_id}/backup"

    # 헤더는 최소화(Authorization + X-Organization). Accept/Content-Type 강제 X
    headers_bin = auth_headers(org, is_json=False)

    # 다운로드 파라미터 명시 (일부 배포에서 필수)
    params = {
        "action": "download",
        "location": "local",          # 로컬로 내려받기 (클라우드 저장소 전송 아님)
        "filename": zip_path.name,    # 파일명 힌트 (없어도 되지만 호환성 ↑)
        # "use_default_location": "true",  # 필요시 해제 주석을 풀어 사용
    }

    start = time.time()
    while True:
        r = session.get(url_get, headers=headers_bin, params=params, timeout=REQUEST_TIMEOUT, stream=True)

        # 준비 완료 → 저장
        if r.status_code in (200, 201):
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            log(f"✅ 백업 완료(레거시): {zip_path}")
            return

        # 준비 중 → 기다렸다가 반복
        if r.status_code == 202:
            r.close()
            if time.time() - start > RQ_POLL_TIMEOUT:
                raise TimeoutError(f"레거시 GET 대기 타임아웃: project={project_id}")
            time.sleep(RQ_POLL_INTERVAL)
            continue

        # 캐시 만료/소멸 등 → 같은 요청으로 다시 생성 트리거가 걸리니 잠깐 대기 후 재시도
        if r.status_code == 410:
            r.close()
            log("↻ 410(Gone) → 백업 파일 재생성 트리거 후 재시도")
            if time.time() - start > RQ_POLL_TIMEOUT:
                raise TimeoutError(f"레거시 GET(410 재생성) 타임아웃: project={project_id}")
            time.sleep(RQ_POLL_INTERVAL)
            continue

        # 드물게 Accept 문제로 406이 나오면 1회만 application/zip로 재시도
        if r.status_code == 406:
            r.close()
            log("↻ 406 → Accept=application/zip 로 1회 재시도")
            headers_zip = dict(headers_bin)
            headers_zip["Accept"] = "application/zip"
            r2 = session.get(url_get, headers=headers_zip, params=params, timeout=REQUEST_TIMEOUT, stream=True)
            r2.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r2.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            log(f"✅ 백업 완료(406 재시도): {zip_path}")
            return

        # 그 외는 에러로 처리
        r.raise_for_status()



def restore_project_from_backup(session: requests.Session, zip_path: Path, org: str) -> None:
    """
    단일 프로젝트 ZIP 복원
    - POST /api/projects/backup (multipart/form-data, 파일 업로드)
    - 응답의 rq_id 로 /api/requests/{rq_id} 폴링
    """
    log(f"📤 복원 시작: {zip_path}")
    with open(zip_path, "rb") as f:
        files = {"backup": f}
        r = session.post(
            f"{CVAT_URL}/api/projects/backup",
            headers={
                "Authorization": f"Token {TOKEN}",
                "X-Organization": org,
                # multipart 업로드에서는 Content-Type 를 requests가 자동 설정하므로 직접 넣지 않습니다.
            },
            files=files,
            timeout=REQUEST_TIMEOUT
        )
    r.raise_for_status()
    rq_id = r.json().get("rq_id")
    if not rq_id:
        raise RuntimeError(f"rq_id를 받지 못했습니다: {r.text}")
    poll_request(session, rq_id, org)
    log(f"✅ 복원 완료: {zip_path}")


def backup_selected_projects(session: requests.Session) -> None:
    """
    SRC_ORG의 프로젝트 목록에서 PROJECTS_TO_MOVE 이름과 일치하는 항목만 백업합니다.
    - 이름→ID 매핑 후 순회
    """
    projects = list_projects(session, SRC_ORG)
    name_to_id = {p["name"]: p["id"] for p in projects}

    for proj_name in PROJECTS_TO_MOVE:
        if proj_name not in name_to_id:
            log(f"⚠ 프로젝트 없음: {proj_name}")
            continue
        proj_id = name_to_id[proj_name]
        zip_path = BACKUP_DIR / f"{proj_name}_{proj_id}.zip"
        export_project(session, proj_id, SRC_ORG, zip_path)


def restore_all_backups(session: requests.Session) -> None:
    """
    BACKUP_DIR 안의 모든 .zip 파일을 DST_ORG로 복원합니다.
    - 이미 존재하는 프로젝트가 있으면 서버 정책에 따라 병합/거부/새로생성으로 다를 수 있습니다.
    """
    zip_files = sorted(glob.glob(str(BACKUP_DIR / "*.zip")))
    for zip_file in zip_files:
        restore_project_from_backup(session, Path(zip_file), DST_ORG)


# -------------------- 실행 엔트리포인트 --------------------
def main() -> None:
    with requests.Session() as s:
        # 1) 인증/조직/프록시 이슈를 먼저 진단 (401/403을 빠르게 잡기 위함)
        bootstrap_auth_check(s)

        # 2) 백업 수행
        backup_selected_projects(s)

        # 3) 복원 수행
        restore_all_backups(s)


if __name__ == "__main__":
    main()
