import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import argparse
import datetime

# .env 로드
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# -----------------------------
# 1) 인자 파서
# -----------------------------
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--quiet', action='store_true', help='출력을 최소화합니다')
    return parser.parse_args()

# -----------------------------
# 2) 안전 디코딩 유틸
# -----------------------------
def _safe_decode(b: bytes) -> str:
    """
    subprocess 결과(바이너리)를 안전하게 문자열로 변환.
    1) UTF-8 시도
    2) cp949 (윈도우/한국어 콘솔) 시도
    3) euc-kr 시도
    4) latin-1 (깨짐 최소화) 시도
    """
    if b is None:
        return ""
    for enc in ("utf-8", "cp949", "euc-kr", "latin-1"):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            continue
    # 모든 시도가 실패하면 "replace"로라도 살립니다.
    return b.decode("utf-8", errors="replace")

# -----------------------------
# 3) 스크립트 실행기
# -----------------------------
def run_script(script_path, args_str="", max_retries=1, timeout_sec=86400, quiet=False):
    """
    - text=True 를 사용하지 않고 바이너리 수집(text=False, 기본)
    - 환경 변수로 UTF-8 출력 유도
    - 안전 디코딩으로 UnicodeDecodeError 방지
    - 재시도(max_retries) 지원
    """
    if not script_path:
        print("⚠️  실행 대상 스크립트 경로가 비어있습니다. (.env 확인)")
        return

    script_path = str(script_path)
    if not Path(script_path).exists():
        print(f"⚠️  스크립트 파일이 존재하지 않습니다: {script_path}")
        return

    # 실행 커맨드 구성
    command = [sys.executable, script_path]
    if args_str:
        command.extend(args_str.split())

    # 환경 변수 구성
    env = os.environ.copy()

    # 가능하면 하위 프로세스가 UTF-8로 출력하도록 유도
    env["PYTHONUTF8"] = "1"         # 파이썬 I/O를 UTF-8로
    env.setdefault("LC_ALL", "C.UTF-8")
    env.setdefault("LANG", "C.UTF-8")

    # (옵션) CVAT CLI 경로 PATH에 추가
    cvat_cli_path = os.getenv("CVAT_CLI_PATH")
    if cvat_cli_path:
        env["PATH"] += os.pathsep + cvat_cli_path

    attempts = max_retries + 1
    for attempt in range(1, attempts + 1):
        try:
            print(f"🚀 '{script_path}' 실행 시도 {attempt}/{attempts}")
            # text=False => stdout/stderr 는 bytes (직접 디코딩)
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                env=env,
                timeout=timeout_sec,
            )

            out = _safe_decode(result.stdout)
            err = _safe_decode(result.stderr)

            # 조용 모드가 아니면 출력 보여주기
            if not quiet:
                if out.strip():
                    print("[STDOUT]")
                    print(out)
                if err.strip():
                    print("[STDERR]")
                    print(err)

            return  # 성공이면 종료

        except subprocess.TimeoutExpired:
            print(f"⏰ '{script_path}' 실행이 {timeout_sec}초를 초과하여 타임아웃되었습니다.")
        except subprocess.CalledProcessError as e:
            # 실패해도 stdout/stderr를 안전 디코딩하여 로그 남김
            out = _safe_decode(e.stdout)
            err = _safe_decode(e.stderr)
            print(f"❌ '{script_path}' 실행 실패 (exit code {e.returncode}):")
            if out.strip():
                print("[STDOUT]")
                print(out)
            if err.strip():
                print("[STDERR]")
                print(err)

        # 재시도 여부
        if attempt < attempts:
            print("🔁 재시도 중...")
        else:
            print("🚫 모든 재시도 실패. 다음 단계로 넘어갑니다.")

# -----------------------------
# 4) 메인 파이프라인
# -----------------------------
def main(quiet=False):
    # 1️⃣ omission.py 실행
    omission_path = os.getenv("OMISSION_SCRIPT")
    omission_args = os.getenv("OMISSION_ARGS", "")
    print("🚀 omission.py 실행 중...")
    run_script(omission_path, omission_args, quiet=quiet)

    # 2️⃣ send_report.py 실행
    report_path = os.getenv("REPORT_SCRIPT")
    report_args = os.getenv("REPORT_ARGS", "")
    print("📊 generate_report.py 실행 중...")
    run_script(report_path, report_args, quiet=quiet)

    # 3️⃣ export.py 실행
    export_path = os.getenv("EXPORT_SCRIPT")
    print("📦 export.py 실행 중..." )
    run_script(export_path, quiet=quiet)
    
    # 4️⃣ move_exported_file_newversion.py 실행
    move_path = os.getenv("MOVE_SCRIPT")
    print("📦 move_exported_file_newversion.py 실행 중...")
    run_script(move_path, quiet=quiet)

# -----------------------------
# 5) 진입점
# -----------------------------
if __name__ == "__main__":
    args = parse_args()

    now = datetime.datetime.now()
    print(f"[START] {now.strftime('%Y-%m-%d %H:%M:%S')} ✅ main.py 시작됨")

    main(quiet=args.quiet)

    now = datetime.datetime.now()
    print(f"[END]   {now.strftime('%Y-%m-%d %H:%M:%S')} ✅ main.py 종료됨")
