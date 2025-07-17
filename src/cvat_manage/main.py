import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import argparse
import datetime

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


# def run_script(script_path, args_str=""):
#     command = [sys.executable, script_path]
#     if args_str:
#         command.extend(args_str.split())  # 문자열을 리스트로 분할

#     env = os.environ.copy()
#     cvat_cli_path = os.getenv("CVAT_CLI_PATH")
#     if cvat_cli_path:
#         env["PATH"] += os.pathsep + cvat_cli_path

#     try:
#         result = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
#         print(result.stdout)
#     except subprocess.CalledProcessError as e:
#         print(f"❌ '{script_path}' 실행 실패:")
#         print(e.stderr)

# 인자 파싱
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--quiet', action='store_true', help='출력을 최소화합니다')
    return parser.parse_args()


def run_script(script_path, args_str="", max_retries=1, timeout_sec=3600):
    command = [sys.executable, script_path]
    if args_str:
        command.extend(args_str.split())

    env = os.environ.copy()
    cvat_cli_path = os.getenv("CVAT_CLI_PATH")
    if cvat_cli_path:
        env["PATH"] += os.pathsep + cvat_cli_path

    for attempt in range(1, max_retries + 2):
        try:
            print(f"🚀 '{script_path}' 실행 시도 {attempt}/{max_retries + 1}")
            result = subprocess.run(command, check=True, capture_output=True, text=True, env=env, timeout=timeout_sec)
            print(result.stdout)
            return
        except subprocess.TimeoutExpired:
            print(f"⏰ '{script_path}' 실행이 {timeout_sec}초를 초과하여 타임아웃되었습니다.")
        except subprocess.CalledProcessError as e:
            print(f"❌ '{script_path}' 실행 실패 (exit code {e.returncode}):")
            print(e.stderr)

        if attempt <= max_retries:
            print(f"🔁 재시도 중...")
        else:
            print(f"🚫 모든 재시도 실패. 다음 스크립트로 넘어갑니다.")

def main(quiet=False):
    # 1️⃣ omission.py 실행
    omission_path = os.getenv("OMISSION_SCRIPT")
    omission_args = os.getenv("OMISSION_ARGS", "")
    print("🚀 omission.py 실행 중...")
    run_script(omission_path, omission_args)

    # 2️⃣ send_report.py 실행
    report_path = os.getenv("REPORT_SCRIPT")
    report_args = os.getenv("REPORT_ARGS", "")
    print("📊 generate_report.py 실행 중...")
    run_script(report_path, report_args)

    # 3️⃣ export.py 실행
    export_path = os.getenv("EXPORT_SCRIPT")
    print("📦 export.py 실행 중..." )
    run_script(export_path)
    
    # 4️⃣ move_exported_file.py 실행
    move_path = os.getenv("MOVE_SCRIPT")
    print("📦 move_exported_file.py 실행 중...")
    run_script(move_path)


if __name__ == "__main__":
    args = parse_args()

    if not args.quiet:
        now = datetime.datetime.now()
        print(f"[START] {now.strftime('%Y-%m-%d %H:%M:%S')} ✅ main.py 시작됨")

    main(quiet=args.quiet)

    if not args.quiet:
        now = datetime.datetime.now()
        print(f"[END]   {now.strftime('%Y-%m-%d %H:%M:%S')} ✅ main.py 종료됨")