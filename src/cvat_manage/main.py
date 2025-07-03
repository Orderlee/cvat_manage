import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


def run_script(script_path, args_str=""):
    command = [sys.executable, script_path]
    if args_str:
        command.extend(args_str.split())  # 문자열을 리스트로 분할

    env = os.environ.copy()
    cvat_cli_path = os.getenv("CVAT_CLI_PATH")
    if cvat_cli_path:
        env["PATH"] += os.pathsep + cvat_cli_path

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, env=env)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ '{script_path}' 실행 실패:")
        print(e.stderr)

def main():
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
    main()