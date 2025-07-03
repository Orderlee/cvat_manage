import os
import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import yaml

# === 환경 변수 로드 ===
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

RESULT_DIR = Path(os.getenv("RESULT_DIR", "/tmp/cvat_exports"))
DEST_DIR = Path(os.getenv("DEST_DIR", "/tmp/cvat_exports/moved_files"))

def find_matching_folder(base_dir: Path, target_folder_name: str) -> Path or None:
    print(f"[🔍] '{target_folder_name}' 폴더를 {base_dir} 하위에서 탐색 중...")
    for path in base_dir.rglob("*"):
        if path.is_dir() and path.name == target_folder_name:
            print(f"[✅] 매칭 폴더 발견: {path}")
            return path
    print(f"[❌] 매칭 폴더 없음: {target_folder_name}")
    return None


def generate_meta_yaml(target_dir: Path, zip_filename: str, label_type: str):
    meta_path = target_dir / "meta.yaml"

    # 1. 기존 meta.yaml이 있으면 불러오기
    if meta_path.exists():
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = yaml.safe_load(f) or {}

    else:
        meta = {}

    # 2. 기본 메타 항목 채우기
    meta.setdefault("label_format", "coco")
    meta.setdefault("label_type", label_type)
    meta.setdefault("extracted_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    meta.setdefault("status", "extracted")
    meta.setdefault("notes", "자동 생성됨")

    # 3. source_zip 항목을 리스트로 관리
    source_zips = meta.get("source_zip", [])
    if isinstance(source_zips, str):
        source_zips = [source_zips]
    if zip_filename not in source_zips:
        source_zips.append(zip_filename)
    meta["source_zip"] = source_zips

    # 4. 저장
    with open(meta_path, 'w', encoding='utf-8') as f:
        yaml.dump(meta, f, allow_unicode=True)
    print(f"[📝] meta.yaml 갱신 완료: {meta_path}")


def move_and_extract_zip(zip_file: Path, matched_folder: Path):
    if "_keypoint" in zip_file.name:
        subfolder_name = "keypoints"
        label_type = "keypoint"
    elif "_boundingbox" in zip_file.name:
        subfolder_name = "bboxes"
        label_type = "bounding_box"
    else:
        print(f"[⚠️] 무시됨: 파일명이 keypoint 또는 boundingbox를 포함하지 않음 → {zip_file.name}")
        return
    
    target_dir = matched_folder / subfolder_name
    target_dir.mkdir(parents=True, exist_ok=True)

    dest_path = target_dir / zip_file.name
    print(f"[🚚] 이동 경로: {dest_path}")
    shutil.move(str(zip_file), str(dest_path))

    try:
        with zipfile.ZipFile(dest_path, 'r') as zip_ref:
            zip_ref.extractall(target_dir)
        print(f"[✅] 압축 해제 완료: {dest_path.name} → {target_dir}")

        # dest_path.unlink()
        # print(f"[🗑️] ZIP 파일 삭제 완료: {dest_path.name}")

        # meta.yaml 자동생성
        generate_meta_yaml(target_dir, zip_file.name, label_type)

    except zipfile.BadZipFile:
        print(f"[❌] 압축 해제 실패 (손상된 zip 파일): {dest_path.name}")


def move_zip_to_corresponding_folder(result_dir: Path, dest_dir: Path):
    print(f"[📁] 결과 폴더: {result_dir}")
    print(f"[📦] 대상 최상위 폴더: {dest_dir}")
    
    zip_files = list(result_dir.glob("*.zip"))
    print(f"[🔎] ZIP 파일 {len(zip_files)}개 발견됨")

    if not zip_files:
        print("❌ 이동할 zip 파일이 없습니다.")
        return

    for zip_file in zip_files:
        print(f"\n---\n[➡️] 파일 처리 중: {zip_file.name}")
        stem = zip_file.stem.replace("_keypoint", "").replace("_boundingbox", "")
        folder_name = re.sub(r"_\d+$", "", stem)
        print(f"[📂] 예상 폴더 이름: {folder_name}")

        matched_folder = find_matching_folder(dest_dir, folder_name)

        if matched_folder:
            move_and_extract_zip(zip_file, matched_folder)
        else:
            print(f"[⚠️] 이동 생략: 대상 폴더 없음 → {folder_name}")

if __name__ == "__main__":
    today_str = datetime.today().strftime("%Y-%m-%d")
    result_dir = Path(f"{RESULT_DIR}/{today_str}")
    print(f"\n[🕒] 실행 날짜: {today_str}")
    move_zip_to_corresponding_folder(result_dir, DEST_DIR)
