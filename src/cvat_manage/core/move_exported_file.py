import os
import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import yaml
import csv

# === 환경 변수 로드 ===
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

RESULT_DIR = Path(os.getenv("RESULT_DIR", "/tmp/cvat_exports"))
DEST_DIR = Path(os.getenv("DEST_DIR", "/tmp/cvat_exports/moved_files"))
MATCH_SCOPE = os.getenv("MATCH_SCOPE_DIR", "processed_data") 


# def find_matching_folder(base_dir: Path, target_folder_name: str) -> Path or None:
#     print(f"[🔍] '{target_folder_name}' 폴더를 {base_dir} 하위의 '{MATCH_SCOPE}' 디렉토리 안에서 탐색 중...")
    
#     # 1. 먼저 MATCH_SCOPE ("processed_data") 디렉토리들 찾기
#     candidate_scope_dirs = [p for p in base_dir.rglob(MATCH_SCOPE) if p.is_dir()]
#     print(f"[📁] '{MATCH_SCOPE}' 디렉토리 {len(candidate_scope_dirs)}개 발견됨")

#     # 2. 각 processed_data 경로 하위에서 target 폴더 재귀 탐색
#     for scope_dir in candidate_scope_dirs:
#         for sub_path in scope_dir.rglob("*"):
#             if sub_path.is_dir() and sub_path.name == target_folder_name:
#                 print(f"[✅] 매칭 폴더 발견: {sub_path}")
#                 return sub_path
#     print(f"[❌] 매칭 폴더 없음: {target_folder_name} ({MATCH_SCOPE} 하위)")
#     return None


# 2. 추천안
def find_matching_folder(base_dir: Path, target_folder_name: str) -> Path or None:
    print(f"[🔍] '{target_folder_name}' 폴더를 {base_dir} 하위의 '{MATCH_SCOPE}' 디렉토리 안에서 탐색 중...")

    candidate_scope_dirs = [p for p in base_dir.rglob(MATCH_SCOPE) if p.is_dir()]
    print(f"[📁] '{MATCH_SCOPE}' 디렉토리 {len(candidate_scope_dirs)}개 발견됨")

    for scope_dir in candidate_scope_dirs:
        # 깊이 1~2까지만 제한 탐색
        for sub_path in list(scope_dir.glob("*")) + list(scope_dir.glob("*/*")):
            if sub_path.is_dir() and sub_path.name == target_folder_name:
                print(f"[✅] 매칭 폴더 발견: {sub_path}")
                return sub_path
    print(f"[❌] 매칭 폴더 없음: {target_folder_name} ({MATCH_SCOPE} 하위)")
    return None

# # 3. 사용 (I/O 속도 최적)
# import os
# def find_matching_folder(base_dir: Path, target_folder_name: str) -> Path or None:
#     print(f"[🔍] '{target_folder_name}' 폴더를 {base_dir} 하위의 '{MATCH_SCOPE}' 디렉토리 안에서 탐색 중...")

#     candidate_scope_dirs = [p for p in base_dir.rglob(MATCH_SCOPE) if p.is_dir()]
#     print(f"[📁] '{MATCH_SCOPE}' 디렉토리 {len(candidate_scope_dirs)}개 발견됨")

#     for scope_dir in candidate_scope_dirs:
#         for root, dirs, _ in os.walk(scope_dir):
#             for dir_name in dirs:
#                 if dir_name == target_folder_name:
#                     matched_path = Path(root) / dir_name
#                     print(f"[✅] 매칭 폴더 발견: {matched_path}")
#                     return matched_path
                
#     print(f"[❌] 매칭 폴더 없음: {target_folder_name} ({MATCH_SCOPE} 하위)")
#     return None

def generate_meta_yaml(target_dir: Path, zip_filename: str, label_type: str, zip_file_path: Path):
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

    meta["source_path"] = str(zip_file_path.parent)

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

def safe_move(src, dst):
    try:
        if not os.path.exists(src):
            print(f"[❌] 원본 파일이 존재하지 않습니다: {src}")
            return False
        shutil.copy2(src, dst)
        os.remove(src)
        print(f"[✅] 파일 복사 및 삭제 완료: {src} → {dst}")
        return True
    except Exception as e:
        print(f"[❌] 파일 이동 실패: {src} → {dst}\n에러: {e}")
        return False
    

def move_and_extract_zip(zip_file: Path, matched_folder: Path, moved_log_writer=None):
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
    # shutil.move(str(zip_file), str(dest_path))

    if not safe_move(str(zip_file), str(dest_path)):
        return
    

    try:
        with zipfile.ZipFile(dest_path, 'r') as zip_ref:
            zip_ref.extractall(target_dir)
        print(f"[✅] 압축 해제 완료: {dest_path.name} → {target_dir}")

        # dest_path.unlink()
        # print(f"[🗑️] ZIP 파일 삭제 완료: {dest_path.name}")

        # meta.yaml 자동생성 + source_path 포함
        generate_meta_yaml(target_dir, zip_file.name, label_type, zip_file)

        # 처리 로그 저장
        if moved_log_writer:
            moved_log_writer.writerow([
                zip_file.name,
                str(zip_file.parent),
                str(matched_folder),
                label_type,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ])

    except zipfile.BadZipFile:
        print(f"[❌] 압축 해제 실패 (손상된 zip 파일): {dest_path.name}")


def move_zip_to_corresponding_folder(result_dir: Path, dest_dir: Path):
    print(f"[📁] 결과 폴더: {result_dir}")
    print(f"[📦] 대상 최상위 폴더: {dest_dir}")
    
    zip_files = list(result_dir.rglob("*.zip"))
    print(f"[🔎] ZIP 파일 {len(zip_files)}개 발견됨")

    if not zip_files:
        print("❌ 이동할 zip 파일이 없습니다.")
        return
    
    moved_log_path = Path("/home/pia/work_p/dfn/omission/result/moved_log.csv")

    is_new_log = not moved_log_path.exists()

    with open(moved_log_path, "a", newline="") as log_file:
        writer = csv.writer(log_file)
        if is_new_log:
            writer.writerow(["zip_file", "original_path", "matched_folder", "label_type", "extracted_at"])

        for zip_file in zip_files:
            print(f"\n---\n[➡️] 파일 처리 중: {zip_file.name}")
            stem = zip_file.stem.replace("_keypoint", "").replace("_boundingbox", "")
            folder_name = re.sub(r"_\d+$", "", stem)
            print(f"[📂] 예상 폴더 이름: {folder_name}")

            matched_folder = find_matching_folder(dest_dir, folder_name)

            if matched_folder:
                move_and_extract_zip(zip_file, matched_folder, moved_log_writer=writer)
            else:
                print(f"[⚠️] 이동 생략: 대상 폴더 없음 → {folder_name}")

if __name__ == "__main__":
    today_str = datetime.today().strftime("%Y-%m-%d")
    result_dir = Path(f"{RESULT_DIR}/{today_str}")
    print(f"\n[🕒] 실행 날짜: {today_str}")
    move_zip_to_corresponding_folder(result_dir, DEST_DIR)
