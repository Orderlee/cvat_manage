import os
import re
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import yaml
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# === 환경 변수 로드 ===
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

RESULT_DIR = Path(os.getenv("RESULT_DIR", "/tmp/cvat_exports"))
DEST_DIR = Path(os.getenv("DEST_DIR", "/tmp/cvat_exports/moved_files"))
MATCH_SCOPE = os.getenv("MATCH_SCOPE_DIR", "processed_data")
MAX_DEPTH = 2
WORKERS = 0

# 자주 쓰는 정규식을 미리 컴파일
RE_TAIL_DIGIT = re.compile(r"_\d+$")
RE_KP = re.compile(r"_keypoint$")
RE_BB = re.compile(r"_boundingbox$")

def build_target_index(base_dir: Path) -> dict[str, list[Path]]:
    """
    MATCH_SCOPE 하위에서 depth <= MAX_DEPTH 인 모든 디렉터리를 인덱싱.
    key: 디렉터리명, value: 해당 경로 리스트(동명이인 대응)
    - NAS 재귀 비용을 1회로 제한하기 위한 핵심 함수
    """
    print(f"Index create start: base={base_dir}, scope='{MATCH_SCOPE}', depth<={MAX_DEPTH}")
    index: dict[str, list[Path]] = {}

    # 1) 먼저 scope 디렉터리들 찾기 (여기서만 rglob 1회)
    scope_dirs = [p for p in base_dir.rglob(MATCH_SCOPE) if p.is_dir()]
    print(f"scope directory {len(scope_dirs)}")

    for scope in scope_dirs:
        # 2) scope 하위 얕은 깊이만 인덱싱: os.walk로 현재 깊이를 계산
        root_depth = len(scope.parts)
        # followlinks=False: NAS에서 심볼릭 링크 때문에 깊어지는 것 방지
        for root, dirs, _ in os.walk(scope, topdown=True, followlinks=False):
            depth = len(Path(root).parts) - root_depth
            if depth > MAX_DEPTH:
                # 더 깊이 들어가지 않도록 dirs를 비워서 walk 중단
                dirs[:] = []
                continue
            
            # 현재 depth의 디렉터리들을 인덱스에 추가
            for d in dirs:
                p = Path(root) / d
                index.setdefault(d, []).append(p)

    total_entries = sum(len(v) for v in index.values())
    print(f"Index created completed: {len(index)}, entries={total_entries}")
    return index

def resolve_label_info(zip_name: str) -> tuple[str | None, str | None]:
    """
    ZIP 파일명으로부터 (subfolder_name, label_type) 결정.
    """
    if "_keypoint" in zip_name:
        return "keypoints", "keypoint"
    if "_boundingbox" in zip_name:
        return "bboxes", "bounding_box"
    return None, None

def generate_meta_yaml(target_dir: Path, zip_filename: str, label_type: str, zip_file_path: Path):
    meta_path = target_dir / "meta.yaml"
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = yaml.safe_load(f) or {}
    except Exception:
        meta = {}

    meta.setdefault("label_format", "coco")
    meta.setdefault("label_type", label_type)
    meta.setdefault("extracted_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    meta.setdefault("status", "extracted")
    meta.setdefault("notes", "자동 생성됨")

    # ZIP 원본 상위 경로 기록
    meta["source_path"] = str(zip_file_path.parent)

    # source_zip은 리스트로 유지
    source_zips = meta.get("source_zip", [])
    if isinstance(source_zips, str):
        source_zips = [source_zips]
    if zip_filename not in source_zips:
        source_zips.append(zip_filename)
    meta["source_zip"] = source_zips

    with open(meta_path, "w", encoding="utf-8") as f:
        yaml.dump(meta, f, allow_unicode=True)
    print(f"[📝] meta.yaml 갱신: {meta_path}")

def same_device(a: Path, b: Path) -> bool:
    """두 경로가 같은 디바이스(파일시스템)인지 판단."""
    try:
        return os.stat(a).st_dev == os.stat(b.parent).st_dev
    except FileNotFoundError:
        # 대상 부모가 없을 수 있으니 미리 생성 후 비교하는 게 정석이지만,
        # 보수적으로 False를 반환하여 copy 경로로 처리
        return False
    
def fast_move(src: Path, dst: Path) -> bool:
    """
    가능한 경우 같은 디바이스에서는 os.replace(진짜 move),
    아니면 shutil.move(복사 후 삭제). 예외는 False.
    """
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if same_device(src, dst):
            os.replace(src, dst) # atmoic move
        else:
            shutil.move(str(src), str(dst)) # cross-device: copy + delete
        print(f"[🚚] 이동 완료: {src} → {dst}")
        return True
    except Exception as e:
        print(f"[❌] 이동 실패: {src} → {dst}\n에러: {e}")
        return False
    
def extract_zip(zip_path: Path, target_dir: Path) -> bool:
    """ZIP 해제. 실패 시 False."""
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(target_dir)
        print(f"[✅] 압축 해제: {zip_path.name} → {target_dir}")
    except zipfile.BadZipFile:
        print(f"[❌] 손상 ZIP: {zip_path.name}")
        return False
    except Exception as e:
        print(f"[❌] 압축 해제 에러: {zip_path.name}\n에러: {e}")
        return False
    
def pick_matched_folder(folder_index: dict[str, list[Path]], name: str) -> Path | None:
    """
    인덱스에서 폴더명으로 대상 경로 찾기.
    - 동명이인일 경우 휴리스틱: 더 얕은 경로(=scope에서 가까운 경로) 우선.
    """
    candidates = folder_index.get(name)
    if not candidates:
        return None
    # 얕은 경로(경로 길이 짧은 순) 우선 선택
    return sorted(candidates, key=lambda p: len(p.parts))[0]

def plan_target(zip_file: Path, folder_index: dict[str, list[Path]]) -> tuple[Path | None, Path | None, str | None]:
    """
    ZIP 파일로부터 이동 및 추출 대상 경로 계산.
    return: (matched_folder, dest_zip_path, label_type)

    동작 규칙:
    - zip 파일명에 "_keypoint" 또는 "_boundingbox" 포함 → 각각 "keypoints" / "bboxes" 하위로 이동
    - zip은 matched_folder/<subfolder_name>/zip_name 에 보관
    - 해제는 dest_zip_path.parent (동일 디렉터리)로 수행
    """
    subfolder_name, label_type = resolve_label_info(zip_file.name)
    if not subfolder_name:
        print(f"[⚠️] 무시: keypoint/boundingbox 미포함 → {zip_file.name}")
        return None, None, None

    # 원본 파일명에서 작업 폴더 이름 추출
    stem = zip_file.stem
    stem = RE_KP.sub("", stem)
    stem = RE_BB.sub("", stem)
    folder_name = RE_TAIL_DIGIT.sub("", stem)

    matched_folder = pick_matched_folder(folder_index, folder_name)
    if not matched_folder:
        print(f"[⚠️] 대상 폴더 없음 → {folder_name}")
        return None, None, None

    # zip 최종 보관 경로: <matched>/<keypoints|bboxes>/<zip_name>
    dest_dir = matched_folder / subfolder_name
    dest_zip_path = dest_dir / zip_file.name
    return matched_folder, dest_zip_path, label_type
    
def process_one_zip(zip_file: Path, folder_index: dict[str, list[Path]], moved_log_writer=None):
    """
    ZIP 하나를 처리: 이동 → 해제 → meta.yaml → 로그
    (스레드에서 호출 가능)
    """
    matched_folder, dest_zip_path, label_type = plan_target(zip_file, folder_index)
    if not matched_folder:
        return

    # 이동 (부모 디렉터리는 fast_move 내부에서 생성)
    if not fast_move(zip_file, dest_zip_path):
        return

    # 해제: zip이 있는 동일 폴더로 통일
    extract_to = dest_zip_path.parent
    if not extract_zip(dest_zip_path, extract_to):
        return

    # meta.yaml: zip이 위치한 디렉터리 기준으로 기록
    generate_meta_yaml(extract_to, dest_zip_path.name, label_type, zip_file)

    # 로그
    if moved_log_writer:
        moved_log_writer.writerow([
            zip_file.name,
            str(zip_file.parent),
            str(matched_folder),
            label_type,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])

def move_zip_to_corresponding_folder(result_dir: Path, dest_dir: Path):
    print(f"[📁] 결과 폴더: {result_dir}")
    print(f"[📦] 대상 최상위 폴더: {dest_dir}")

    zip_files = list(result_dir.rglob("*.zip"))
    print(f"[🔎] ZIP {len(zip_files)}개 발견")

    if not zip_files:
        print("❌ 이동할 zip 파일이 없습니다.")
        return
    
    # 1) 대상 폴더 인덱스 1회 생성
    folder_index = build_target_index(dest_dir)

    moved_log_path = Path("/home/pia/work_p/dfn/omission/result/moved_log.csv")
    is_new_log = not moved_log_path.exists()

    # 2) 병렬 처리 워커 수 결정
    if WORKERS > 0:
        workers = WORKERS
    else:
        cpu = os.cpu_count() or 4
        workers = min(8, cpu * 2)

    print(f"[🧵] 병렬 처리 워커 수: {workers}")

    # 3) 스레드에서 처리하되, CSV는 메인에서 한 번에 기록하기 위해 리스트로 모음
    pending_logs = []

    def _task(zip_path: Path):
        matched_folder, dest_zip_path, label_type = plan_target(zip_path, folder_index)
        if not matched_folder:
            return None
        if not fast_move(zip_path, dest_zip_path):
            return None
        extract_to = dest_zip_path.parent  # zip과 동일 폴더에 해제
        if not extract_zip(dest_zip_path, extract_to):
            return None
        generate_meta_yaml(extract_to, dest_zip_path.name, label_type, zip_path)
        # CSV는 여기서 바로 쓰지 않고, 메인에서 일괄 기록
        return [
            zip_path.name,
            str(zip_path.parent),
            str(matched_folder),
            label_type,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_task, z) for z in zip_files]
        for fut in as_completed(futures):
            log_row = fut.result()
            if log_row:
                pending_logs.append(log_row)

    # 4) CSV 기록: 반드시 with 블록 **안에서** writerows까지 처리해야 함!
    moved_log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(moved_log_path, "a", newline="", encoding="utf-8") as log_file:
            writer = csv.writer(log_file)
            if is_new_log:
                writer.writerow(["zip_file", "original_path", "matched_folder", "label_type", "extracted_at"])
            # << 이 줄이 기존에는 with 밖에 있어서 파일 닫힌 뒤 호출되던 버그가 있었습니다
            writer.writerows(pending_logs)
        print(f"[🧾] 이동 로그 {len(pending_logs)}건 기록: {moved_log_path}")
    except Exception as e:
        print(f"[❌] 로그 기록 실패: {e}")


if __name__ == "__main__":
    today_str = datetime.today().strftime("%Y-%m-%d")
    result_dir = Path(f"{RESULT_DIR}/{today_str}")
    print(f"\n[🕒] 실행 날짜: {today_str}")
    move_zip_to_corresponding_folder(result_dir, DEST_DIR)