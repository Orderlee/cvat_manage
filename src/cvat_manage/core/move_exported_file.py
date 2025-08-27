"""
ZIP 파일을 대상 폴더 트리로 이동/해제하고 meta.yaml을 관리하는 스크립트 (병렬 처리 지원)

주요 기능
1) DEST_DIR 안의 MATCH_SCOPE(기본 'processed_data') 하위(깊이<=MAX_DEPTH)에 있는
   모든 디렉터리를 1회 스캔하여 '이름 -> 경로들' 인덱스를 생성합니다.
2) RESULT_DIR/YYYY-MM-DD 내부에서 발견한 *.zip 파일을 다음 로직으로 처리합니다.
   - 파일명이 *_keypoint.zip → 대상/<keypoints>/ 로 이동 후 해제
   - 파일명이 *_boundingbox.zip → 대상/<bboxes>/ 로 이동 후 해제
   - ZIP 파일명에서 원본 폴더명을 유추하고(접미 숫자/_keypoint/_boundingbox 제거)
     인덱스에서 가장 근접(경로 길이 짧은) 폴더를 선택합니다.
3) meta.yaml을 갱신합니다. (label_type, source_zip 누적 등)
4) moved_log.csv에 처리 결과를 누적 기록합니다.
"""

import os
import re
import csv
import zipfile
import shutil
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================
# 환경 변수 로드 (.env는 상위 폴더 기준)
# ==============================
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# ==============================
# 설정값 (필요 시 .env로도 오버라이드 가능)
# ==============================
RESULT_DIR = Path(os.getenv("RESULT_DIR", "/tmp/cvat_exports"))          # ZIP 검색 루트
DEST_DIR   = Path(os.getenv("DEST_DIR", "/tmp/cvat_exports/moved_files"))# 대상 트리 루트
MATCH_SCOPE = os.getenv("MATCH_SCOPE_DIR", "processed_data")             # 대상 트리에서 탐색할 스코프 디렉터리 이름
MAX_DEPTH = 2                                                            # MATCH_SCOPE 하위 인덱싱 깊이 제한
WORKERS = 8                                                              # 요청사항: 고정 8로 운용

# moved_log.csv 저장 위치 (원문 경로 유지, 없으면 RESULT_DIR 하위 result/로 생성)
DEFAULT_LOG_PATH = Path("/home/pia/work_p/dfn/omission/result/moved_log.csv")
MOVED_LOG_PATH = Path(os.getenv("MOVED_LOG_PATH", str(DEFAULT_LOG_PATH)))

# ==============================
# 자주 쓰는 정규식 미리 컴파일
# ==============================
RE_TAIL_DIGIT = re.compile(r"_\d+$")        # 폴더명 뒤쪽 _숫자 제거용
RE_KP = re.compile(r"_keypoint$")           # 파일 stem에서 _keypoint 제거용
RE_BB = re.compile(r"_boundingbox$")        # 파일 stem에서 _boundingbox 제거용


# ==============================
# 유틸 함수들
# ==============================
def resolve_label_info(zip_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    ZIP 파일명에서 서브폴더, 라벨 타입 결정
    - *_keypoint*.zip  → ("keypoints", "keypoint")
    - *_boundingbox*.zip → ("bboxes", "bounding_box")
    """
    lower = zip_name.lower()
    if "_keypoint" in lower:
        return "keypoints", "keypoint"
    if "_boundingbox" in lower:
        return "bboxes", "bounding_box"
    return None, None


def build_target_index(base_dir: Path) -> Dict[str, List[Path]]:
    """
    DEST_DIR 하위에서 MATCH_SCOPE 디렉터리들을 찾고,
    각 scope 내에서 depth <= MAX_DEPTH 범위를 인덱싱합니다.
    - key: 디렉토리명
    - value: 해당 경로 리스트(동명이인 대응)
    한 번 인덱싱해두면 NAS 재귀 비용을 크게 줄일 수 있음.
    """
    print(f"[🧭] 인덱스 생성 시작: base={base_dir}, scope='{MATCH_SCOPE}', depth<={MAX_DEPTH}")
    index: Dict[str, List[Path]] = {}

    # 1) scope 디렉토리들만 rglob로 1회 탐색
    scope_dirs = [p for p in base_dir.rglob(MATCH_SCOPE) if p.is_dir()]
    print(f"[📁] scope 디렉토리 발견 개수: {len(scope_dirs)}")

    for scope in scope_dirs:
        root_depth = len(scope.parts)
        # os.walk로 현재 깊이 추적하면서 제한
        for root, dirs, _ in os.walk(scope, topdown=True, followlinks=False):
            depth = len(Path(root).parts) - root_depth
            if depth > MAX_DEPTH:
                # 더 깊이 들어가지 않도록 탐색 중단
                dirs[:] = []
                continue
            for d in dirs:
                p = Path(root) / d
                index.setdefault(d, []).append(p)

    total_entries = sum(len(v) for v in index.values())
    print(f"[✅] 인덱스 생성 완료: 고유 디렉토리명 {len(index)}개, 총 경로 엔트리 {total_entries}개")
    return index


def pick_matched_folder(folder_index: Dict[str, List[Path]], name: str) -> Optional[Path]:
    """
    인덱스에서 폴더명으로 대상 경로 찾기.
    - 동명이인일 경우: 더 얕은 경로(=경로 길이가 짧은) 우선 선택
    """
    candidates = folder_index.get(name)
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: len(p.parts))[0]


def plan_target(zip_file: Path, folder_index: Dict[str, List[Path]]) -> Tuple[Optional[Path], Optional[Path], Optional[str]]:
    """
    ZIP 파일 → (matched_folder, dest_zip_path, label_type) 결정
    - zip 파일명에 "_keypoint"/"_boundingbox" 포함 → 각각 "keypoints"/"bboxes" 서브폴더 사용
    - 대상 zip 보관 위치: matched_folder/<subfolder>/zip_name
    - 해제 위치: dest_zip_path.parent (zip이 위치한 동일 폴더)
    """
    subfolder_name, label_type = resolve_label_info(zip_file.name)
    if not subfolder_name:
        print(f"[⚠️] 무시: keypoint/boundingbox 미포함 → {zip_file.name}")
        return None, None, None

    # 원본 파일명에서 작업 폴더 이름 유추
    stem = zip_file.stem
    stem = RE_KP.sub("", stem)
    stem = RE_BB.sub("", stem)
    folder_name = RE_TAIL_DIGIT.sub("", stem)

    matched_folder = pick_matched_folder(folder_index, folder_name)
    if not matched_folder:
        print(f"[⚠️] 대상 폴더를 찾지 못했습니다 → 유추명: '{folder_name}'")
        return None, None, None

    dest_dir = matched_folder / subfolder_name
    dest_zip_path = dest_dir / zip_file.name
    return matched_folder, dest_zip_path, label_type


def same_device(src: Path, dst: Path) -> bool:
    """
    두 경로가 같은 파일시스템/디바이스에 있는지 여부.
    - dst.parent가 없으면 비교 시 에러가 날 수 있으므로 우선 생성해둠.
    """
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        return os.stat(src).st_dev == os.stat(dst.parent).st_dev
    except Exception:
        # 보수적으로 False
        return False


def fast_move(src: Path, dst: Path) -> bool:
    """
    가능한 경우 같은 디바이스에서는 os.replace(진짜 move, atomic),
    아니면 shutil.move(복사+삭제)로 처리. 예외 시 False 반환.
    """
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if same_device(src, dst):
            os.replace(src, dst)  # atomic move (동일 파일시스템)
        else:
            shutil.move(str(src), str(dst))  # cross-device: copy + delete
        print(f"[🚚] 이동 완료: {src} → {dst}")
        return True
    except Exception as e:
        print(f"[❌] 파일 이동 실패: {src} → {dst}\n에러: {e}")
        return False


def extract_zip(zip_path: Path, target_dir: Path) -> bool:
    """
    ZIP 압축 해제. 실패 시 False.
    """
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(target_dir)
        print(f"[✅] 압축 해제: {zip_path.name} → {target_dir}")
        return True
    except zipfile.BadZipFile:
        print(f"[❌] 손상 ZIP: {zip_path.name}")
        return False
    except Exception as e:
        print(f"[❌] 압축 해제 에러: {zip_path.name}\n에러: {e}")
        return False


def generate_meta_yaml(target_dir: Path, zip_filename: str, label_type: str, source_zip_file: Path) -> None:
    """
    target_dir 기준으로 meta.yaml을 생성/갱신.
    - label_format: coco (기본)
    - label_type: keypoint / bounding_box
    - extracted_at: 처리 시각
    - status: extracted
    - notes: 자동 생성됨
    - source_path: 원 ZIP의 상위 디렉터리
    - source_zip: ZIP 파일명 리스트로 누적
    """
    meta_path = target_dir / "meta.yaml"

    # 1) 기존 메타 로드 (없거나 깨지면 빈 dict)
    meta = {}
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = yaml.safe_load(f) or {}
        except Exception:
            meta = {}

    # 2) 기본 항목 채우기(없으면)
    meta.setdefault("label_format", "coco")
    meta["label_type"] = label_type
    meta["extracted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta.setdefault("status", "extracted")
    meta.setdefault("notes", "자동 생성됨")
    meta["source_path"] = str(source_zip_file.parent)

    # 3) source_zip: 리스트로 누적
    source_zips = meta.get("source_zip", [])
    if isinstance(source_zips, str):
        source_zips = [source_zips]
    if zip_filename not in source_zips:
        source_zips.append(zip_filename)
    meta["source_zip"] = source_zips

    # 4) 저장
    try:
        with open(meta_path, "w", encoding="utf-8") as f:
            yaml.dump(meta, f, allow_unicode=True)
        print(f"[📝] meta.yaml 갱신: {meta_path}")
    except Exception as e:
        print(f"[❌] meta.yaml 기록 실패: {meta_path}\n에러: {e}")


def process_one_zip(zip_path: Path, folder_index: Dict[str, List[Path]]) -> Optional[List[str]]:
    """
    ZIP 하나 처리(스레드용):
    - 대상 폴더 결정 → zip 이동 → 해제 → meta.yaml 갱신
    - 성공 시 로그 1행을 리스트로 반환, 실패/스킵 시 None
    """
    matched_folder, dest_zip_path, label_type = plan_target(zip_path, folder_index)
    if not matched_folder:
        return None

    # zip 이동 (부모 생성은 fast_move 내부에서 수행)
    if not fast_move(zip_path, dest_zip_path):
        return None

    # 해제는 zip이 위치한 동일 폴더(dest_zip_path.parent)
    extract_to = dest_zip_path.parent
    if not extract_zip(dest_zip_path, extract_to):
        return None

    # meta.yaml 갱신
    generate_meta_yaml(extract_to, dest_zip_path.name, label_type, zip_path)

    # CSV 로그 레코드 반환
    return [
        zip_path.name,
        str(zip_path.parent),
        str(matched_folder),
        label_type,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ]


def move_zip_to_corresponding_folder(result_dir: Path, dest_dir: Path) -> None:
    """
    엔트리 포인트:
    - result_dir(보통 RESULT_DIR/YYYY-MM-DD)에서 *.zip을 모두 찾아 병렬 처리
    - CSV 로그(moved_log.csv)에 결과 누적
    """
    print(f"[📦] 결과 루트: {result_dir}")
    print(f"[🏁] 대상 루트: {dest_dir}")

    zip_files = list(result_dir.rglob("*.zip"))
    print(f"[🔎] ZIP 파일 발견: {len(zip_files)}개")

    if not zip_files:
        print("[ℹ️] 이동/해제할 zip 파일이 없습니다.")
        return

    # 1) 대상 인덱스 1회 생성
    folder_index = build_target_index(dest_dir)

    # 2) 병렬 처리 워커 수
    workers = WORKERS if WORKERS > 0 else max(1, min(8, (os.cpu_count() or 4) * 2))
    print(f"[🧵] 병렬 처리 워커 수: {workers}")

    # 3) 스레드 실행 (CSV는 메인에서 일괄 기록)
    pending_logs: List[List[str]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(process_one_zip, z, folder_index) for z in zip_files]
        for fut in as_completed(futures):
            row = fut.result()
            if row:
                pending_logs.append(row)

    # 4) CSV 기록
    MOVED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    is_new_log = not MOVED_LOG_PATH.exists()
    try:
        with open(MOVED_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if is_new_log:
                writer.writerow(["zip_file", "original_path", "matched_folder", "label_type", "extracted_at"])
            writer.writerows(pending_logs)
        print(f"[🧾] 이동 로그 {len(pending_logs)}건 기록: {MOVED_LOG_PATH}")
    except Exception as e:
        print(f"[❌] 로그 기록 실패: {e}")


# ==============================
# 메인
# ==============================
if __name__ == "__main__":
    # 오늘 날짜 기준 하위 폴더에서 zip을 처리 (예: /root/RESULT_DIR/2025-08-27)
    today_str = datetime.today().strftime("%Y-%m-%d")
    result_dir = RESULT_DIR / today_str

    print(f"\n[🕒] 실행 날짜: {today_str}")
    move_zip_to_corresponding_folder(result_dir, DEST_DIR)
