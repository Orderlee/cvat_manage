#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
신규 폴더 감지 → image_extract.py → import_autolabeling.py

요구사항 충족 사항
- .env 불사용
- 신규 폴더가 속한 organized_videos/<category> 를 입력 기준으로 처리
- 결과는 같은 상위 레벨의 processed_data/<category> 아래에 생성 (없으면 생성)
- 감지 대상: /home/pia/mou/nas_192tb/datasets/{projects,public} 하위 전체
"""

import os
import csv
import subprocess
from pathlib import Path
from datetime import datetime
from typing import List, Set, Tuple, Optional

# ========= 기본 설정 =========
BASE_DIR = Path("/home/pia/mou/nas_192tb/datasets")
TARGET_FOLDERS = ["projects", "public"]
SNAPSHOT_CSV = Path("dir_snapshot.csv")
IGNORE_HIDDEN = True

VIDEO_EXTS = (".mp4", ".avi", ".mov")
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".bmp", ".webp")

# ======== 스크립트 경로 (환경에 맞게 필요 시 조정) ========
IMAGE_EXTRACT_2_PY = Path("/home/pia/work_p/dfn/omission/image_extract_2.py")
IMPORT_AUTOLABELING_2_PY = Path("/home/pia/work_p/dfn/omission/import_autolabeling_2.py")

# ======== import_autolabeling.py 관련 필수 ENV(없으면 해당 단계는 경고 후 건너뜀) ========
REQUIRE_AUTOLABEL_ENV = ["ORGANIZATIONS", "PROJECT_NAME", "LABELS", "ASSIGNEES"]


# ------------------ 유틸 함수 ------------------

def is_hidden(path: Path) -> bool:
    return path.name.startswith(".")

def scan_all_dirs() -> List[str]:
    """projects/public 하위의 모든 디렉터리 절대경로 수집 (top 자체 제외)"""
    collected: List[str] = []
    for top in TARGET_FOLDERS:
        top_path = BASE_DIR / top
        if not top_path.exists():
            continue
        for root, dirnames, _ in os.walk(top_path):
            root_path = Path(root)
            for d in dirnames:
                dpath = (root_path / d).resolve()
                if IGNORE_HIDDEN and (is_hidden(dpath) or any(p.startswith(".") for p in dpath.parts)):
                    continue
                collected.append(str(dpath))
    return collected

def load_snapshot(csv_path: Path) -> Set[str]:
    if not csv_path.exists():
        return set()
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        return {row[0] for row in reader if row}

def save_snapshot(csv_path: Path, dirs: List[str]) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        for d in sorted(dirs):
            writer.writerow([d])

def folder_has_videos(folder: Path) -> bool:
    """폴더(재귀) 내 영상 존재 여부"""
    for root, _, files in os.walk(folder):
        for name in files:
            if name.lower().endswith(VIDEO_EXTS):
                return True
    return False

def has_any_images(dir_path: Path) -> bool:
    """디렉터리에 이미지가 하나라도 있는지(재귀)"""
    for root, _, files in os.walk(dir_path):
        for name in files:
            if name.lower().endswith(IMAGE_EXTS):
                return True
    return False

def find_ancestor_with_name(path: Path, target_name: str) -> Optional[Path]:
    """path에서 시작해 상위로 올라가며 이름이 target_name인 디렉터리를 찾음"""
    p = path.resolve()
    for ancestor in [p] + list(p.parents):
        if ancestor.name == target_name:
            return ancestor
    return None

def list_immediate_subdirs(dir_path: Path) -> List[str]:
    """dir_path 바로 아래의 하위 디렉터리명 목록"""
    result = []
    for name in os.listdir(dir_path):
        p = dir_path / name
        if p.is_dir() and not (IGNORE_HIDDEN and name.startswith(".")):
            result.append(name)
    return result


# ------------------ 핵심 로직 ------------------

def run_image_extract_for_category(trigger_dir: Path) -> Tuple[Path, Path, str]:
    """
    신규 폴더(trigger_dir)를 기준으로 image_extract_2.py 실행.
    - INPUT_ROOT = trigger_dir의 조상 중 이름이 'organized_videos'인 디렉터리
    - target_category = trigger_dir의 마지막 디렉터리명
    - OUTPUT_ROOT = organized_videos의 부모에 있는 processed_data 경로
    - 결과가 저장될 실제 디렉터리 = OUTPUT_ROOT/target_category (없으면 생성)

    반환: (processed_base, processed_base/target_category, target_category)
    """
    trigger_dir = trigger_dir.resolve()

    # 1) organized_videos 조상 찾기
    org_videos_dir = find_ancestor_with_name(trigger_dir, "organized_videos")
    if org_videos_dir is None:
        raise RuntimeError(f"'organized_videos' 조상을 찾을 수 없습니다: {trigger_dir}")

    # 2) target 카테고리 = 신규 폴더명
    target_category = trigger_dir.name

    # 3) OUTPUT_ROOT = organized_videos의 형제 'processed_data'
    processed_base = org_videos_dir.parent / "processed_data"
    processed_base.mkdir(parents=True, exist_ok=True)

    # 4) OUTPUT_ROOT/<category> 디렉터리 보장
    output_category_dir = processed_base / target_category
    output_category_dir.mkdir(parents=True, exist_ok=True)

    # 5) image_extract_2.py 실행 준비
    #    INPUT_ROOT = organized_videos, EXCLUDED_CATEGORIES = (organized_videos 바로 아래의 형제 중 target 제외)
    input_root = org_videos_dir
    siblings = list_immediate_subdirs(input_root)
    excluded = [c for c in siblings if c != target_category]
    excluded_str = ",".join(excluded)

    assign_log_dir = processed_base / "_logs" / "extract"
    assign_log_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "python3", str(IMAGE_EXTRACT_2_PY),
        "--input_root", str(input_root),
        "--output_root", str(processed_base),
        "--assign_log_dir", str(assign_log_dir),
        "--excluded_categories", excluded_str,
        "--num_frames", "30",
    ]

    print(f"[STEP1] image_extract_2.py 실행")
    print(f"        INPUT_ROOT          = {input_root}")
    print(f"        OUTPUT_ROOT         = {processed_base} (→ {output_category_dir})")
    print(f"        EXCLUDED_CATEGORIES = {excluded_str or '(없음)'}")

    subprocess.run(cmd, check=True)

    return processed_base, output_category_dir, target_category


def run_import_autolabeling(image_dir: Path, target_category: str) -> None:
    """
    import_autolabeling_2.py 실행 (고정 값 버전)
    - image_dir = processed_data/<target_category>
    - ORGANIZATIONS, PROJECT_NAME, LABELS, ASSIGNEES는 코드에 하드코딩
    """
    if not image_dir.exists() or not has_any_images(image_dir):
        print(f"[INFO] 이미지가 없어 import_autolabeling_2.py 건너뜁니다: {image_dir}")
        return
    
    # === 고정 값 세팅 ===
    org_name = "vietnamlabeling"
    project_name = f"vietnam_{target_category}"
    labels = ["person", f"{target_category}_person"]
    assignees = ["user08", "user02", "user03", "user04", "user05", "user06"]

    cmd = [
        "python3", str(IMPORT_AUTOLABELING_2_PY),
        "--org_name", org_name,
        "--image_dir", str(image_dir),
        "--project_name", project_name,
        "--labels", labels,
        "--assignees", *assignees,
    ]
    print(f"[STEP2] import_autolabeling_2.py 실행")
    print(f"        org={org_name} | project={project_name} | image_dir={image_dir}")
    subprocess.run(cmd, check=True)

# ------------------ 메인 ------------------

def main() -> None:
    # 1) 스냅샷 비교
    before = load_snapshot(SNAPSHOT_CSV)
    now_list = scan_all_dirs()
    now = set(now_list)
    added = sorted(now - before)

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not before:
        print(f"[{ts}] 최초 실행: 기준 스냅샷을 생성했습니다. 다음 실행부터 신규 폴더를 보고/처리합니다.")
        save_snapshot(SNAPSHOT_CSV, now_list)
        return

    if not added:
        print(f"[{ts}] 새로 생성된 폴더가 없습니다.")
        save_snapshot(SNAPSHOT_CSV, now_list)
        return

    print(f"[{ts}] 신규 폴더 {len(added)}개 발견:")
    for d in added:
        print(f"  ➕ {d}")

    # 2) 영상이 있는 폴더만 트리거로 사용
    trigger_dirs = []
    for d in added:
        dpath = Path(d)
        if folder_has_videos(dpath):
            trigger_dirs.append(dpath)
        else:
            print(f"  ⤷ (동영상 없음, 건너뜀) {d}")

    if not trigger_dirs:
        print("[INFO] 트리거 가능한(동영상 포함) 신규 폴더가 없습니다.")
        save_snapshot(SNAPSHOT_CSV, now_list)
        return

    # 3) organized_videos/<category> 단위로 중복 제거
    processed_keys = set()  # (organized_videos_path, category)
    for d in trigger_dirs:
        org_videos_dir = find_ancestor_with_name(d, "organized_videos")
        if org_videos_dir is None:
            print(f"[WARN] 'organized_videos' 상위가 아니라서 건너뜀: {d}")
            continue
        processed_keys.add((str(org_videos_dir.resolve()), d.name))

    # 4) 각 카테고리에 대해: 프레임 추출 → 자동 라벨링
    for org_videos_path, category in sorted(processed_keys):
        try:
            # STEP 1: 프레임 추출
            processed_base, image_dir, target_category = run_image_extract_for_category(
                Path(org_videos_path) / category
            )
            # STEP 2: 자동 라벨링(업로드)
            run_import_autolabeling(image_dir, target_category)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] 파이프라인 실패: organized_videos={org_videos_path}, category={category} | {e}")
        except Exception as e:
            print(f"[ERROR] 파이프라인 예외: organized_videos={org_videos_path}, category={category} | {e}")

    # 5) 스냅샷 갱신
    save_snapshot(SNAPSHOT_CSV, now_list)
    print("[DONE] 신규 폴더 처리 및 스냅샷 갱신 완료.")


if __name__ == "__main__":
    main()