#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import cv2
import csv
import argparse
import time
from pathlib import Path
from multiprocessing import Pool
from collections import defaultdict
from datetime import datetime

VIDEO_EXTS = (".mp4", ".avi", ".mov")
IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".bmp", ".webp")

def extract_frames(video_path, save_dir, num_frames=30):
    """영상에서 일정 수의 프레임을 균등 간격으로 추출"""
    cap = cv2.VideoCapture(str(video_path))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    if total_frames <= 0:
        print(f"⚠️ {video_path.name}: 프레임 수를 읽지 못했습니다. 스킵")
        cap.release()
        return 0

    if total_frames < num_frames:
        print(f"⚠️ {video_path.name}: 프레임 부족 ({total_frames}개), 스킵")
        cap.release()
        return 0

    interval = max(1, total_frames // num_frames)
    base_name = video_path.stem
    save_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for i in range(num_frames):
        frame_idx = i * interval
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = cap.read()
        if ret:
            save_path = save_dir / f"{base_name}_frame{i:02d}.jpg"
            cv2.imwrite(str(save_path), frame)
            count += 1
    cap.release()
    return count

def is_processed(log_file, root_category, sub_category, video_file):
    """처리 여부 확인"""
    if not log_file.exists():
        return False
    with log_file.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get('root_category') == root_category and
                row.get('sub_category') == sub_category and
                row.get('filename') == video_file):
                return True
    return False

def mark_as_processed(log_file, root_category, sub_category, video_file):
    """처리 완료 로그 기록"""
    file_exists = log_file.exists()
    with log_file.open('a', newline='', encoding='utf-8') as f:
        fieldnames = ['root_category', 'sub_category', 'filename']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            'root_category': root_category,
            'sub_category': sub_category,
            'filename': video_file
        })

def recommend_num_workers(reserve_cores=2):
    """사용 가능한 CPU 기준 워커 수 추천"""
    logical_cores = os.cpu_count() or 1
    try:
        load_avg_1min = os.getloadavg()[0]
    except Exception:
        load_avg_1min = 0.0
    usable_cores = max(1, logical_cores - reserve_cores)
    utilization = min(load_avg_1min / max(1, logical_cores), 1.0)
    recommended = int(usable_cores * (1 - utilization * 0.5))
    return max(1, recommended)

def process_video_task(task):
    """Pool용 단일 작업"""
    video_path, save_dir, num_frames = task
    return save_dir, extract_frames(video_path, save_dir, num_frames)

def main():
    parser = argparse.ArgumentParser(description="프레임 추출기 (.env 불사용, CLI 인자 기반)")
    parser.add_argument("--input_root", required=True, help="organized_videos 경로")
    parser.add_argument("--output_root", required=True, help="processed_data 경로")
    parser.add_argument("--assign_log_dir", default=None, help="로그 디렉터리 (기본: output_root/_logs/extract)")
    parser.add_argument("--excluded_categories", default="", help="쉼표구분 제외 카테고리")
    parser.add_argument("--num_frames", type=int, default=30, help="동영상당 추출 프레임 수")
    args = parser.parse_args()

    input_root = Path(args.input_root).resolve()
    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    assign_log_dir = Path(args.assign_log_dir).resolve() if args.assign_log_dir else (output_root / "_logs" / "extract")
    assign_log_dir.mkdir(parents=True, exist_ok=True)

    log_file = assign_log_dir / "processed_videos.csv"
    summary_log_path = assign_log_dir / "frame_summary_log.csv"

    excluded_categories = set(x.strip() for x in args.excluded_categories.split(",") if x.strip())

    # root_category는 input_root의 부모 이름(= organized_videos 상위 이름, 예: vietnam_data)
    # 또는 input_root의 부모가 존재하지 않으면 input_root의 이름
    root_category = input_root.parent.name if input_root.parent and input_root.parent.name else input_root.name

    num_workers = recommend_num_workers()
    print(f"자동 추천된 num_workers: {num_workers}")
    print(f"입력: {input_root}")
    print(f"출력: {output_root} (카테고리별 자동 생성)")
    print(f"제외: {', '.join(sorted(excluded_categories)) or '(없음)'}")

    # 작업 리스트 구성
    video_tasks = []
    save_info = []  # (sub_category, video_file)
    category_frame_counter = defaultdict(int)
    total_extracted_frames = 0

    for category in sorted(os.listdir(input_root)):
        if category in excluded_categories:
            print(f"❌ 제외된 폴더: {category}")
            continue
        category_path = input_root / category
        if not category_path.is_dir():
            continue

        # 출력 카테고리 폴더 보장 (요구사항: 없으면 생성)
        (output_root / category).mkdir(parents=True, exist_ok=True)

        # 현재 카테고리의 직접 하위의 동영상만 처리(기존 로직과 동일)
        video_files = [f for f in os.listdir(category_path) if f.lower().endswith(VIDEO_EXTS)]
        for video_file in sorted(video_files):
            video_path = category_path / video_file

            if is_processed(log_file, root_category, category, video_file):
                print(f"✅ 이미 처리됨: {video_file}")
                continue

            # 저장 디렉터리: output_root/<category>/<video_base_name>
            video_name = Path(video_file).stem
            save_dir = output_root / category / video_name

            video_tasks.append((video_path, save_dir, args.num_frames))
            save_info.append((category, video_file))

    print(f"총 처리할 영상 수: {len(video_tasks)}")

    if video_tasks:
        # 병렬 처리
        start_time = time.time()
        with Pool(processes=num_workers) as pool:
            results = pool.map(process_video_task, video_tasks)

        # 결과 기록
        for idx, (save_dir, frame_count) in enumerate(results):
            sub_category, video_file = save_info[idx]
            if frame_count > 0:
                category_frame_counter[sub_category] += frame_count
                total_extracted_frames += frame_count
            mark_as_processed(log_file, root_category, sub_category, video_file)

        print("프레임 추출 완료")

        # 요약 로그 저장
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_exists = summary_log_path.exists()
        with summary_log_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["date", "root_category", "sub_category", "image_count"])
            for cat, count in category_frame_counter.items():
                print(f" {cat}: {count}장")
                writer.writerow([now, root_category, cat, count])
            print(f"전체 이미지 수: {total_extracted_frames}장")
            writer.writerow([now, root_category, "TOTAL", total_extracted_frames])

        elapsed = time.time() - start_time
        print(f"✅ 모든 영상 처리 및 로그 작성 완료 | 총 소요 시간: {elapsed:.1f}초")
    else:
        print("처리할 영상이 없습니다.")

if __name__ == "__main__":
    main()
