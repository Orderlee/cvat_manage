import os
import cv2
import csv
import json
import psutil
import time
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime

# .env 로드
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def extract_frames(video_path, save_dir, num_frames=30):
    """영상에서 일정 수의 프레임을 균등 간격으로 추출"""
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    if total_frames < num_frames:
        print(f"⚠️ {os.path.basename(video_path)}: 프레임 부족 ({total_frames}개), 스킵")
        cap.release()
        return 0  # 프레임 저장 안 함

    interval = total_frames // num_frames
    base_name = os.path.splitext(os.path.basename(video_path))[0]
    os.makedirs(save_dir, exist_ok=True)

    count = 0
    for i in range(num_frames):
        frame_idx = i * interval
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = cap.read()
        if ret:
            save_path = os.path.join(save_dir, f"{base_name}_frame{i:02d}.jpg")
            cv2.imwrite(save_path, frame)
            count += 1
    cap.release()
    return count  # 저장된 프레임 수 반환


def is_processed(log_file, root_category, sub_category, video_file):
    """처리 여부 확인"""
    if not os.path.exists(log_file):
        return False
    with open(log_file, 'r') as f:
        reader = csv.DictReader(f)
        return any(
            row['root_category'] == root_category and
            row['sub_category'] == sub_category and
            row['filename'] == video_file
            for row in reader
        )


def mark_as_processed(log_file, root_category, sub_category, video_file):
    """처리 완료 로그 기록"""
    file_exists = os.path.exists(log_file)
    with open(log_file, 'a', newline='') as f:
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
    logical_cores = os.cpu_count()
    load_avg_1min = os.getloadavg()[0]
    usable_cores = max(1, logical_cores - reserve_cores)
    utilization = min(load_avg_1min / logical_cores, 1.0)
    recommended = int(usable_cores * (1 - utilization * 0.5))
    return max(1, recommended)


# 영상 1개 처리할 함수 (Pool용)
def process_video_task(task):
    video_path, save_dir, num_frames = task
    return save_dir, extract_frames(video_path, save_dir, num_frames)


if __name__ == "__main__":
    start_time = time.time()
    num_workers = recommend_num_workers()
    print(f"자동 추천된 num_workers: {num_workers}")

    input_root = os.getenv("INPUT_ROOT")
    output_root = os.getenv("OUTPUT_ROOT")
    log_dir = os.getenv("ASSIGN_LOG_DIR", ".")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "processed_videos.csv")

    # 제외할 카테고리
    excluded_raw = os.getenv("EXCLUDED_CATEGORIES", "")
    excluded_categories = set(x.strip() for x in excluded_raw.split(",") if x.strip())

    root_category = Path(input_root).parts[-2]
    category_frame_counter = defaultdict(int)
    total_extracted_frames = 0

    # 병렬 처리를 위한 작업 리스트 생성
    video_tasks = []
    save_info = []  # (sub_category, video_file) 정보 저장

    for category in os.listdir(input_root):
        if category in excluded_categories:
            print(f"❌ 제외된 폴더: {category}")
            continue

        category_path = os.path.join(input_root, category)
        if not os.path.isdir(category_path):
            continue

        sub_category = category
        video_files = [f for f in os.listdir(category_path) if f.lower().endswith((".mp4", ".avi", ".mov"))]

        for video_file in video_files:
            video_path = os.path.join(category_path, video_file)

            if is_processed(log_file, root_category, sub_category, video_file):
                print(f"✅ 이미 처리됨: {video_file}")
                continue

            video_name = os.path.splitext(video_file)[0]
            save_dir = os.path.join(output_root, sub_category, video_name)

            video_tasks.append((video_path, save_dir, 30))
            save_info.append((sub_category, video_file))

    print(f"총 처리할 영상 수: {len(video_tasks)}")

    # 병렬 처리 실행
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
    summary_log_path = os.path.join(log_dir, "frame_summary_log.csv")
    file_exists = os.path.exists(summary_log_path)

    print("추출 요약")
    with open(summary_log_path, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date", "root_category", "sub_category", "image_count"])
        for cat, count in category_frame_counter.items():
            print(f" {cat}: {count}장")
            writer.writerow([now, root_category, cat, count])
        print(f"전체 이미지 수: {total_extracted_frames}장")
        writer.writerow([now, root_category, "TOTAL", total_extracted_frames])

    print("✅ 모든 영상 처리 및 로그 작성 완료")
    end_time = time.time()
    elapsed = end_time - start_time
    print(f" 총 소요 시간: {elapsed:.1f}초")