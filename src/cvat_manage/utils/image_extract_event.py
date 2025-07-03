import os
import cv2
import csv
import json
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
from dotenv import load_dotenv

# -------------------- 환경 변수 -------------------- #
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# -------------------- JSON 이벤트 처리 -------------------- #
def load_json(json_path):
    with open(json_path, 'r') as f:
        return json.load(f)

def get_event_time(json_path):
    data = load_json(json_path)
    return [(clip['category'], clip['timestamp']) for clip in data.get('clips', {}).values()]

def get_video_info(video_path):
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise ValueError(f"❌ 비디오를 열 수 없습니다: {video_path}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.release()

    if fps <= 0 or total_frames <= 0:
        raise ValueError(f"❌ FPS 또는 총 프레임 수가 유효하지 않습니다: {video_path} (fps={fps}, frames={total_frames})")

    return fps, total_frames

def get_capture_frame_indices(video_path, time_interval_sec=2, event_interval_sec=0.5):
    fps, total_frames = get_video_info(video_path)

    normal_interval = max(1, round(fps * time_interval_sec))
    event_interval = max(1, round(fps * event_interval_sec))

    frames = set(range(0, total_frames, normal_interval))

    json_path = os.path.splitext(video_path)[0] + ".json"
    if os.path.exists(json_path):
        event_times = get_event_time(json_path)
        for category_name, (start_frame, end_frame) in event_times:
            frames.update(range(start_frame, end_frame, event_interval))

    return sorted(f for f in frames if f < total_frames)

# -------------------- 병렬 분할 -------------------- #
def chunk_indices(total_frames, num_chunks):
    chunk_size = total_frames // num_chunks
    return [(i * chunk_size, (i + 1) * chunk_size) if i < num_chunks - 1 else (i * chunk_size, total_frames)
            for i in range(num_chunks)]

# -------------------- 프레임 저장 작업 -------------------- #
def extract_frames_worker(args):
    start_idx, end_idx, video_path, target_indices, output_root, category = args

    cap = cv2.VideoCapture(video_path)
    video_filename = os.path.splitext(os.path.basename(video_path))[0]
    save_dir = os.path.join(output_root, category, video_filename)
    os.makedirs(save_dir, exist_ok=True)

    worker_targets = sorted([idx for idx in target_indices if start_idx <= idx < end_idx])
    target_cursor = 0
    frame_idx = start_idx
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)

    with tqdm(total=len(worker_targets), desc=f"{video_filename} [{start_idx}-{end_idx}]") as pbar:
        while frame_idx < end_idx and target_cursor < len(worker_targets):
            if frame_idx != worker_targets[target_cursor]:
                cap.read()
                frame_idx += 1
                continue

            ret, frame = cap.read()
            if not ret:
                break

            out_name = f"{video_filename}_{frame_idx:06d}.jpg"
            out_path = os.path.join(save_dir, out_name)
            cv2.imwrite(out_path, frame)

            frame_idx += 1
            target_cursor += 1
            pbar.update(1)

    cap.release()

# -------------------- 처리 로그 확인/기록 -------------------- #
def is_processed(log_file, root_category, sub_category, video_file):
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

# -------------------- 메인 실행 -------------------- #
if __name__ == "__main__":
    num_workers = int(os.getenv("NUM_WORKERS", "2"))

    input_root = os.getenv("INPUT_ROOT")
    output_root = os.getenv("OUTPUT_ROOT")
    log_dir = os.getenv("ASSIGN_LOG_DIR", ".")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "processed_videos.csv")

    excluded_raw = os.getenv("EXCLUDED_CATEGORIES", "")
    excluded_categories = set(x.strip() for x in excluded_raw.split(",") if x.strip())

    root_category = Path(input_root).parts[-2]

    for category in os.listdir(input_root):
        category_path = os.path.join(input_root, category)
        if not os.path.isdir(category_path):
            continue

        video_files = [f for f in os.listdir(category_path) if f.lower().endswith((".mp4", ".avi", ".mov"))]

        for video_file in video_files:
            video_path = os.path.join(category_path, video_file)
            sub_category = os.path.basename(os.path.dirname(video_path))

            if sub_category in excluded_categories:
                print(f"❌ 제외된 폴더: {sub_category}")
                continue

            if is_processed(log_file, root_category, sub_category, video_file):
                print(f"✅ 이미 처리됨: {video_file}, 스킵합니다.")
                continue

            try:
                target_frame_indices = get_capture_frame_indices(video_path, 2, 0.5)
            except ValueError as e:
                print(f"[오류] {video_file}: {e}")
                continue

            print(f"📹 {video_file}: 총 {len(target_frame_indices)}개 프레임 추출 예정")

            total_frames = get_video_info(video_path)[1]
            chunk_ranges = chunk_indices(total_frames, num_workers)
            args_list = [
                (start, end, video_path, target_frame_indices, output_root, sub_category)
                for (start, end) in chunk_ranges
            ]

            with Pool(processes=num_workers) as pool:
                pool.map(extract_frames_worker, args_list)

            mark_as_processed(log_file, root_category, sub_category, video_file)

    print("✅ 모든 프레임 추출 완료")
