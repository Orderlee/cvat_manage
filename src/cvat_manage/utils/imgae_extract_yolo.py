import os
import cv2
import torch
import csv
import zipfile
from pathlib import Path
from math import ceil
from multiprocessing import Pool
from ultralytics import YOLO
from tqdm import tqdm
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

def chunk_indices(total_frames, num_chunks):
    chunk_size = total_frames // num_chunks
    return [(i * chunk_size, (i + 1) * chunk_size) if i < num_chunks - 1 else (i * chunk_size, total_frames)
            for i in range(num_chunks)]

def detect_and_extract_worker(args):
    start_idx, end_idx, device_id, video_path, fps, sampling_rate, output_root, category = args

    torch.cuda.set_device(device_id)
    yolo = YOLO("yolov8n.pt")
    cap = cv2.VideoCapture(video_path)
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)

    video_filename = os.path.splitext(os.path.basename(video_path))[0]
    save_dir = os.path.join(output_root, category, video_filename)
    os.makedirs(save_dir, exist_ok=True)

    frame_idx = start_idx
    with tqdm(total=end_idx - start_idx, desc=f"{video_filename} GPU:{device_id}") as pbar:
        while frame_idx < end_idx:
            ret, frame = cap.read()
            if not ret:
                break
            
            if frame_idx % sampling_rate != 0:
                frame_idx += 1
                pbar.update(1)
                continue

            results = yolo(frame, verbose=False)[0]
            has_person = any(results.names[int(cls)] == "person" for cls in results.boxes.cls)

            if has_person:
                out_name = f"{video_filename}_{frame_idx}.jpg"
                out_path = os.path.join(save_dir, out_name)
                cv2.imwrite(out_path, frame)

            frame_idx += 1
            pbar.update(1)
    
    cap.release()

def compress_images(output_root, batch_size=100):
    for category in os.listdir(output_root):
        category_path = os.path.join(output_root, category)

        if not os.path.isdir(category_path):
            continue
        
        for folder_name in os.listdir(category_path):
            folder_path = os.path.join(category_path, folder_name)

            if not os.path.isdir(folder_path):
                continue

            image_files = sorted([f for f in os.listdir(folder_path) if f.lower().endswith(".jpg")])
            total_images = len(image_files)

            if total_images == 0:
                continue

            num_batches = ceil(total_images / batch_size)

            for i in range(num_batches):
                batch_files = image_files[i * batch_size: (i + 1) * batch_size]
                zip_filename = f"{folder_name}_{i + 1:02d}.zip"
                zip_path = os.path.join(folder_path, zip_filename)

                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for img_file in batch_files:
                        img_path = os.path.join(folder_path, img_file)
                        zipf.write(img_path, arcname=img_file)
                
                print(f"Created: {zip_path} with {len(batch_files)} images")
        
        print("모든 압축 파일 생성이 완료되었습니다.")

def is_processed(log_file, video_file):
    if not os.path.exists(log_file):
        return False
    with open(log_file, 'r') as f:
        reader = csv.DictReader(f)
        return video_file in {row['filename'] for row in reader}

def mark_as_processed(log_file, video_file):
    file_exists = os.path.exists(log_file)
    with open(log_file, 'a', newline='') as f:
        fieldnames = ['filename']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow({'filename': video_file})

if __name__ == "__main__":
    num_gpus = 2
    seconds_interval = 2

    input_root = os.getenv("INPUT_ROOT")
    output_root = os.getenv("OUTPUT_ROOT")
    log_dir = os.getenv("ASSIGN_LOG_DIR", ".")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "processed_videos.csv")

    
    # 제외할 폴더 이름 .env에다가 작성해주세요
    excluded_raw = os.getenv("EXCLUDED_CATEGORIES","")
    excluded_categories = set(x.strip() for x in excluded_raw.split(",") if x.strip())
    
    for category in os.listdir(input_root):
        if category in excluded_categories:
            print(f"❌ 제외된 폴더: {category}")
            continue

        category_path = os.path.join(input_root, category)
        if not os.path.isdir(category_path):
            continue

        video_files = [f for f in os.listdir(category_path) if f.lower().endswith((".mp4", ".avi", ".mov"))]

        for video_file in video_files:
            video_path = os.path.join(category_path, video_file)

            if is_processed(log_file, video_file):
                print(f"{video_file} 이미 처리됨, 스킵합니다.")
                continue

            cap = cv2.VideoCapture(video_path)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            fps = cap.get(cv2.CAP_PROP_FPS)
            cap.release()

            sampling_rate = int(fps * seconds_interval)

            chunk_ranges = chunk_indices(total_frames, num_gpus)
            args_list = [
                (start, end, device_id, video_path, fps, sampling_rate, output_root, category)
                for device_id, (start, end) in enumerate(chunk_ranges)
            ]

            with Pool(processes=num_gpus) as pool:
                pool.map(detect_and_extract_worker, args_list)

            mark_as_processed(log_file, video_file)

    print(f"✅ 사람 감지된 프레임을 {seconds_interval}초마다 저장 완료")

    compress_images(output_root, batch_size=100)
