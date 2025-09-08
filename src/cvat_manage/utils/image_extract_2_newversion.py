# import os
# import cv2
# import csv
# import time
# from pathlib import Path
# from collections import defaultdict, deque
# from datetime import datetime
# from multiprocessing import Pool, get_start_method, set_start_method, Manager
# import threading

# from tqdm import tqdm
# from dotenv import load_dotenv

# """
# 고급 스케줄러 (세마포어 기반, Manager.Value 적용)
# ────────────────────────────────────────────────────────────────────
# 요구사항 반영:
# - YOLO로 사람을 찾아, '사람이 가장 많이 탐지된 연속 15분' 구간만 골라서
#   해당 구간에서 '2초당 1프레임(0.5fps)'로 이미지 추출
# - 기존 멀티-GPU 스케줄러/세마포어/로그 구조는 유지

# 구현 개요:
# - detect_and_extract_worker:
#   1) 1차 패스(샘플링 탐지): sampling_rate 간격으로 프레임을 훑으며 초 단위로 person 카운트를 누적
#   2) 15분(900초) 슬라이딩 윈도우로 최다 탐지 구간 계산
#   3) 2차 패스: 해당 구간에서만 2초당 1프레임으로 저장
# """

# # =============================
# # 환경 변수 로드 (.env는 상위 폴더에 있다고 가정)
# # =============================
# env_path = Path(__file__).resolve().parent.parent / ".env"
# load_dotenv(dotenv_path=env_path)

# # 공통 경로/설정
# INPUT_ROOT = os.getenv("INPUT_ROOT")
# OUTPUT_ROOT = os.getenv("OUTPUT_ROOT")
# ASSIGN_LOG_DIR = os.getenv("ASSIGN_LOG_DIR", ".")
# os.makedirs(ASSIGN_LOG_DIR, exist_ok=True)
# PROCESSED_LOG = os.path.join(ASSIGN_LOG_DIR, "processed_videos.csv")
# SUMMARY_LOG = os.path.join(ASSIGN_LOG_DIR, "frame_summary_log.csv")

# # 모드 전환
# PERSON_ONLY = os.getenv("PERSON_ONLY", "0") in ("1", "true", "True")
# UNIFORM_FRAMES_PER_SEC = float(os.getenv("UNIFORM_FRAMES_PER_SEC", "3"))

# # 사람 감지(세그먼트/병렬) 설정
# PERSON_SEGMENTS = int(os.getenv("PERSON_SEGMENTS", "4"))             # 비디오를 N등분
# WORKERS_PER_GPU = int(os.getenv("WORKERS_PER_GPU", str(PERSON_SEGMENTS)))  # GPU당 동시 프로세스 수
# PERSON_SAMPLING_RATE = int(os.getenv("PERSON_SAMPLING_RATE", "5"))    # 1차 패스: n프레임마다 탐지
# YOLO_WEIGHTS = os.getenv("YOLO_WEIGHTS", "yolov8n.pt")
# PERSON_CONF = float(os.getenv("PERSON_CONF", "0.25"))

# # 멀티-GPU: 쉼표로 구분된 GPU ID 리스트 (예: "0,1")
# GPU_IDS_ENV = os.getenv("GPU_IDS", "0,1")   # 기본값: 2개 GPU 있다고 가정

# # 제외 카테고리(콤마 구분)
# EXCLUDED_CATEGORIES = set(
#     x.strip() for x in os.getenv("EXCLUDED_CATEGORIES", "").split(",") if x.strip()
# )

# # [신규] 15분 타겟 구간(초)
# TARGET_WINDOW_SEC = int(os.getenv("TARGET_WINDOW_SEC", str(15 * 60)))
# # [고정] 2초당 1프레임 저장
# EXTRACT_EVERY_SEC = 2.0

# # =============================
# # YOLO / torch 임포트 (사람 감지 모드에서 사용)
# # =============================
# _YOLO_AVAILABLE = True
# try:
#     import torch
#     from ultralytics import YOLO
# except Exception as e:
#     _YOLO_AVAILABLE = False
#     _YOLO_IMPORT_ERROR = e


# # =============================
# # 유틸 함수들 (균일간격 모드 유지)
# # =============================
# def get_frames_to_extract(duration_sec, fps, frames_per_sec=3):
#     if fps <= 0 or duration_sec <= 0:
#         return 0, 0
#     num_frames = int(duration_sec * frames_per_sec)
#     interval = int(fps / frames_per_sec)
#     if interval < 1:
#         interval = 1
#         num_frames = int(duration_sec * fps)
#     return num_frames, interval


# def extract_frames_uniform(video_path, save_dir, num_frames, interval_frames):
#     cap = cv2.VideoCapture(video_path)
#     total_video_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

#     if num_frames == 0 or total_video_frames < interval_frames:
#         print(f"⚠️ {os.path.basename(video_path)}: 추출 프레임 없음/간격 과대. 스킵")
#         cap.release()
#         return 0

#     base_name = os.path.splitext(os.path.basename(video_path))[0]
#     os.makedirs(save_dir, exist_ok=True)

#     count = 0
#     for i in range(num_frames):
#         frame_idx = i * interval_frames
#         if frame_idx >= total_video_frames:
#             break
#         cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
#         ret, frame = cap.read()
#         if ret:
#             save_path = os.path.join(save_dir, f"{base_name}_frame{i:06d}.jpg")
#             cv2.imwrite(save_path, frame)
#             count += 1
#     cap.release()
#     return count


# def is_processed(log_file, root_category, sub_category, video_file):
#     if not os.path.exists(log_file):
#         return False
#     with open(log_file, 'r') as f:
#         reader = csv.DictReader(f)
#         return any(
#             row['root_category'] == root_category and
#             row['sub_category'] == sub_category and
#             row['filename'] == video_file
#             for row in reader
#         )


# def mark_as_processed(log_file, root_category, sub_category, video_file):
#     file_exists = os.path.exists(log_file)
#     with open(log_file, 'a', newline='') as f:
#         fieldnames = ['root_category', 'sub_category', 'filename']
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         if not file_exists:
#             writer.writeheader()
#         writer.writerow({
#             'root_category': root_category,
#             'sub_category': sub_category,
#             'filename': video_file
#         })


# # =============================
# # [핵심] 사람 감지용 워커 (업데이트 버전)
# # =============================
# def detect_and_extract_worker(args):
#     """
#     args: (start_idx, end_idx, device_id, video_path, fps, sampling_rate, output_root, category)
#     동작:
#       1) [start_idx, end_idx) 범위를 1차 패스로 훑으며 PERSON 존재 여부를 '초 단위' 카운트에 누적
#       2) 해당 범위 내에서 '연속 15분(900초)' 윈도우 중 합계가 최대인 구간 찾기
#       3) 그 구간에서만 2초당 1프레임으로 이미지 저장
#     반환: (saved_count, save_dir)
#     """
#     start_idx, end_idx, device_id, video_path, fps, sampling_rate, output_root, category = args

#     if not _YOLO_AVAILABLE:
#         raise RuntimeError(
#             f"ultralytics/torch 임포트 실패: {_YOLO_IMPORT_ERROR}\n"
#             f"사람 감지 모드를 사용하려면 `pip install ultralytics torch`를 먼저 설치하세요."
#         )

#     # 디바이스 설정
#     use_cuda = False
#     if 'torch' in globals():
#         use_cuda = torch.cuda.is_available() and device_id is not None and device_id >= 0
#         if use_cuda:
#             torch.cuda.set_device(device_id)

#     # 모델 로드 (각 프로세스 별 1회)
#     yolo = YOLO(YOLO_WEIGHTS)

#     # 비디오 준비
#     cap = cv2.VideoCapture(video_path)
#     total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
#     fps = fps or cap.get(cv2.CAP_PROP_FPS) or 0.0

#     # 출력 폴더
#     video_filename = os.path.splitext(os.path.basename(video_path))[0]
#     save_dir = os.path.join(output_root, category, video_filename)
#     os.makedirs(save_dir, exist_ok=True)

#     # 경계 보정
#     start_idx = max(0, start_idx)
#     end_idx = min(end_idx, total_frames) if end_idx > 0 else total_frames
#     if fps <= 0 or start_idx >= end_idx:
#         cap.release()
#         return 0, save_dir

#     # ---------------------------
#     # 1) 1차 패스: 샘플링 탐지 → 초 단위 카운트
#     # ---------------------------
#     # 메모리 절약을 위해 '초 -> 카운트'만 저장
#     # 초 계산: second = int(frame_idx / fps)
#     person_count_by_sec = defaultdict(int)

#     # 1차 패스 진행 바
#     with tqdm(total=(end_idx - start_idx), desc=f"[PASS1] {video_filename} [{start_idx}-{end_idx}) GPU:{device_id}") as pbar:
#         frame_idx = start_idx
#         cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)

#         while frame_idx < end_idx:
#             ret, frame = cap.read()
#             if not ret:
#                 break

#             # 샘플링 스킵 (속도)
#             if sampling_rate > 1 and (frame_idx % sampling_rate != 0):
#                 frame_idx += 1
#                 pbar.update(1)
#                 continue

#             # YOLO 추론 (conf 지정) — 결과가 비어도 names/boxes 접근 안전
#             results = yolo(frame, verbose=False, conf=PERSON_CONF)[0]
#             has_person = False
#             if results.boxes is not None and len(results.boxes.cls) > 0:
#                 for cls_idx in results.boxes.cls.tolist():
#                     # 안전 처리: names 딕셔너리 접근
#                     name = results.names.get(int(cls_idx), "")
#                     if name == "person":
#                         has_person = True
#                         break

#             if has_person:
#                 sec = int(frame_idx / fps)
#                 person_count_by_sec[sec] += 1

#             frame_idx += 1
#             pbar.update(1)

#     # ---------------------------
#     # 2) 최적 15분(900초) 연속 구간 계산 (슬라이딩 윈도우)
#     # ---------------------------
#     total_secs = int((end_idx - start_idx) / fps) + 1
#     if total_secs <= 0:
#         cap.release()
#         return 0, save_dir

#     window_len = min(TARGET_WINDOW_SEC, total_secs)  # 영상이 15분보다 짧으면 영상 길이로
#     # 초별 카운트를 배열로 변환 (start_sec=0 기준)
#     sec_counts = [0] * total_secs
#     for s, c in person_count_by_sec.items():
#         local_s = s - int(start_idx / fps)
#         if 0 <= local_s < total_secs:
#             sec_counts[local_s] += c

#     # 슬라이딩 윈도우로 최대 합 찾기 (prefix sum)
#     prefix = [0]
#     for v in sec_counts:
#         prefix.append(prefix[-1] + v)

#     best_sum = -1
#     best_start_sec_local = 0
#     for i in range(0, total_secs - window_len + 1):
#         j = i + window_len
#         ssum = prefix[j] - prefix[i]
#         if ssum > best_sum:
#             best_sum = ssum
#             best_start_sec_local = i

#     # 로컬 구간(시작초) → 실제 프레임 인덱스
#     best_start_sec_global = int(start_idx / fps) + best_start_sec_local
#     best_end_sec_global = best_start_sec_global + window_len

#     # 프레임 범위 (닫힌-열린)
#     best_start_frame = int(best_start_sec_global * fps)
#     best_end_frame = int(min(end_idx, best_end_sec_global * fps))

#     # 예외: 1차 패스에서 단 한 번도 사람이 안 잡힌 경우 → 규칙 그대로 15분 구간 사용
#     # (즉, 사람 0이어도 해당 구간에서 2초당 1프레임 추출)
#     if best_sum <= 0:
#         # start_idx 기준으로 가능한 최대 15분(또는 나머지 길이) 사용
#         best_start_frame = start_idx
#         best_end_frame = min(end_idx, start_idx + int(window_len * fps))

#     # ---------------------------
#     # 3) 2차 패스: 해당 구간에서만 2초당 1프레임 저장
#     # ---------------------------
#     interval_frames = max(1, int(round(fps * EXTRACT_EVERY_SEC)))
#     saved = 0

#     # 2차 패스 진행 바
#     with tqdm(total=max(0, best_end_frame - best_start_frame),
#               desc=f"[PASS2] {video_filename} [{best_start_frame}-{best_end_frame}) GPU:{device_id}") as pbar:

#         for fidx in range(best_start_frame, best_end_frame, interval_frames):
#             cap.set(cv2.CAP_PROP_POS_FRAMES, fidx)
#             ret, frame = cap.read()
#             if not ret:
#                 break

#             out_name = f"{video_filename}_{fidx:06d}.jpg"
#             out_path = os.path.join(save_dir, out_name)
#             cv2.imwrite(out_path, frame)
#             saved += 1

#             # 진행바는 실제 읽은 프레임 수 기준으로 대략 업데이트
#             pbar.update(interval_frames)

#     cap.release()
#     return saved, save_dir


# # =============================
# # [중요] 멀티프로세싱용 Top-level 러너 (Picklable)
# # =============================
# def runner_top(task_args, video_key, gpu_id):
#     saved, _ = detect_and_extract_worker(task_args)
#     return (video_key, gpu_id, int(saved))


# # =============================
# # GPU 유틸: GPU 리스트 파싱
# # =============================
# def parse_gpu_ids() -> list:
#     env = GPU_IDS_ENV.strip()
#     ids = []
#     if env:
#         for tok in env.split(','):
#             tok = tok.strip()
#             if tok and tok.lstrip('-').isdigit():
#                 ids.append(int(tok))

#     if ids:
#         return ids

#     if _YOLO_AVAILABLE and 'torch' in globals() and torch.cuda.is_available():
#         n = torch.cuda.device_count()
#         if n > 0:
#             return list(range(n))

#     return [-1]


# # =============================
# # 세그먼트 태스크 빌더 (비디오→GPU 고정, 내부 세그먼트 병렬)
# # =============================
# def build_segment_tasks_for_video(video_path: str, output_root: str, sub_category: str,
#                                   sampling_rate: int, device_id: int, segments: int):
#     """
#     ※ 주의
#     - 15분 최적 구간은 일반적으로 '전체 비디오'를 대상으로 하나,
#       여기서는 기존 스케줄러 구조(세그먼트 병렬)를 최대한 유지하기 위해
#       '각 세그먼트 내부에서' 최적 15분을 찾고 그 세그먼트에서만 추출합니다.
#     - 전체 비디오 단일 태스크로 돌려 전역 최적 15분을 찾고 싶다면,
#       segments=1로 설정하세요(PERSON_SEGMENTS=1).
#     """
#     cap = cv2.VideoCapture(video_path)
#     total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
#     fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
#     cap.release()

#     if total_frames <= 0:
#         return []

#     seg = max(1, int(segments))
#     seg_size = (total_frames + seg - 1) // seg

#     tasks = []
#     for k in range(seg):
#         start_idx = k * seg_size
#         end_idx = min((k + 1) * seg_size, total_frames)
#         if start_idx >= end_idx:
#             break
#         args = (start_idx, end_idx, device_id, video_path, fps, max(1, sampling_rate), output_root, sub_category)
#         tasks.append(args)
#     return tasks


# # =============================
# # 메인
# # =============================
# if __name__ == "__main__":
#     t0 = time.time()

#     if INPUT_ROOT is None or OUTPUT_ROOT is None:
#         raise ValueError("INPUT_ROOT / OUTPUT_ROOT 환경변수를 설정하세요.")

#     root_category = Path(INPUT_ROOT).parts[-2] if len(Path(INPUT_ROOT).parts) >= 2 else Path(INPUT_ROOT).name

#     # 입력 폴더 스캔
#     all_video_entries = []  # (sub_category, video_file, video_path)
#     for category in os.listdir(INPUT_ROOT):
#         if category in EXCLUDED_CATEGORIES:
#             print(f"❌ 제외된 폴더: {category}")
#             continue
#         category_path = os.path.join(INPUT_ROOT, category)
#         if not os.path.isdir(category_path):
#             continue
#         video_files = [f for f in os.listdir(category_path) if f.lower().endswith((".mp4", ".avi", ".mov", ".mkv"))]
#         for video_file in video_files:
#             video_path = os.path.join(category_path, video_file)
#             all_video_entries.append((category, video_file, video_path))

#     if not all_video_entries:
#         print("처리할 비디오가 없습니다.")
#         raise SystemExit(0)

#     print(f"발견한 영상 수: {len(all_video_entries)}")

#     # 멀티프로세싱 시작 방식
#     try:
#         if get_start_method(allow_none=True) != 'spawn':
#             set_start_method('spawn', force=True)
#     except RuntimeError:
#         pass

#     category_frame_counter = defaultdict(int)
#     total_extracted_frames = 0

#     # =====================
#     # 사람 감지 + 고급 스케줄러
#     # =====================
#     if PERSON_ONLY:
#         if not _YOLO_AVAILABLE:
#             raise RuntimeError(
#                 "사람 감지 모드를 선택했지만 YOLO/torch 를 불러올 수 없습니다. "
#                 "`pip install ultralytics torch` 설치 후 다시 시도하세요."
#             )

#         gpu_ids = parse_gpu_ids()  # 예: [0,1]
#         valid_gpu_ids = [g for g in gpu_ids if g >= 0]
#         gpu_count = len(valid_gpu_ids)
#         if gpu_count == 0:
#             print("⚠️ GPU를 사용하지 않습니다. CPU로 진행합니다. (GPU_IDS가 비어있거나 GPU 미탐지)")
#             valid_gpu_ids = [-1]
#             gpu_count = 1

#         print(f"🟢 사용 GPU: {valid_gpu_ids} | GPU당 동시 처리 제한: {WORKERS_PER_GPU}")

#         # 1) 비디오 라운드로빈 배정 + 세그먼트 태스크 생성
#         per_gpu_queues = {gid: deque() for gid in valid_gpu_ids}
#         expected_segments = {}
#         done_segments = defaultdict(int)
#         saved_frames_per_video = defaultdict(int)

#         for vidx, (sub_category, video_file, video_path) in enumerate(all_video_entries):
#             if is_processed(PROCESSED_LOG, root_category, sub_category, video_file):
#                 print(f"✅ 이미 처리됨: {video_file}")
#                 continue

#             assigned_gpu = valid_gpu_ids[vidx % gpu_count]
#             print(f"🎬 준비: {video_file} → GPU {assigned_gpu}, 세그먼트 {PERSON_SEGMENTS}")

#             seg_tasks = build_segment_tasks_for_video(
#                 video_path=video_path,
#                 output_root=OUTPUT_ROOT,
#                 sub_category=sub_category,
#                 sampling_rate=PERSON_SAMPLING_RATE,
#                 device_id=assigned_gpu,
#                 segments=PERSON_SEGMENTS,
#             )
#             if not seg_tasks:
#                 print(f"⚠️ {video_file}: 유효한 프레임이 없어 스킵")
#                 continue

#             video_key = (sub_category, video_file)
#             expected_segments[video_key] = len(seg_tasks)

#             for t in seg_tasks:
#                 per_gpu_queues[assigned_gpu].append((t, video_key, assigned_gpu))

#         total_tasks = sum(len(q) for q in per_gpu_queues.values())
#         if total_tasks == 0:
#             print("처리할 세그먼트 태스크가 없습니다.")
#             raise SystemExit(0)

#         gpu_semaphores = {gid: threading.BoundedSemaphore(WORKERS_PER_GPU) for gid in valid_gpu_ids}
#         pool_size = max(1, gpu_count * max(1, WORKERS_PER_GPU))
#         print(f"🚀 전역 병렬 처리 시작 — 풀 크기: {pool_size} (GPU:{valid_gpu_ids}, GPU당 워커:{WORKERS_PER_GPU})")

#         cb_lock = threading.Lock()
#         manager = Manager()
#         remaining = manager.Value('i', total_tasks)

#         def _on_done(result):
#             vkey, gid, saved = result
#             with cb_lock:
#                 done_segments[vkey] += 1
#                 saved_frames_per_video[vkey] += int(saved)
#                 remaining.value -= 1

#                 gpu_semaphores[gid].release()

#                 if done_segments[vkey] == expected_segments[vkey]:
#                     sub_category, video_file = vkey
#                     mark_as_processed(PROCESSED_LOG, root_category, sub_category, video_file)
#                     print(f"✅ 완료: {video_file} | 저장 프레임: {saved_frames_per_video[vkey]}")

#                 _schedule_more()

#         def _schedule_more():
#             for gid in list(per_gpu_queues.keys()):
#                 while per_gpu_queues[gid] and gpu_semaphores[gid].acquire(blocking=False):
#                     task_args, vkey, g = per_gpu_queues[gid].popleft()
#                     pool.apply_async(runner_top, args=(task_args, vkey, g), callback=_on_done)

#         with Pool(processes=pool_size) as pool:
#             _schedule_more()
#             while True:
#                 with cb_lock:
#                     if remaining.value <= 0:
#                         break
#                 time.sleep(0.1)

#         for (sub_category, video_file), saved_cnt in saved_frames_per_video.items():
#             category_frame_counter[sub_category] += saved_cnt
#             total_extracted_frames += saved_cnt

#     else:
#         # 기존 균일간격 모드 유지
#         def _probe(video_path):
#             cap = cv2.VideoCapture(video_path)
#             fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
#             total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
#             duration_sec = int(total_frames / fps) if fps > 0 else 0
#             cap.release()
#             return duration_sec, fps

#         video_tasks = []
#         save_info = []

#         for (sub_category, video_file, video_path) in all_video_entries:
#             if is_processed(PROCESSED_LOG, root_category, sub_category, video_file):
#                 print(f"✅ 이미 처리됨: {video_file}")
#                 continue

#             duration_sec, fps = _probe(video_path)
#             num_frames_to_extract, interval_frames = get_frames_to_extract(duration_sec, fps, frames_per_sec=UNIFORM_FRAMES_PER_SEC)

#             if num_frames_to_extract > 0:
#                 print(f"📹 {video_file} (길이: {duration_sec}s, FPS: {fps:.2f}) → 추출 {num_frames_to_extract}장 (간격 {interval_frames}프레임)")
#                 video_name = os.path.splitext(video_file)[0]
#                 save_dir = os.path.join(OUTPUT_ROOT, sub_category, video_name)
#                 video_tasks.append((video_path, save_dir, num_frames_to_extract, interval_frames))
#                 save_info.append((sub_category, video_file))
#             else:
#                 print(f"⚠️ {video_file}: 추출 프레임 없음. 스킵")

#         print(f"총 처리할 영상 수: {len(video_tasks)}")

#         def process_video_task(task):
#             video_path, save_dir, num_frames, interval_frames = task
#             return save_dir, extract_frames_uniform(video_path, save_dir, num_frames, interval_frames)

#         num_workers = max(1, os.cpu_count() or 4)
#         with Pool(processes=num_workers) as pool:
#             results = pool.map(process_video_task, video_tasks)

#         for idx, (save_dir, frame_count) in enumerate(results):
#             sub_category, video_file = save_info[idx]
#             if frame_count > 0:
#                 category_frame_counter[sub_category] += frame_count
#                 total_extracted_frames += frame_count
#             mark_as_processed(PROCESSED_LOG, root_category, sub_category, video_file)

#     # =====================
#     # 요약 로그 저장
#     # =====================
#     now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     file_exists = os.path.exists(SUMMARY_LOG)

#     print("\n추출 요약")
#     with open(SUMMARY_LOG, "a", newline="") as f:
#         writer = csv.writer(f)
#         if not file_exists:
#             writer.writerow(["date", "root_category", "sub_category", "image_count"])
#         for cat, count in category_frame_counter.items():
#             print(f" {cat}: {count}장")
#             writer.writerow([now, root_category, cat, count])
#         print(f"전체 이미지 수: {total_extracted_frames}장")
#         writer.writerow([now, root_category, "TOTAL", total_extracted_frames])

#     print("✅ 모든 영상 처리 및 로그 작성 완료")
#     print(f" 총 소요 시간: {time.time() - t0:.1f}초")


#============================================ 
# one segment
#============================================

import os
import cv2
import csv
import time
from pathlib import Path
from collections import defaultdict, deque
from datetime import datetime
from multiprocessing import Pool, get_start_method, set_start_method, Manager
import threading

from tqdm import tqdm
from dotenv import load_dotenv

"""
고급 스케줄러 (세마포어 기반, Manager.Value 적용)
────────────────────────────────────────────────────────────────────
요구사항 반영:
- YOLO로 사람을 찾아, '사람이 가장 많이 탐지된 연속 15분' 구간만 골라서
  해당 구간에서 '2초당 1프레임(0.5fps)'로 이미지 추출
- 기존 멀티-GPU 스케줄러/세마포어/로그 구조는 유지

구현 개요:
- detect_and_extract_worker:
  1) 1차 패스(샘플링 탐지): sampling_rate 간격으로 프레임을 훑으며 초 단위로 person 카운트를 누적
  2) 15분(900초) 슬라이딩 윈도우로 최다 탐지 구간 계산
  3) 2차 패스: 해당 구간에서만 2초당 1프레임으로 저장
"""

# =============================
# 환경 변수 로드 (.env는 상위 폴더에 있다고 가정)
# =============================
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# 공통 경로/설정
INPUT_ROOT = os.getenv("INPUT_ROOT")
OUTPUT_ROOT = os.getenv("OUTPUT_ROOT")
ASSIGN_LOG_DIR = os.getenv("ASSIGN_LOG_DIR", ".")
os.makedirs(ASSIGN_LOG_DIR, exist_ok=True)
PROCESSED_LOG = os.path.join(ASSIGN_LOG_DIR, "processed_videos.csv")
SUMMARY_LOG = os.path.join(ASSIGN_LOG_DIR, "frame_summary_log.csv")

# 모드 전환
PERSON_ONLY = os.getenv("PERSON_ONLY", "0") in ("1", "true", "True")
UNIFORM_FRAMES_PER_SEC = float(os.getenv("UNIFORM_FRAMES_PER_SEC", "3"))

# 사람 감지(세그먼트/병렬) 설정
PERSON_SEGMENTS = int(os.getenv("PERSON_SEGMENTS", "4"))             # [주석처리 대상과 연관] 비디오를 N등분
WORKERS_PER_GPU = int(os.getenv("WORKERS_PER_GPU", str(PERSON_SEGMENTS)))  # GPU당 동시 프로세스 수 (기본값: 세그먼트 수)
PERSON_SAMPLING_RATE = int(os.getenv("PERSON_SAMPLING_RATE", "5"))    # 1차 패스: n프레임마다 탐지
YOLO_WEIGHTS = os.getenv("YOLO_WEIGHTS", "yolov8n.pt")
PERSON_CONF = float(os.getenv("PERSON_CONF", "0.25"))

# 멀티-GPU: 쉼표로 구분된 GPU ID 리스트 (예: "0,1")
GPU_IDS_ENV = os.getenv("GPU_IDS", "0,1")   # 기본값: 2개 GPU 있다고 가정

# 제외 카테고리(콤마 구분)
EXCLUDED_CATEGORIES = set(
    x.strip() for x in os.getenv("EXCLUDED_CATEGORIES", "").split(",") if x.strip()
)

# [신규] 15분 타겟 구간(초)
TARGET_WINDOW_SEC = int(os.getenv("TARGET_WINDOW_SEC", str(15 * 60)))
# [고정] 2초당 1프레임 저장
EXTRACT_EVERY_SEC = 2.0

# =============================
# YOLO / torch 임포트 (사람 감지 모드에서 사용)
# =============================
_YOLO_AVAILABLE = True
try:
    import torch
    from ultralytics import YOLO
except Exception as e:
    _YOLO_AVAILABLE = False
    _YOLO_IMPORT_ERROR = e


# =============================
# 유틸 함수들 (균일간격 모드 유지)
# =============================
def get_frames_to_extract(duration_sec, fps, frames_per_sec=3):
    if fps <= 0 or duration_sec <= 0:
        return 0, 0
    num_frames = int(duration_sec * frames_per_sec)
    interval = int(fps / frames_per_sec)
    if interval < 1:
        interval = 1
        num_frames = int(duration_sec * fps)
    return num_frames, interval


def extract_frames_uniform(video_path, save_dir, num_frames, interval_frames):
    cap = cv2.VideoCapture(video_path)
    total_video_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    if num_frames == 0 or total_video_frames < interval_frames:
        print(f"⚠️ {os.path.basename(video_path)}: 추출 프레임 없음/간격 과대. 스킵")
        cap.release()
        return 0

    base_name = os.path.splitext(os.path.basename(video_path))[0]
    os.makedirs(save_dir, exist_ok=True)

    count = 0
    for i in range(num_frames):
        frame_idx = i * interval_frames
        if frame_idx >= total_video_frames:
            break
        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
        ret, frame = cap.read()
        if ret:
            save_path = os.path.join(save_dir, f"{base_name}_frame{i:06d}.jpg")
            cv2.imwrite(save_path, frame)
            count += 1
    cap.release()
    return count


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


# =============================
# [핵심] 사람 감지용 워커 (업데이트 버전)
# =============================
def detect_and_extract_worker(args):
    """
    args: (start_idx, end_idx, device_id, video_path, fps, sampling_rate, output_root, category)
    동작:
      1) [start_idx, end_idx) 범위를 1차 패스로 훑으며 PERSON 존재 여부를 '초 단위' 카운트에 누적
      2) 해당 범위 내에서 '연속 15분(900초)' 윈도우 중 합계가 최대인 구간 찾기
      3) 그 구간에서만 2초당 1프레임으로 이미지 저장
    반환: (saved_count, save_dir)
    """
    start_idx, end_idx, device_id, video_path, fps, sampling_rate, output_root, category = args

    if not _YOLO_AVAILABLE:
        raise RuntimeError(
            f"ultralytics/torch 임포트 실패: {_YOLO_IMPORT_ERROR}\n"
            f"사람 감지 모드를 사용하려면 `pip install ultralytics torch`를 먼저 설치하세요."
        )

    # 디바이스 설정
    use_cuda = False
    if 'torch' in globals():
        use_cuda = torch.cuda.is_available() and device_id is not None and device_id >= 0
        if use_cuda:
            torch.cuda.set_device(device_id)

    # 모델 로드 (각 프로세스 별 1회)
    yolo = YOLO(YOLO_WEIGHTS)

    # 비디오 준비
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = fps or cap.get(cv2.CAP_PROP_FPS) or 0.0

    # 출력 폴더
    video_filename = os.path.splitext(os.path.basename(video_path))[0]
    save_dir = os.path.join(output_root, category, video_filename)
    os.makedirs(save_dir, exist_ok=True)

    # 경계 보정
    start_idx = max(0, start_idx)
    end_idx = min(end_idx, total_frames) if end_idx > 0 else total_frames
    if fps <= 0 or start_idx >= end_idx:
        cap.release()
        return 0, save_dir

    # ---------------------------
    # 1) 1차 패스: 샘플링 탐지 → 초 단위 카운트
    # ---------------------------
    person_count_by_sec = defaultdict(int)

    # 1차 패스 진행 바
    with tqdm(total=(end_idx - start_idx), desc=f"[PASS1] {video_filename} [{start_idx}-{end_idx}) GPU:{device_id}") as pbar:
        frame_idx = start_idx
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)

        while frame_idx < end_idx:
            ret, frame = cap.read()
            if not ret:
                break

            # 샘플링 스킵 (속도)
            if sampling_rate > 1 and (frame_idx % sampling_rate != 0):
                frame_idx += 1
                pbar.update(1)
                continue

            # YOLO 추론
            results = yolo(frame, verbose=False, conf=PERSON_CONF)[0]
            has_person = False
            if results.boxes is not None and len(results.boxes.cls) > 0:
                for cls_idx in results.boxes.cls.tolist():
                    name = results.names.get(int(cls_idx), "")
                    if name == "person":
                        has_person = True
                        break

            if has_person:
                sec = int(frame_idx / fps)
                person_count_by_sec[sec] += 1

            frame_idx += 1
            pbar.update(1)

    # ---------------------------
    # 2) 최적 15분(900초) 연속 구간 계산 (슬라이딩 윈도우)
    # ---------------------------
    total_secs = int((end_idx - start_idx) / fps) + 1
    if total_secs <= 0:
        cap.release()
        return 0, save_dir

    window_len = min(TARGET_WINDOW_SEC, total_secs)  # 영상이 15분보다 짧으면 영상 길이로
    # 초별 카운트를 배열로 변환 (start_sec=0 기준)
    sec_counts = [0] * total_secs
    for s, c in person_count_by_sec.items():
        local_s = s - int(start_idx / fps)
        if 0 <= local_s < total_secs:
            sec_counts[local_s] += c

    # 슬라이딩 윈도우로 최대 합 찾기 (prefix sum)
    prefix = [0]
    for v in sec_counts:
        prefix.append(prefix[-1] + v)

    best_sum = -1
    best_start_sec_local = 0
    for i in range(0, total_secs - window_len + 1):
        j = i + window_len
        ssum = prefix[j] - prefix[i]
        if ssum > best_sum:
            best_sum = ssum
            best_start_sec_local = i

    # 로컬 구간(시작초) → 실제 프레임 인덱스
    best_start_sec_global = int(start_idx / fps) + best_start_sec_local
    best_end_sec_global = best_start_sec_global + window_len

    # 프레임 범위 (닫힌-열린)
    best_start_frame = int(best_start_sec_global * fps)
    best_end_frame = int(min(end_idx, best_end_sec_global * fps))

    # 예외: 1차 패스에서 단 한 번도 사람이 안 잡힌 경우 → 규칙 그대로 15분 구간 사용
    if best_sum <= 0:
        best_start_frame = start_idx
        best_end_frame = min(end_idx, start_idx + int(window_len * fps))

    # ---------------------------
    # 3) 2차 패스: 해당 구간에서만 2초당 1프레임 저장
    # ---------------------------
    interval_frames = max(1, int(round(fps * EXTRACT_EVERY_SEC)))
    saved = 0

    # 2차 패스 진행 바
    with tqdm(total=max(0, best_end_frame - best_start_frame),
              desc=f"[PASS2] {video_filename} [{best_start_frame}-{best_end_frame}) GPU:{device_id}") as pbar:

        for fidx in range(best_start_frame, best_end_frame, interval_frames):
            cap.set(cv2.CAP_PROP_POS_FRAMES, fidx)
            ret, frame = cap.read()
            if not ret:
                break

            out_name = f"{video_filename}_{fidx:06d}.jpg"
            out_path = os.path.join(save_dir, out_name)
            cv2.imwrite(out_path, frame)
            saved += 1

            # 진행바는 실제 읽은 프레임 수 기준으로 대략 업데이트
            pbar.update(interval_frames)

    cap.release()
    return saved, save_dir


# =============================
# [중요] 멀티프로세싱용 Top-level 러너 (Picklable)
# =============================
def runner_top(task_args, video_key, gpu_id):
    saved, _ = detect_and_extract_worker(task_args)
    return (video_key, gpu_id, int(saved))


# =============================
# GPU 유틸: GPU 리스트 파싱
# =============================
def parse_gpu_ids() -> list:
    env = GPU_IDS_ENV.strip()
    ids = []
    if env:
        for tok in env.split(','):
            tok = tok.strip()
            if tok and tok.lstrip('-').isdigit():
                ids.append(int(tok))

    if ids:
        return ids

    if _YOLO_AVAILABLE and 'torch' in globals() and torch.cuda.is_available():
        n = torch.cuda.device_count()
        if n > 0:
            return list(range(n))

    return [-1]


# =============================
# 세그먼트 태스크 빌더 (비디오→GPU 고정, 내부 세그먼트 병렬)
# =============================
# [주석처리] — 세그먼트 분할 제거를 위해 함수 전체를 막습니다.
# def build_segment_tasks_for_video(video_path: str, output_root: str, sub_category: str,
#                                   sampling_rate: int, device_id: int, segments: int):
#     """
#     ※ 주의
#     - 15분 최적 구간은 일반적으로 '전체 비디오'를 대상으로 하나,
#       여기서는 기존 스케줄러 구조(세그먼트 병렬)를 최대한 유지하기 위해
#       '각 세그먼트 내부에서' 최적 15분을 찾고 그 세그먼트에서만 추출합니다.
#     - 전체 비디오 단일 태스크로 돌려 전역 최적 15분을 찾고 싶다면,
#       segments=1로 설정하세요(PERSON_SEGMENTS=1).
#     """
#     cap = cv2.VideoCapture(video_path)
#     total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
#     fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
#     cap.release()
#
#     if total_frames <= 0:
#         return []
#
#     seg = max(1, int(segments))
#     seg_size = (total_frames + seg - 1) // seg
#
#     tasks = []
#     for k in range(seg):
#         start_idx = k * seg_size
#         end_idx = min((k + 1) * seg_size, total_frames)
#         if start_idx >= end_idx:
#             break
#         args = (start_idx, end_idx, device_id, video_path, fps, max(1, sampling_rate), output_root, sub_category)
#         tasks.append(args)
#     return tasks


# =============================
# 메인
# =============================
if __name__ == "__main__":
    t0 = time.time()

    if INPUT_ROOT is None or OUTPUT_ROOT is None:
        raise ValueError("INPUT_ROOT / OUTPUT_ROOT 환경변수를 설정하세요.")

    root_category = Path(INPUT_ROOT).parts[-2] if len(Path(INPUT_ROOT).parts) >= 2 else Path(INPUT_ROOT).name

    # 입력 폴더 스캔
    all_video_entries = []  # (sub_category, video_file, video_path)
    for category in os.listdir(INPUT_ROOT):
        if category in EXCLUDED_CATEGORIES:
            print(f"❌ 제외된 폴더: {category}")
            continue
        category_path = os.path.join(INPUT_ROOT, category)
        if not os.path.isdir(category_path):
            continue
        video_files = [f for f in os.listdir(category_path) if f.lower().endswith((".mp4", ".avi", ".mov", ".mkv"))]
        for video_file in video_files:
            video_path = os.path.join(category_path, video_file)
            all_video_entries.append((category, video_file, video_path))

    if not all_video_entries:
        print("처리할 비디오가 없습니다.")
        raise SystemExit(0)

    print(f"발견한 영상 수: {len(all_video_entries)}")

    # 멀티프로세싱 시작 방식
    try:
        if get_start_method(allow_none=True) != 'spawn':
            set_start_method('spawn', force=True)
    except RuntimeError:
        pass

    category_frame_counter = defaultdict(int)
    total_extracted_frames = 0

    # =====================
    # 사람 감지 + 고급 스케줄러
    # =====================
    if PERSON_ONLY:
        if not _YOLO_AVAILABLE:
            raise RuntimeError(
                "사람 감지 모드를 선택했지만 YOLO/torch 를 불러올 수 없습니다. "
                "`pip install ultralytics torch` 설치 후 다시 시도하세요."
            )

        gpu_ids = parse_gpu_ids()  # 예: [0,1]
        valid_gpu_ids = [g for g in gpu_ids if g >= 0]
        gpu_count = len(valid_gpu_ids)
        if gpu_count == 0:
            print("⚠️ GPU를 사용하지 않습니다. CPU로 진행합니다. (GPU_IDS가 비어있거나 GPU 미탐지)")
            valid_gpu_ids = [-1]
            gpu_count = 1

        print(f"🟢 사용 GPU: {valid_gpu_ids} | GPU당 동시 처리 제한: {WORKERS_PER_GPU}")

        # 1) 비디오 라운드로빈 배정 + (세그먼트 대신) '단일 태스크' 생성
        per_gpu_queues = {gid: deque() for gid in valid_gpu_ids}
        expected_segments = {}
        done_segments = defaultdict(int)
        saved_frames_per_video = defaultdict(int)

        for vidx, (sub_category, video_file, video_path) in enumerate(all_video_entries):
            if is_processed(PROCESSED_LOG, root_category, sub_category, video_file):
                print(f"✅ 이미 처리됨: {video_file}")
                continue

            assigned_gpu = valid_gpu_ids[vidx % gpu_count]

            # [변경] 세그먼트 문구 제거 + 전역 15분 1구간 처리 안내
            print(f"🎬 준비: {video_file} → GPU {assigned_gpu} (전역 15분 1구간 추출)")

            # 전체 영상 길이/ fps 조회 후 '한 건'의 태스크만 생성
            cap = cv2.VideoCapture(video_path)
            fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            cap.release()

            if total_frames <= 0:
                print(f"⚠️ {video_file}: 유효한 프레임이 없어 스킵")
                continue

            task_args = (
                0,                              # start_idx (영상 처음)
                total_frames,                   # end_idx   (영상 끝)
                assigned_gpu,                   # device_id
                video_path,                     # video_path
                fps,                            # fps
                max(1, PERSON_SAMPLING_RATE),   # sampling_rate
                OUTPUT_ROOT,                    # output_root
                sub_category                    # category(=폴더명)
            )

            video_key = (sub_category, video_file)
            expected_segments[video_key] = 1  # [변경] 항상 1개로 고정
            per_gpu_queues[assigned_gpu].append((task_args, video_key, assigned_gpu))

        total_tasks = sum(len(q) for q in per_gpu_queues.values())
        if total_tasks == 0:
            print("처리할 태스크가 없습니다.")
            raise SystemExit(0)

        gpu_semaphores = {gid: threading.BoundedSemaphore(WORKERS_PER_GPU) for gid in valid_gpu_ids}
        pool_size = max(1, gpu_count * max(1, WORKERS_PER_GPU))
        print(f"🚀 전역 병렬 처리 시작 — 풀 크기: {pool_size} (GPU:{valid_gpu_ids}, GPU당 워커:{WORKERS_PER_GPU})")

        cb_lock = threading.Lock()
        manager = Manager()
        remaining = manager.Value('i', total_tasks)

        def _on_done(result):
            vkey, gid, saved = result
            with cb_lock:
                done_segments[vkey] += 1
                saved_frames_per_video[vkey] += int(saved)
                remaining.value -= 1

                gpu_semaphores[gid].release()

                if done_segments[vkey] == expected_segments[vkey]:
                    sub_category, video_file = vkey
                    mark_as_processed(PROCESSED_LOG, root_category, sub_category, video_file)
                    print(f"✅ 완료: {video_file} | 저장 프레임: {saved_frames_per_video[vkey]}")

                _schedule_more()

        def _schedule_more():
            for gid in list(per_gpu_queues.keys()):
                while per_gpu_queues[gid] and gpu_semaphores[gid].acquire(blocking=False):
                    task_args, vkey, g = per_gpu_queues[gid].popleft()
                    pool.apply_async(runner_top, args=(task_args, vkey, g), callback=_on_done)

        with Pool(processes=pool_size) as pool:
            _schedule_more()
            while True:
                with cb_lock:
                    if remaining.value <= 0:
                        break
                time.sleep(0.1)

        for (sub_category, video_file), saved_cnt in saved_frames_per_video.items():
            category_frame_counter[sub_category] += saved_cnt
            total_extracted_frames += saved_cnt

    else:
        # 기존 균일간격 모드 유지
        def _probe(video_path):
            cap = cv2.VideoCapture(video_path)
            fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            duration_sec = int(total_frames / fps) if fps > 0 else 0
            cap.release()
            return duration_sec, fps

        video_tasks = []
        save_info = []

        for (sub_category, video_file, video_path) in all_video_entries:
            if is_processed(PROCESSED_LOG, root_category, sub_category, video_file):
                print(f"✅ 이미 처리됨: {video_file}")
                continue

            duration_sec, fps = _probe(video_path)
            num_frames_to_extract, interval_frames = get_frames_to_extract(duration_sec, fps, frames_per_sec=UNIFORM_FRAMES_PER_SEC)

            if num_frames_to_extract > 0:
                print(f"📹 {video_file} (길이: {duration_sec}s, FPS: {fps:.2f}) → 추출 {num_frames_to_extract}장 (간격 {interval_frames}프레임)")
                video_name = os.path.splitext(video_file)[0]
                save_dir = os.path.join(OUTPUT_ROOT, sub_category, video_name)
                video_tasks.append((video_path, save_dir, num_frames_to_extract, interval_frames))
                save_info.append((sub_category, video_file))
            else:
                print(f"⚠️ {video_file}: 추출 프레임 없음. 스킵")

        print(f"총 처리할 영상 수: {len(video_tasks)}")

        def process_video_task(task):
            video_path, save_dir, num_frames, interval_frames = task
            return save_dir, extract_frames_uniform(video_path, save_dir, num_frames, interval_frames)

        num_workers = max(1, os.cpu_count() or 4)
        with Pool(processes=num_workers) as pool:
            results = pool.map(process_video_task, video_tasks)

        for idx, (save_dir, frame_count) in enumerate(results):
            sub_category, video_file = save_info[idx]
            if frame_count > 0:
                category_frame_counter[sub_category] += frame_count
                total_extracted_frames += frame_count
            mark_as_processed(PROCESSED_LOG, root_category, sub_category, video_file)

    # =====================
    # 요약 로그 저장
    # =====================
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_exists = os.path.exists(SUMMARY_LOG)

    print("\n추출 요약")
    with open(SUMMARY_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date", "root_category", "sub_category", "image_count"])
        for cat, count in category_frame_counter.items():
            print(f" {cat}: {count}장")
            writer.writerow([now, root_category, cat, count])
        print(f"전체 이미지 수: {total_extracted_frames}장")
        writer.writerow([now, root_category, "TOTAL", total_extracted_frames])

    print("✅ 모든 영상 처리 및 로그 작성 완료")
    print(f" 총 소요 시간: {time.time() - t0:.1f}초")
