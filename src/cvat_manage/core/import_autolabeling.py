import os, json, zipfile, argparse, torch, time
from pathlib import Path
from ultralytics import YOLO
from PIL import Image
from dotenv import load_dotenv
from datetime import datetime
import requests, colorsys
from math import ceil
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import csv


load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
CVAT_URL = os.getenv("CVAT_URL_2")
TOKEN = os.getenv("TOKEN_2")
ORGANIZATION = os.getenv("ORGANIZATION")
ASSIGN_LOG_PATH = Path(f"./logs/assignments_log.csv")
ASSIGN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def hsv_to_hex(h, s, v):
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return '#{:02X}{:02X}{:02X}'.format(int(r * 255), int(g * 255), int(b * 255))

# === CVAT API ===
def get_or_create_organization(name):
    headers = {"Authorization": f"Token {TOKEN}", "Content-Type": "application/json"}
    res = requests.get(f"{CVAT_URL}/api/organizations", headers=headers)
    res.raise_for_status()
    for org in res.json()["results"]:
        if org["slug"] == name or org["name"] == name:
            return org["id"], org["slug"]
    slug = name.lower().replace(" ", "-")
    res = requests.post(f"{CVAT_URL}/api/organizations", headers=headers, json={"name": name, "slug": slug})
    res.raise_for_status()
    return res.json()["id"], slug

def build_headers(org_slug):
    return {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json",
        "X-Organization": org_slug
    }

def create_project(name, labels, headers):
    label_defs = [{"name": label, "color": hsv_to_hex(i/len(labels), 0.7, 0.95)} for i, label in enumerate(labels)]
    res = requests.post(f"{CVAT_URL}/api/projects", headers=headers, json={"name": name, "labels": label_defs})
    res.raise_for_status()
    return res.json()["id"]

def create_task_with_zip(name, project_id, zip_path, headers):
    res = requests.post(f"{CVAT_URL}/api/tasks", headers=headers, json={
        "name": name, "project_id": project_id, "image_quality": 100, "segment_size": 100
    })
    res.raise_for_status()
    task_id = res.json()["id"]

    upload_headers = headers.copy()
    upload_headers.pop("Content-Type", None)
    with open(zip_path, "rb") as zip_file:
        files = {"client_files[0]": (os.path.basename(zip_path), zip_file, "application/zip")}
        data = {
            "image_quality": 100,
            "use_zip_chunks": "false",
            "use_cache": "false",
            "sorting_method": "lexicographical",
            "upload_format": "zip"
        }
        res = requests.post(f"{CVAT_URL}/api/tasks/{task_id}/data", headers=upload_headers, files=files, data=data)
        res.raise_for_status()
    return task_id

def wait_until_task_ready(task_id, headers, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=headers)
        if res.status_code != 200:
            print(f"❌ Task 상태 확인 실패: {res.status_code}")
            break
        task_info = res.json()
        if task_info.get("size", 0) > 0:
            return True
        time.sleep(2)
    return False

def upload_annotations(task_id, json_path, headers):
    print(f"⏳ 어노테이션 업로드 시작: Task ID {task_id}")
    upload_headers = headers.copy()
    upload_headers.pop("Content-Type", None)
    with open(json_path, "rb") as jf:
        files = {"annotation_file": (json_path.name, jf, "application/json")}
        url = f"{CVAT_URL}/api/tasks/{task_id}/annotations"
        params = {
            "org": ORGANIZATION,
            "format": "COCO 1.0",
            "filename": json_path.name,
            "conv_mask_to_poly": "true"
        }
        # print(f"[DEBUG] 요청 URL: {url}")
        # print(f"[DEBUG] 요청 Params: {params}")
        # print(f"[DEBUG] 업로드 파일명: {json_path.name}")

        res = requests.put(url, headers=upload_headers, files=files, params=params)
        # print(f"[DEBUG] 요청 URL (res.url): {res.url}")

    if res.status_code in [200, 202]:
        print(f"✅ 어노테이션 업로드 성공: Task {task_id}")
    else:
        print(f"❌ 어노테이션 업로드 실패: {res.status_code}")
        print(res.text)


def get_jobs(task_id, headers):
    res = requests.get(f"{CVAT_URL}/api/jobs?task_id={task_id}", headers=headers)
    res.raise_for_status()
    return res.json()["results"]

def get_user_id(username, headers):
    res = requests.get(f"{CVAT_URL}/api/users", headers=headers)
    res.raise_for_status()
    for user in res.json()["results"]:
        if user["username"] == username:
            return user["id"]
    return None

def assign_jobs_to_one_user(jobs, headers, assignee_name):
    user_id = get_user_id(assignee_name, headers)
    if not user_id:
        print(f"❌ 사용자 '{assignee_name}'를 찾을 수 없습니다.")
        return
    for job in jobs:
        if job.get("assignee"): continue
        try:
            res = requests.patch(f"{CVAT_URL}/api/jobs/{job['id']}", headers=headers, json={"assignee": user_id})
            res.raise_for_status()
            print(f"✅ Job {job['id']} → '{assignee_name}' 할당 완료")
        except requests.HTTPError as e:
            print(f"⚠️ 할당 실패: {e.response.status_code} - {e.response.text}")

def get_user_display_name(username):
    return os.getenv(f"USERMAP_{username}", username)

def log_assignment(task_name, task_id, assignee_name, num_jobs):

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    display_name = get_user_display_name(assignee_name)
    log_entry = [now, task_name, task_id, display_name, num_jobs]
    log_columns = ["timestamp", "task_name", "task_id", "assignee", "num_jobs"]

    if not ASSIGN_LOG_PATH.exists():
        with open(ASSIGN_LOG_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(log_columns)
            writer.writerow(log_entry)
        return

    # 기존 로그 읽기
    with open(ASSIGN_LOG_PATH, mode="r", newline="", encoding="utf-8") as f:
        reader = list(csv.reader(f))
        header = reader[0]
        rows = reader[1:]
    
    # timestamp 기준 내림차순정렬
    rows.sort(key=lambda r: datetime.strptime(r[0], "%d/%m/%Y %H:%M"), reverse=True)

    # 다시 저장
    with open(ASSIGN_LOG_PATH, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)




def run_yolo_on_image(model, img_path, image_id, annotation_id_start):
    start = time.time()
    img = Image.open(img_path)
    width, height = img.size
    image_entry = {
        "id": image_id,
        "file_name": img_path.name,
        "width": width,
        "height": height
    }
    results = model(img_path)
    annotations = []
    aid = annotation_id_start
    for r in results:
        for i, box in enumerate(r.boxes):
            cls_id = int(r.boxes.cls[i].item())
            cls_name = r.names[cls_id]
            if cls_name != "person":
                continue
            x1, y1, x2, y2 = map(float, box.xyxy[0])
            bbox = [x1, y1, x2 - x1, y2 - y1]
            area = bbox[2] * bbox[3]
            annotations.append({
                "id": aid,
                "image_id": image_id,
                "category_id": 1,
                "bbox": bbox,
                "area": area,
                "iscrowd": 0
            })
            aid += 1
    elapsed = time.time() - start
    print(f"🕒 {img_path.name} 추론 소요시간: {elapsed:.2f}초 (person {len(annotations)}개 감지)")
    return image_entry, annotations, aid

def run_yolo_and_create_json_parallel(images, output_json_path, model0, model1):
    coco = {
        "images": [],
        "annotations": [],
        "categories": [{"id": 1, "name": "person", "supercategory": "object"}]
    }
    def process(i_img_ann):
        i, (img_path, aid) = i_img_ann
        model = model0 if i % 2 == 0 else model1
        return run_yolo_on_image(model, img_path, i + 1, aid)
    start_all = time.time()
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = executor.map(process, enumerate([(img, i * 1000 + 1) for i, img in enumerate(images)]))
    for image_entry, anns, _ in results:
        coco["images"].append(image_entry)
        coco["annotations"].extend(anns)
    with open(output_json_path, "w") as f:
        json.dump(coco, f, indent=2)
    print(f"✅ 전체 YOLO 추론 시간: {time.time() - start_all:.2f}초")

def compress_and_upload_all(image_root_dir: Path, project_id, headers, assignees, batch_size=100):
    model0 = YOLO("yolov8s.pt").to("cuda:0")
    model1 = YOLO("yolov8s.pt").to("cuda:1")
    num_users = len(assignees)
    print(f"✅ GPU 사용 확인: {torch.cuda.get_device_name(0)}, {torch.cuda.get_device_name(1)}")
    for group_dir in image_root_dir.rglob("*"):
        if not group_dir.is_dir(): continue
        image_files = sorted([f for f in group_dir.glob("*") if f.suffix.lower() in [".jpg", ".jpeg", ".png", ".bmp"]])
        if not image_files: continue
        num_batches = ceil(len(image_files) / batch_size)
        for i in range(num_batches):
            batch_files = image_files[i * batch_size : (i + 1) * batch_size]
            zip_filename = f"{group_dir.name}_{i+1:02d}.zip"
            json_filename = f"{group_dir.name}_{i+1:02d}.json"
            zip_path = group_dir / zip_filename
            json_path = group_dir / json_filename
            run_yolo_and_create_json_parallel(batch_files, json_path, model0, model1)
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                for img_path in batch_files:
                    zipf.write(img_path, arcname=img_path.name)
            print(f"[Batch] {zip_filename} created with {len(batch_files)} images")
            task_name = zip_path.stem
            task_id = create_task_with_zip(task_name, project_id, zip_path, headers)
            if wait_until_task_ready(task_id, headers):
                upload_annotations(task_id, json_path, headers)
                print(f"[CVAT] Task {task_name} 등록 및 어노테이션 완료")
            else:
                print(f"[CVAT] Task {task_name} 초기화 실패")
                continue
            jobs = get_jobs(task_id, headers)
            assignee_name = assignees[i % num_users] if num_users > 0 else None
            if assignee_name:
                assign_jobs_to_one_user(jobs, headers, assignee_name)
                log_assignment(task_name, task_id, assignee_name, len(jobs))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YOLO 병렬 추론 + COCO JSON + CVAT 자동 업로드")
    parser.add_argument("--org_name", required=True)
    parser.add_argument("--image_dir", required=True)
    parser.add_argument("--project_name", required=True)
    parser.add_argument("--labels", type=str, nargs="+", required=True)
    parser.add_argument("--assignees", type=str, nargs="+", required=True)
    args = parser.parse_args()
    image_dir = Path(args.image_dir)
    org_id, org_slug = get_or_create_organization(args.org_name)
    headers = build_headers(org_slug)
    project_id = create_project(args.project_name, labels=args.labels, headers=headers)
    compress_and_upload_all(image_dir, project_id, headers, assignees=args.assignees, batch_size=100)
