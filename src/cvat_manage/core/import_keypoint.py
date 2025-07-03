import os
import random
import requests
from pathlib import Path
import csv
from datetime import datetime
import argparse
import time
import json
from typing import List, Dict
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import html
import re



# ===========================
# 환경 변수 및 상수 설정
# ===========================

env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    print(f"⚠️ 환경변수 파일({env_path})을 찾을 수 없습니다.")

CVAT_URL = os.getenv("CVAT_URL")
TOKEN = os.getenv("TOKEN")
today_str = datetime.today().strftime("%Y-%m-%d")
log_dir = Path(os.getenv("ASSIGN_LOG_DIR", "/home/pia/work_p/dfn/omission/logs"))
log_dir.mkdir(parents=True, exist_ok=True)
ASSIGN_LOG_PATH = log_dir / f"assignments_log_keypoint_{today_str}.csv"

# ===========================
# SVG 파싱 및 라벨 정의 생성
# ===========================

def tag_to_explicit_string(tag) -> str:
    tag_name = tag.name
    attributes = ' '.join(f'{key}=&quot;{value}&quot;' for key, value in tag.attrs.items())
    return f'<{tag_name} {attributes}></{tag_name}>'

def extract_label_names_from_svg(svg_file_path: str) -> List[str]:
    """
    SVG 파일 내 <desc> 태그에서 라벨 이름(name) 리스트를 추출합니다.
    """
    svg_input = Path(svg_file_path).read_text(encoding="utf-8")
    soup = BeautifulSoup(svg_input, 'xml')
    desc_tag = soup.find("desc")
    label_def_json = json.loads(desc_tag.text) if desc_tag else {}
    label_items = sorted(label_def_json.items(), key=lambda x: int(x[0]))
    return [item[1]["name"] for item in label_items]

def process_svg_to_simplified_string(svg_file_path: str, sublabels: List[Dict]) -> str:
    """
    <line> 및 <circle> 태그를 명시적 문자열로 추출하고,
    sublabel 순서에 맞춰 <circle>에 data-label-id를 설정합니다.
    """
    try:
        svg_input = Path(svg_file_path).read_text(encoding="utf-8")
        soup = BeautifulSoup(svg_input, 'xml')
        output_parts = []

        for line_tag in soup.find_all('line'):
            for attr in ['stroke', 'stroke-width']:
                line_tag.attrs.pop(attr, None)
            output_parts.append(tag_to_explicit_string(line_tag))

        sublabel_ids = [s['id'] for s in sublabels]
        circle_tags = soup.find_all('circle')

        if len(circle_tags) != len(sublabel_ids):
            raise ValueError(f"<circle> 개수({len(circle_tags)})와 sublabel ID 개수({len(sublabel_ids)})가 일치하지 않습니다.")

        for circle_tag, label_id in zip(circle_tags, sublabel_ids):
            for attr in ['stroke', 'stroke-width', 'fill']:
                circle_tag.attrs.pop(attr, None)
            circle_tag['data-label-id'] = str(label_id)
            output_parts.append(tag_to_explicit_string(circle_tag))

        return "\n".join(output_parts)

    except FileNotFoundError:
        print(f"❌ SVG 파일을 찾을 수 없습니다: {svg_file_path}")
        return ""
    except Exception as e:
        print(f"오류 발생: {e}")
        return ""

def build_label_defs_from_svg_file(svg_path: str, label_names: List[str]) -> List[Dict]:
    # 의미별 공통 색상 정의
    PART_COLOR_MAP = {
        "Eye": "#07eda5",
        "Nose": "#c59e21",
        "Ear": "#7571dd",
        "Shoulder": "#0227c7",
        "Elbow": "#4dc53a",
        "Wrist": "#927bc3",
        "Hip": "#99e1b5",
        "Knee": "#e33bf4",
        "Ankle": "#9f4907",
    }

    def get_common_part_color(label_name:str) -> str:
        for part, color in PART_COLOR_MAP.items():
            if part in label_name:
                return color
        return "#aaaaaa"  # fallback color for unmatched parts
    
    def generate_distinct_colors(n):
        base_colors = [
            "#ff0000", "#00aa00", "#0000ff", "#ffa500", "#800080",
            "#00ced1", "#ff69b4", "#4682b4", "#b8860b", "#20b2aa"
        ]
        if n <= len(base_colors):
            return random.sample(base_colors, n)
        # 부족할 경우, hex 코드 랜덤 생성 추가
        extra_needed = n - len(base_colors)
        extra_colors = [f"#{random.randint(0x100000, 0xFFFFFF):06x}" for _ in range(extra_needed)]
        return random.sample(base_colors, len(base_colors)) + extra_colors
    
    svg_input = Path(svg_path).read_text(encoding='utf-8')
    soup = BeautifulSoup(svg_input, 'xml')
    circle_tags = soup.find_all('circle')
    line_tags = soup.find_all('line')
    num_points = len(circle_tags)

    #라벨 이름 추출
    def extract_label_names_from_svg(svg_file_path: str) -> list[str]:
        desc_tag = soup.find("desc")
        label_def_json = json.loads(desc_tag.text) if desc_tag else {}
        label_items = sorted(label_def_json.items(), key=lambda x: int(x[0]))
        return [item[1]["name"] for item in label_items]
    
    label_names_from_svg = extract_label_names_from_svg(svg_path)

    if len(label_names_from_svg) != num_points:
        raise ValueError("SVG에서 추출한 라벨 이름 수와 <circle> 개수가 일치하지 않습니다.")
    
    # sublabels 정의 (의미 기반 색상 적용)
    sublabels = [
        {
            "name": label_names_from_svg[i],
            "type": "points",
            "color": get_common_part_color(label_names_from_svg[i]),
            "id": i + 1,
            "attributes": []
        }
        for i in range(num_points)
    ]

    # SVG 태그 내 속성 수정
    for circle_tag, sub in zip(circle_tags, sublabels):
        for attr in ["stroke", "stroke-width", "fill"]:
            circle_tag.attrs.pop(attr, None)
        circle_tag['data-label-id'] = str(sub['id'])
        circle_tag['data-label-name'] = sub['name']

    for line_tag in line_tags:
        for attr in ['stroke', 'stroke-width']:
            line_tag.attrs.pop(attr, None)

    def tag_to_explicit_string(tag) -> str:
        tag_name = tag.name
        attributes = ' '.join(f'{key}=&quot;{value}&quot;' for key, value in tag.attrs.items())
        return f'<{tag_name} {attributes}></{tag_name}>'
    
    simplified_svg = "\n".join([tag_to_explicit_string(t) for t in line_tags + circle_tags])

    label_colors = generate_distinct_colors(len(label_names))

    # 최종 라벨 리스트 구성
    return [
        {
            "name": name,
            "color":label_colors[i],  # 라벨 그룹 기본색 (예: person vs falldown_person)
            "type": "skeleton",
            "sublabels": [{k: v for k, v in sub.items() if k != "id"} for sub in sublabels],
            "svg": simplified_svg,
            "attributes": []
        }
        for i, name in enumerate(label_names)
    ]

# ===========================
# CVAT API 유틸리티 함수
# ===========================

def build_headers(org_slug: str) -> dict:
    return {
        "Authorization": f"Token {TOKEN}",
        "Content-Type": "application/json",
        "X-Organization": org_slug,
    }

def get_or_create_organization(name: str) -> tuple:
    headers = build_headers("")
    res = requests.get(f"{CVAT_URL}/api/organizations", headers=headers)
    res.raise_for_status()
    for org in res.json()["results"]:
        if org["slug"] == name:
            return org["id"], org["slug"]
    res = requests.post(f"{CVAT_URL}/api/organizations", headers=headers, json={"name": name, "slug": name})
    res.raise_for_status()
    return res.json()["id"], res.json()["slug"]

def create_project(name: str, label_defs: list, headers: dict) -> int:
    res = requests.post(f"{CVAT_URL}/api/projects", headers=headers, json={"name": name, "labels": label_defs})
    res.raise_for_status()
    return res.json()["id"]

def get_project_labels(project_id: int, headers: dict) -> List[Dict]:
    res = requests.get(f"{CVAT_URL}/api/labels", headers=headers, params={"project_id": project_id})
    res.raise_for_status()
    return res.json()["results"]


def get_existing_task_names(project_id: int, headers: dict) -> set:
    names = set()
    page = 1
    while True:
        res = requests.get(f"{CVAT_URL}/api/tasks", headers=headers, params={"project_id": project_id, "page": page})
        res.raise_for_status()
        data = res.json()
        names.update(t["name"] for t in data["results"])
        if not data["next"]:
            break
        page += 1
    return names

def create_task_with_zip(name: str, project_id: int, zip_path: str, headers: dict) -> int:
    task_data = {
        "name": name,
        "project_id": project_id,
        "use_default_project_settings": True,
        "image_quality": 70,
        "segment_size": 100
    }
    res = requests.post(f"{CVAT_URL}/api/tasks", headers=headers, json=task_data)
    res.raise_for_status()
    task_id = res.json()["id"]

    upload_headers = headers.copy()
    upload_headers.pop("Content-Type", None)

    with open(zip_path, "rb") as f:
        files = {"client_files[0]": (os.path.basename(zip_path), f, "application/zip")}
        data = {
            "image_quality": 70,
            "use_zip_chunks": "false",
            "use_cache": "false",
            "sorting_method": "lexicographical",
            "upload_format": "zip",
        }
        res = requests.post(f"{CVAT_URL}/api/tasks/{task_id}/data", headers=upload_headers, files=files, data=data)
        res.raise_for_status()
    return task_id

def wait_until_task_ready(task_id: int, headers: dict, timeout: int = 60) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        res = requests.get(f"{CVAT_URL}/api/tasks/{task_id}", headers=headers)
        res.raise_for_status()
        if res.json().get("size", 0) > 0:
            return True
        time.sleep(1)
    return False

def get_jobs(task_id: int, headers: dict) -> list:
    res = requests.get(f"{CVAT_URL}/api/jobs?task_id={task_id}", headers=headers)
    res.raise_for_status()
    return res.json()["results"]

def get_user_id(username: str, headers: dict) -> int | None:
    res = requests.get(f"{CVAT_URL}/api/users", headers=headers, params={"search": username})
    res.raise_for_status()
    for u in res.json()["results"]:
        if u["username"] == username:
            return u["id"]
    return None

def assign_jobs_to_one_user(jobs: list, headers: dict, assignee_name: str) -> None:
    uid = get_user_id(assignee_name, headers)
    if not uid:
        print(f"❌ 사용자 '{assignee_name}'를 찾을 수 없습니다.")
        return
    for job in jobs:
        if not job.get("assignee"):
            requests.patch(f"{CVAT_URL}/api/jobs/{job['id']}", headers=headers, json={"assignee": uid}).raise_for_status()

def review_jobs(jobs: list, headers: dict) -> None:
    for job in jobs:
        ann = requests.get(f"{CVAT_URL}/api/jobs/{job['id']}/annotations", headers=headers).json()
        if ann.get("shapes"):
            requests.patch(f"{CVAT_URL}/api/jobs/{job['id']}", headers=headers, json={"stage": "validation", "state": "completed"}).raise_for_status()

def log_assignment(task_name: str, task_id: int, assignee: str, num_jobs: int) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_header = not ASSIGN_LOG_PATH.exists()
    with open(ASSIGN_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["timestamp", "task_name", "task_id", "assignee", "num_jobs"])
        w.writerow([now, task_name, task_id, assignee, num_jobs])

###
def get_project_id_by_name(project_name: str, headers: dict) -> int:
    res = requests.get(f"{CVAT_URL}/api/projects", headers=headers, params={"name": project_name})
    res.raise_for_status()
    results = res.json()["results"]
    if not results:
        raise ValueError(f"❌ 프로젝트 '{project_name}'을 찾을 수 없습니다.")
    return results[0]["id"]

def sync_sublabel_ids_in_json(json_path: str, target_skeleton_names: List[str]) -> None:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for skeleton in data:
        if skeleton.get("name") in target_skeleton_names:
            # print(f'\n✔ 대상 skeleton: "{skeleton["name"]}", id: {skeleton["id"]}')
            sublabel_map = {s["name"]: s["id"] for s in skeleton.get("sublabels", [])}
            svg_raw = skeleton.get("svg", "")
            svg_decoded = html.unescape(svg_raw)

            def replace_label_id(match):
                tag = match.group(0)
                name_match = re.search(r'data-label-name="([^"]+)"', tag)
                if not name_match:
                    return tag
                name = name_match.group(1)
                sublabel_id = sublabel_map.get(name)
                if sublabel_id is None:
                    return tag
                # data-label-id 없으면 추가
                if 'data-label-id="' in tag:
                    new_tag = re.sub(r'data-label-id="\d+"', f'data-label-id="{sublabel_id}"', tag)
                else:
                    new_tag = tag.rstrip('>') + f' data-label-id="{sublabel_id}">'
                return new_tag

            updated_svg = re.sub(r'<circle[^>]+>', replace_label_id, svg_decoded)
            # " → &quot; 만 수동 이스케이프
            updated_svg = updated_svg.replace('"', '&quot;')
            skeleton["svg"] = updated_svg
            skeleton["raw"] = updated_svg 

            # print("✅ 수정된 SVG:")
            # print(updated_svg)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\n📁 JSON 저장 완료: {json_path}")

def patch_label_raw_to_server(project_id: int, json_path: str, headers: dict):
    """
    JSON에서 정의한 라벨의 'raw' 필드를 서버에 PATCH로 업로드
    """
    with open(json_path, encoding="utf-8") as f:
        label_defs = json.load(f)

    label_raw_map = {
        label["name"]: label.get("raw")
        for label in label_defs if "raw" in label
    }

    res = requests.get(f"{CVAT_URL}/api/labels", headers=headers, params={"project_id": project_id})
    res.raise_for_status()
    server_labels = res.json()["results"]
    server_label_map = {label["name"]: label["id"] for label in server_labels}

    for name, raw in label_raw_map.items():
        label_id = server_label_map.get(name)
        if not label_id:
            print(f"❌ 서버에서 '{name}' 라벨을 찾을 수 없습니다.")
            continue

        payload = {"raw": raw}
        print(f"➡ PATCH 전송: label_id={label_id}, label_name={name}")
        res = requests.patch(f"{CVAT_URL}/api/labels/{label_id}", headers=headers, json=payload)
        res.raise_for_status()
        print(f"📌 '{name}' 라벨의 RAW 필드 서버 반영 완료")

def print_escaped_labels_for_textarea(json_path: str):
    """
    JSON 파일을 HTML 이스케이프된 문자열로 출력 (CVAT textarea 입력용)
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for label in data:
        if "raw" not in label and "svg" in label:
            label["raw"] = label["svg"]

    escaped = json.dumps(data, ensure_ascii=False, indent=2).replace('"', '&quot;')

    # print("\n========= 아래 내용을 CVAT UI에 붙여넣으세요 =========\n")
    # print(escaped)
    # print("\n======================================================")



# ===========================
# 실행 진입점
# ===========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--org_name", required=True)
    parser.add_argument("--zip_dir", required=True)
    parser.add_argument("--project_name", required=True)
    parser.add_argument("--svg_path", required=True)
    parser.add_argument("--assignees", nargs="*", default=[])
    parser.add_argument("--labels", required=True)
    parser.add_argument("--json_path", required=True)
    parser.add_argument("--print_escaped", action="store_true", help="CVAT textarea용 HTML 이스케이프 JSON 출력")

    args = parser.parse_args()
    label_names = [name.strip() for name in args.labels.split(",")]
    target_skeleton_names = label_names

    
    try:
        # 1. 조직 및 헤더
        print("📌 조직 및 헤더 생성 중...")
        org_id, org_slug = get_or_create_organization(args.org_name)
        headers = build_headers(org_slug)

        # 2. 프로젝트 생성 + 라벨 정의 등록
        print("📌 라벨 정의 생성 중...")
        label_defs = build_label_defs_from_svg_file(args.svg_path, label_names)

        print("📌 프로젝트 생성 중...")
        project_id = create_project(args.project_name, label_defs, headers)
        existing = get_existing_task_names(project_id, headers)

        # 3. 서버에서 현재 label 정보 저장
        print("📌 서버 라벨 정보 저장 중...")
        server_label_defs = get_project_labels(project_id, headers)
        output_json_path = Path(args.json_path)
        output_json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(server_label_defs, f, ensure_ascii=False, indent=2)

        # 4. SVG 수정
        print("📌 SVG data-label-id 동기화 중...")
        sync_sublabel_ids_in_json(str(output_json_path), target_skeleton_names)

        # 5. 필요시 출력
        if args.print_escaped:
            print("📌 textarea용 라벨 JSON 출력")
            print_escaped_labels_for_textarea(str(output_json_path))
            # exit(0)

        # 6. RAW 필드 PATCH
        print("📌 RAW 필드 서버 반영 중...")
        patch_label_raw_to_server(project_id, args.json_path, headers)

        # 7. 태스크 생성 및 할당
        print("📌 태스크 생성 및 작업 할당 시작...")
        for idx, z in enumerate(sorted(Path(args.zip_dir).rglob("*.zip"))):
            task_name = f"{z.stem}_keypoint"
            if task_name in existing:
                print(f"스킵: {task_name} 이미 존재")
                continue

            print(f"▶ 태스크 생성 중: {task_name}")
            try:
                task_id = create_task_with_zip(task_name, project_id, str(z), headers)
            except Exception as e:
                print(f"❌ 태스크 생성 실패: {task_name}, 오류: {e}")
                continue

            if not wait_until_task_ready(task_id, headers):
                print(f"❌ {task_name} 준비 실패")
                continue

            jobs = get_jobs(task_id, headers)
            if args.assignees:
                assignee = args.assignees[idx % len(args.assignees)]
                assign_jobs_to_one_user(jobs, headers, assignee)
                log_assignment(task_name, task_id, assignee, len(jobs))
                review_jobs(jobs, headers)
            else:
                print(f"📌 작업자 없음: {task_name}은 미할당 상태")

        print("모든 작업 완료.")
    except Exception as e:
        print(f"❌ 전체 실행 중 예외 발생: {e}")
