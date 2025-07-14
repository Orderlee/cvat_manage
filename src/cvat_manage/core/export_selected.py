import csv
from pathlib import Path
from datetime import datetime
from export import (
    get_task_info, get_annotations, get_organization_name,
    run_cvat_cli_export, load_assignee_map_from_env, CVAT_EXPORT_FORMAT,
    CVAT_EXPORT_FORMAT_4, RESULT_DIR, ORGANIZATIONS
)

# csv 경로
CSV_PATH = "path" # columns ->  project_name,task_id,job_id,assignee_display_name
EXPORT_LOG_PATH = Path("path/log.csv")

def main():

    selected_items = [] # 리스트 형태로 (task_id, job_id, assignee) 저장
    
    with open(CSV_PATH, newline="", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            selected_items.append({
                "task_id": row["task_id"],
                "job_id": row["job_id"],
                "assignee": row["assignee_display_name"]
            })


    # exported_task_ids = set()
    # if EXPORT_LOG_PATH.exists():
    #     with open(EXPORT_LOG_PATH, newline="") as f:
    #         reader = csv.DictReader(f)
    #         for row in reader:
    #             exported_task_ids.add(row["task_id"])

    # assignee 매핑
    assignee_map = load_assignee_map_from_env()
    today_str = datetime.today().strftime("%Y-%m-%d")

    for item in selected_items:
        task_id = item["task_id"]
        job_id = item["job_id"]
        assignee = item["assignee"]

        # task 정보 조회
        task_info = get_task_info(int(task_id))
        task_name = task_info.get("name", f"task_{task_id}")
        org_slug = get_organization_name(task_info.get("organization"))

        if org_slug not in ORGANIZATIONS:
            print(f" 조직 불일치 -> 건너뛰: {org_slug}")
            continue

        # job 기반 어노테이션에서 라벨 타입 조회
        label_types = set()
        try:
            annotations = get_annotations(int(job_id))
            for shape in annotations.get("shapes", []):
                shape_type = shape.get("shape_type") or shape.get("type")
                if shape_type:
                    label_types.add(shape_type)
        except Exception as e:
            print(f"어노테이션 조회 실패: job_id={job_id}, {e}")
            continue

        # export 디렉토리
        result_dir = Path(RESULT_DIR) / today_str / org_slug / task_name.replace(" ", "_")
        result_dir.mkdir(parents=True, exist_ok=True)

        # export 실행
        run_cvat_cli_export(
            int(task_id),
            task_name,
            assignee,
            result_dir,
            EXPORT_LOG_PATH,
            assignee_map,
            CVAT_EXPORT_FORMAT_4
        )

if __name__ == "__main__":
    main()


    
