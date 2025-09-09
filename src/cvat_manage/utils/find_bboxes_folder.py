import os

def find_bboxes_in_category(project_name: str, category_name: str):
    """
    datasets/projects/{project_name}/processed_data/{category_name}
    경로 아래 모든 하위폴더를 탐색하여
    'bboxes' 폴더를 찾습니다.
    """
    root_dir = ""

    bboxes_paths = []

    for dirpath, dirnames, _ in os.walk(root_dir):
        if "bboxes" in dirnames:
            full_path = os.path.join(dirpath, "bboxes")
            rel_path = os.path.relpath(full_path, "")
            # bboxes_paths.append(os.path.join(dirpath, "bboxes"))
            bboxes_paths.append(rel_path)

    return bboxes_paths


if __name__ == "__main__":
    # 👉 프로젝트 이름과 카테고리 목록 지정
    project_name = "vietnam_data"
    categories = [
        "bat_as_weapon",
        "gun_as_weapon",
        "knife_as_weapon",
    ]

    total_count = 0  # 전체 합계

    # 결과 저장 파일 경로
    output_file = "vietnam_weapon_bboxes_list.txt"

    with open(output_file, "w", encoding="utf-8") as f:
        for category_name in categories:
            results = find_bboxes_in_category(project_name, category_name)

            header = f"[{project_name}/{category_name}]에서 발견된 'bboxes' 폴더:"
            print(header)
            f.write(header + "\n")

            print("-" * 60)
            f.write("-" * 60 + "\n")

            for path in results:
                print(path)
                f.write(path + "\n")

            summary = f"총 개수: {len(results)}"
            print(summary)
            print("=" * 60)
            f.write(summary + "\n")
            f.write("=" * 60 + "\n")

            total_count += len(results)

        final_line = f"👉 모든 카테고리에서 발견된 'bboxes' 폴더 총 개수: {total_count}"
        print(final_line)
        f.write(final_line + "\n")

    print(f"\n✅ 결과가 '{output_file}' 파일에도 저장되었습니다.")