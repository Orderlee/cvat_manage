import argparse
import sys
import os
import pandas as pd
import matplotlib
# GUI 백엔드 대신 'Agg' 백엔드를 사용하도록 설정 (화면을 띄우지 않고 이미지 파일로만 저장)
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import koreanize_matplotlib  # for Korean fonts in plots
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
import requests
import json
import base64
import re
import time
import numpy as np

# 환경 변수 로딩
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

CSV_DIR = Path(os.getenv("CSV_DIR", "/default/csv"))
VIS_DIR = Path(os.getenv("VIS_DIR", "/default/vis"))
VIS_DIR.mkdir(parents=True, exist_ok=True)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", nargs="+", help="List of organization:project pairs")
    return parser.parse_args()

def get_recent_csv_files(n=5):
    csv_files = sorted(CSV_DIR.glob("cvat_job_report_*.csv"), reverse=True)
    return csv_files[:n]

def read_recent_reports(csv_dir, n=5):
    recent_files = get_recent_csv_files(n)
    combined_df = pd.DataFrame()
    for fpath in recent_files:
        temp_df = pd.read_csv(fpath)
        date_str = fpath.stem.split('_')[-1]
        try:
            report_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        temp_df['report_date'] = report_date
        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)
    return combined_df

def plot_custom_state_status(df, title="Task Status Distribution by Assignee", output_name="user_selected_state_status_count.png"):
    # 조건과 라벨 정의
    conditions = [
        (df["stage"] == "acceptance") & (df["state"] == "completed"),
        (df["stage"] == "annotation") & (df["state"] == "completed"),
        (df["stage"] == "annotation") & (df["state"] == "in progress"),
    ]
    labels = ["Inspection Completed", "Labeling Completed", "Labeling in Progress"]

    df = df.copy()
    df["combined"] = ""
    for cond, label in zip(conditions, labels):
        df.loc[cond, "combined"] = label

    df_filtered = df[df["combined"] != ""]
    if df_filtered.empty:
        print(f"  -> No data available, skipping: {output_name}")
        return

    state_counts = df_filtered.groupby(["assignee", "combined"]).size().unstack(fill_value=0)

    for label in labels:
        if label not in state_counts.columns:
            state_counts[label] = 0
    state_counts = state_counts[labels]

    ax = state_counts.plot(kind="bar", figsize=(12, 6), colormap="Set2", width=0.8)
    ax.set_title(title)
    ax.set_xlabel("Assignee")
    ax.set_ylabel("Task Count")

    for container in ax.containers:
        ax.bar_label(container, label_type="edge", fontsize=9)

    plt.xticks(rotation=45)
    plt.tight_layout()
    out_path = VIS_DIR / output_name
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  -> Graph saved: {out_path}")

def plot_custom_state_status_daily_diff(df, title="Daily Task Status Change by Assignee", output_name="state_status_daily_diff.png"):
    if 'report_date' not in df.columns:
        print(f"  -> No 'report_date' column, skipping: {output_name}")
        return

    conditions = [
        (df["stage"] == "acceptance") & (df["state"] == "completed"),
        (df["stage"] == "annotation") & (df["state"] == "completed"),
        (df["stage"] == "annotation") & (df["state"] == "in progress"),
    ]
    labels = ["Inspection Completed", "Labeling Completed", "Labeling in Progress"]

    df = df.copy()
    df["combined"] = ""
    for cond, label in zip(conditions, labels):
        df.loc[cond, "combined"] = label

    df_filtered = df[df["combined"] != ""]
    unique_dates = sorted(df_filtered['report_date'].unique())
    if len(unique_dates) < 2:
        print(f"  -> Need at least 2 dates, skipping: {output_name}")
        return

    latest_date = unique_dates[-1]
    prev_date = unique_dates[-2]

    latest_counts = df_filtered[df_filtered['report_date'] == latest_date].groupby(['assignee', 'combined']).size().unstack(fill_value=0)
    prev_counts = df_filtered[df_filtered['report_date'] == prev_date].groupby(['assignee', 'combined']).size().unstack(fill_value=0)

    for label in labels:
        if label not in latest_counts.columns:
            latest_counts[label] = 0
        if label not in prev_counts.columns:
            prev_counts[label] = 0
    latest_counts = latest_counts[labels]
    prev_counts = prev_counts[labels]

    diff_counts = latest_counts.subtract(prev_counts, fill_value=0)
    diff_counts[diff_counts < 0] = 0

    if diff_counts.sum().sum() == 0:
        print(f"  -> No daily change, skipping: {output_name}")
        return

    ax = diff_counts.plot(kind="bar", figsize=(12, 6), colormap="Set2", width=0.8)
    ax.set_title(title)
    ax.set_xlabel("Assignee")
    ax.set_ylabel("Daily Change in Task Count")

    for container in ax.containers:
        ax.bar_label(container, label_type="edge", fontsize=9)

    plt.xticks(rotation=45)
    plt.tight_layout()
    # date_label = f"{prev_date} → {latest_date}"
    ax.legend(title=f"Period: {prev_date}")
    out_path = VIS_DIR / output_name
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  -> Daily change graph saved: {out_path}")

def plot_project_counts_by_organization(df, output_name_prefix="project_counts"):
    """
    organization별로 데이터를 나누어 각 조직에 속한 프로젝트들의 작업 개수를 시각화하고,
    파일을 조직별로 개별 저장합니다.
    """
    df = df.copy()

    # 최신 날짜로 필터링
    if 'report_date' in df.columns:
        latest_date = df['report_date'].max()
        df = df[df['report_date'] == latest_date]

    orgs = df['organization'].unique()
    for org in orgs:
        org_df = df[df['organization'] == org]
        if org_df.empty:
            continue

        counts = org_df['project'].value_counts()

        # 컬러맵에서 막대 개수에 맞춰 색상 배열 생성
        cmap = plt.get_cmap('Set3')
        colors = cmap(np.linspace(0, 1, len(counts)))

        plt.figure(figsize=(8,5))
        ax = counts.plot(kind="bar", color=colors)
        plt.title(f"{org} - Job Count by Project")
        plt.xlabel("Project")
        plt.ylabel("Job Count")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()

        for container in ax.containers:
            ax.bar_label(container, label_type="edge", fontsize=9)

        today_str = datetime.today().strftime("%Y-%m-%d")
        org_name_safe = re.sub(r'[^\w\s-]', '_', str(org))
        out_path = VIS_DIR / f"{today_str}_{org_name_safe}_{output_name_prefix}.png"
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"그래프 저장 완료: {out_path}")

def plot_estimated_daily_jobs_by_org(df, output_name_prefix="est_daily_jobs"):
    """
    조직별로 연속된 두 날짜의 작업 수 차이를 계산하여 (다음날 - 이전날),
    음수 값은 0으로 처리한 후 선 그래프를 저장합니다.
    조건: acceptance-completed, annotation-completed.
    """
    # report_date 컬럼 존재 여부 확인
    if 'report_date' not in df.columns:
        print("  -> 'report_date' column is missing; cannot plot.")
        return

    # 두 가지 조건에 따른 필터링
    mask = (
        ((df["stage"] == "acceptance") & (df["state"] == "completed")) |
        ((df["stage"] == "annotation") & (df["state"] == "completed"))
    )
    df_filtered = df[mask].copy()

    # 조직 목록 반복
    organizations = df_filtered['organization'].unique()
    for org in organizations:
        org_df = df_filtered[df_filtered['organization'] == org]
        if org_df.empty:
            print(f"  -> No data for organization '{org}', skipping.")
            continue

        # 날짜별 작업 수 집계 후 오름차순 정렬
        daily_counts = org_df.groupby('report_date').size().sort_index()

        if len(daily_counts) < 2:
            print(f"  -> Not enough dates for '{org}', skipping.")
            continue

        # 연속된 두 날짜의 차이 계산 (다음날 - 이전날), 음수는 0으로 처리
        dates_for_plot = []
        jobs_done_estimates = []
        counts_values = daily_counts.values
        dates = daily_counts.index
        for i in range(len(counts_values) - 1):
            jobs_done = counts_values[i+1] - counts_values[i]
            jobs_done_estimates.append(max(0, jobs_done))
            dates_for_plot.append(dates[i])  # 이전 날짜를 레이블로 사용

        # 선 그래프 그리기
        plt.figure(figsize=(8, 5))
        plt.plot(dates_for_plot, jobs_done_estimates, marker='o', color='#66c2a5')
        plt.title(f"{org} - Estimated Jobs Done per Day")
        plt.xlabel("Date")
        plt.ylabel("Estimated Jobs Done")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()

        # 각 점 위에 값 표시
        for x, y in zip(dates_for_plot, jobs_done_estimates):
            plt.text(x, y, f"{int(y)}", ha='center', va='bottom', fontsize=9)

        # 그래프 저장
        today_str = datetime.today().strftime("%Y-%m-%d")
        org_safe = re.sub(r'[^\w\s-]', '_', str(org))
        out_path = VIS_DIR / f"{today_str}_{org_safe}_{output_name_prefix}.png"
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"  -> Estimated daily jobs graph saved for {org}: {out_path}")



def get_access_token():
    tenant_id = os.getenv("TENANT_ID")
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scopes = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )

    token_response = app.acquire_token_for_client(scopes=scopes)
    if "access_token" in token_response:
        return token_response["access_token"]
    else:
        raise Exception(f"❌ 토큰 발급 실패: {token_response.get('error_description')}")

def send_email_via_graph(targets, subject, body, override_receiver=None, specific_org=None):
    """
    첨부파일 경로 구성 및 이메일 전송.
    override_receiver가 있으면 EMAIL_RECEIVER 대신 해당 이메일로만 보내고,
    specific_org가 있으면 그 조직에 대한 첨부파일만 포함합니다.
    """
    EMAIL_SENDER = os.getenv("EMAIL_SENDER")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
    EMAIL_CC = os.getenv("EMAIL_CC")

    if not EMAIL_SENDER:
        print("📭 EMAIL_SENDER가 설정되지 않아 메일을 전송하지 않습니다.")
        return

    # 수신자 설정
    if override_receiver:
            if isinstance(override_receiver, list):
                to_list = override_receiver
            else:
                to_list = [override_receiver]
            cc_list = []
    else:
        if not EMAIL_RECEIVER:
            print("📭 EMAIL_RECEIVER가 설정되지 않아 메일을 전송하지 않습니다.")
            return
        to_list = [addr.strip() for addr in EMAIL_RECEIVER.split(",")]
        cc_list = [addr.strip() for addr in EMAIL_CC.split(",")] if EMAIL_CC else []

    access_token = get_access_token()
    today_str = datetime.today().strftime("%Y-%m-%d")
    base_path = Path(VIS_DIR)

    attachment_paths = set()
    seen_orgs = set()


    # image_files = []
    for org, proj in targets:
        # 조직과 프로젝트 이름을 안전한 파일명으로 변환
        org_safe = re.sub(r'[^\w\s-]', '_', str(org))
        proj_safe = re.sub(r'[^\w\s-]', '_', str(proj))

        # specific_org 옵션이 있으면 해당 조직만 처리
        if specific_org and org_safe != specific_org:
            continue

        # --- 조직 단위로 저장된 그래프 추가 ---
        # 예: 2025-08-05_vietnamlabeling_state_status_recent5.png
        if org_safe not in seen_orgs:
            prefix_org = f"{today_str}_{org_safe}"
            attachment_paths.update([
                base_path / f"{prefix_org}_state_status_recent5.png",
                base_path / f"{prefix_org}_state_status_daily_diff.png",
                base_path / f"{prefix_org}_project_counts_recent.png",
                base_path / f"{prefix_org}_est_daily_jobs.png"
            ])
            seen_orgs.add(org_safe)

        # --- 조직+프로젝트 단위로 저장된 그래프 추가 ---
        # 예: 2025-08-05_vietnamlabeling_projectA_state_status_recent5.png
        prefix_proj = f"{today_str}_{org_safe}_{proj_safe}"
        attachment_paths.update([
            base_path / f"{prefix_proj}_state_status_recent5.png",
            base_path / f"{prefix_proj}_state_status_daily_diff.png",
        ])

    # 실제 존재하는 파일만 첨부
    attachments = []
    for file_path in attachment_paths:
        if file_path.exists():
            with open(file_path, "rb") as f:
                content_bytes = f.read()
            content_b64 = base64.b64encode(content_bytes).decode()

            attachments.append({
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": file_path.name,
                "contentType": "image/png",
                "contentBytes": content_b64
            })
        else:
            print(f"⚠️ 첨부 이미지 누락: {file_path}")

    if not attachments:
        print("📭 첨부할 파일이 없어 메일을 전송하지 않습니다.")
        return

    message = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [{"emailAddress": {"address": addr}} for addr in to_list],
            "ccRecipients": [{"emailAddress": {"address": addr}} for addr in cc_list] if cc_list else [],
            "attachments": attachments
        }
    }

    url = f"https://graph.microsoft.com/v1.0/users/{EMAIL_SENDER}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, data=json.dumps(message))
    if response.status_code == 202:
        print("📧 Graph API로 이메일 전송 완료")
    else:
        print(f"❌ Graph API 이메일 전송 실패: {response.status_code}, {response.text}")



def parse_targets_from_env(n: int = 5) -> list[tuple[str, str]]:
    organizations = os.getenv("ORGANIZATIONS", "").split(",")
    targets = []
    for csv_file in get_recent_csv_files(N):
        df = pd.read_csv(csv_file)
        for org in organizations:
            projects = df[df["organization"] == org]["project"].unique()
            for project in projects:
                targets.append((org, project))
    return list(set(targets))


if __name__ == "__main__":
    # Number of recent CSVs to process
    N = 5
    args = parse_args()
    if args.targets:
        targets = []
        for t in args.targets:
            if ":" in t:
                org, proj = t.split(":", 1)
                targets.append((org.strip(), proj.strip()))
            else:
                print(f"⚠️ Ignoring malformed target '{t}'. Expected format org:project.")
    else:
        targets = parse_targets_from_env(n=N)

    recent_df = read_recent_reports(CSV_DIR, n=N)
    recent_df2 = read_recent_reports(CSV_DIR, n=10)

    today_str = datetime.today().strftime("%Y-%m-%d")

    # 1. Organization-level visualizations
    orgs = recent_df['organization'].unique()
    for org in orgs:
        org_df = recent_df[recent_df['organization'] == org].copy()
        org_name_safe = re.sub('[^\\w\\s-]', '_', str(org))
        print(f"\n[{org}] Generating visualizations:")
        plot_custom_state_status(
            org_df,
            title=f"{org} - Task Status Distribution by Assignee (Last 5 Days)",
            output_name=f"{today_str}_{org_name_safe}_state_status_recent5.png",
        )
        plot_custom_state_status_daily_diff(
            org_df,
            title=f"{org} - Daily Task Status Change by Assignee (Most Recent Day)",
            output_name=f"{today_str}_{org_name_safe}_state_status_daily_diff.png",
        )
    
    plot_estimated_daily_jobs_by_org(recent_df2, output_name_prefix="est_daily_jobs")

    # 2. Project-level visualizations
    projects = recent_df['project'].unique()
    for proj in projects:
        proj_df = recent_df[recent_df['project'] == proj].copy()
        proj_name_safe = re.sub('[^\\w\\s-]', '_', str(proj))
        org_names = proj_df['organization'].unique()
        org_name = org_names[0] if len(org_names) > 0 else "unknown_org"
        org_name_safe = re.sub('[^\\w\\s-]', '_', str(org_name))

        print(f"\n[{proj} / {org_name}] Generating project-level visualizations:")
        plot_custom_state_status(
            proj_df,
            title=f"{org} - {proj} - Task Status Distribution by Assignee (Last 5 Days)",
            output_name=f"{today_str}_{org_name_safe}_{proj_name_safe}_state_status_recent5.png",
        )
        plot_custom_state_status_daily_diff(
            proj_df,
            title=f"{org} - {proj} - Daily Task Status Change by Assignee (Most Recent Day)",
            output_name=f"{today_str}_{org_name_safe}_{proj_name_safe}_state_status_daily_diff.png",
        )

    # 3. Project count charts per organization
    print("\nGenerating project count charts per organization:")
    plot_project_counts_by_organization(recent_df, output_name_prefix="project_counts_recent")
    print("\n")


    summary_text = "👥 Annotator Report Summary:\n\n"
    summary_text += "This is a report summarizing the annotation work performed by annotators.\n"

    print(summary_text, '\n')
    # send_email_via_graph(targets, "Daily CVAT Report by Annotator", summary_text)
    if targets:
        send_email_via_graph(targets, subject="Daily CVAT Report by Annotator",
                            body=summary_text)
    
    time.sleep(2)
        
    vietnam_targets = [t for t in targets if re.sub(r'[^\w\s-]', '_', t[0]) == "vietnamlabeling"]
    if vietnam_targets:
        send_email_via_graph(vietnam_targets,
                            subject="Daily CVAT Report for Vietnam Labeling",
                            body=summary_text,
                            override_receiver=["xuanht@nobisoft.com.vn", "hayun@nobisoft.kr"],
                            # override_receiver="amy@nobisoft.com.vn",
                            specific_org="vietnamlabeling")