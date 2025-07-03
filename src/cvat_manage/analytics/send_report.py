import argparse
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import koreanize_matplotlib
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from msal import ConfidentialClientApplication
import requests
import json
import base64

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

def extract_mmdd_from_filename(filename):
    date_part = filename.stem.split("_")[-1]
    dt = datetime.strptime(date_part, "%Y-%m-%d")
    return dt.strftime("%m-%d")

def load_frame_data(csv_files, organization=None, project=None):
    records = []
    for file in csv_files:
        df = pd.read_csv(file)
        date_label = extract_mmdd_from_filename(file)

        df = df[df["state"] == "completed"]
        if organization:
            df = df[df["organization"] == organization]
        if project:
            df = df[df["project"] ==project]
            
        for _, row in df.iterrows():
            user = row["assignee"]
            frame_range = row.get("frame_range", "")
            try:
                start, stop = map(int, frame_range.split("~"))
                frame_count = stop - start + 1
            except:
                frame_count = 0
            label_count = int(row.get("label_count", 0))
            records.append({
                "user": user,
                "date": date_label,
                "frame_count": frame_count,
                "label_count": label_count
            })
    return pd.DataFrame(records)

def plot_user_frame_by_day(df, prefix, organization, project):
    pivot = df.groupby(["user", "date"])['frame_count'].sum().unstack(fill_value=0)
    ax = pivot.plot(kind="bar", figsize=(12, 6), colormap="tab20", width=0.8)
    for container in ax.containers:
        ax.bar_label(container, padding=1, fontsize=10)
    plt.title(f"{organization} / {project} - Number of Completed Images per Worker in the Last 5 Days")
    plt.xlabel("Annotator")
    plt.ylabel("Number of Images")
    plt.xticks(rotation=45)
    plt.tight_layout()
    today_str = datetime.today().strftime("%Y-%m-%d")
    out_path = VIS_DIR / f"{prefix}_user_frame_assignment_by_day_{today_str}.png"
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()

def plot_daily_completed_images(df, prefix, organization, project):
    pivot = df.groupby(["user", "date"])['frame_count'].sum().unstack(fill_value=0)
    pivot = pivot.loc[:, sorted(pivot.columns, key=lambda x: datetime.strptime(x, "%m-%d"))]

    daily = pivot.diff(axis=1)
    first_date = pivot.columns[0]
    daily[first_date] = pivot[first_date]

    display_dates = pivot.columns[-5:]
    daily = daily[display_dates]

    ax = daily.plot(kind="bar", figsize=(12, 6), colormap="tab20", width=0.8)
    for container in ax.containers:
        ax.bar_label(container, padding=1, fontsize=10)
    plt.title(f"{organization} / {project} - Daily Number of Completed Images per Worker")
    plt.xlabel("Annotator")
    plt.ylabel("Daily Number of Completed Images")
    plt.xticks(rotation=45)
    plt.tight_layout()
    today_str = datetime.today().strftime("%Y-%m-%d")
    out_path = VIS_DIR / f"{prefix}_user_daily_frame_assignment_{today_str}.png"
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()

def plot_user_labelcount_by_day(df, prefix, organization, project):
    pivot = df.groupby(["user", "date"])["label_count"].sum().unstack(fill_value=0)
    ax = pivot.plot(kind="bar", figsize=(12, 6), colormap="tab20", width=0.8)
    for container in ax.containers:
        ax.bar_label(container, padding=1, fontsize=10)
    plt.title(f"{organization} / {project} - Number of Labeled Objects per Worker in the Last 5 Days")
    plt.xlabel("Annotator")
    plt.ylabel("Labeled Objects")
    plt.xticks(rotation=45)
    plt.tight_layout()
    today_str = datetime.today().strftime("%Y-%m-%d")
    out_path = VIS_DIR / f"{prefix}_user_labelcount_by_day_{today_str}.png"
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()

def plot_daily_labeled_objects(df, prefix, organization, project):
    pivot = df.groupby(["user", "date"])["label_count"].sum().unstack(fill_value=0)
    pivot = pivot.loc[:, sorted(pivot.columns, key=lambda x: datetime.strptime(x, "%m-%d"))]

    daily = pivot.diff(axis=1)
    first_date = pivot.columns[0]
    daily[first_date] = pivot[first_date]

    display_dates = pivot.columns[-5:]
    daily = daily[display_dates]

    ax = daily.plot(kind="bar", figsize=(12, 6), colormap="tab20", width=0.8)
    for container in ax.containers:
        ax.bar_label(container, padding=1, fontsize=10)
    plt.title(f"{organization} / {project} - Daily Number of Labeled Objects per Worker")
    plt.xlabel("Annotator")
    plt.ylabel("Daily Labeled Objects")
    plt.xticks(rotation=45)
    plt.tight_layout()
    today_str = datetime.today().strftime("%Y-%m-%d")
    out_path = VIS_DIR / f"{prefix}_user_daily_labelcount_{today_str}.png"
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()

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

def send_email_via_graph(targets, subject, body):
    EMAIL_SENDER = os.getenv("EMAIL_SENDER")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
    EMAIL_CC = os.getenv("EMAIL_CC")
    if not (EMAIL_SENDER and EMAIL_RECEIVER):
        print("📭 이메일 정보가 누락되어 전송하지 않습니다.")
        return

    to_list = [addr.strip() for addr in EMAIL_RECEIVER.split(",")]
    cc_list = [addr.strip() for addr in EMAIL_CC.split(",")] if EMAIL_CC else []

    access_token = get_access_token()
    today_str = datetime.today().strftime("%Y-%m-%d")
    base_path = Path(VIS_DIR)

    image_files = []
    for org, proj in targets:
        prefix = f"{org}_{proj}"
        image_files += [
            base_path / f"{prefix}_user_daily_frame_assignment_{today_str}.png",
            base_path / f"{prefix}_user_frame_assignment_by_day_{today_str}.png",
            base_path / f"{prefix}_user_labelcount_by_day_{today_str}.png",
            base_path / f"{prefix}_user_daily_labelcount_{today_str}.png"
        ] 

    attachments = []
    for file_path in image_files:
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

if __name__ == "__main__":
    N = 5
    recent_files = get_recent_csv_files(N)

    # targets = [
    #     ("thailabeling", "ad_lib_weapon"),
    #     ("thailabeling", "ad_lib")
    # ]
    args = parse_args()
    if not args.targets:
        print("❌ 전달된 targets 없음 --targets thailabeling:ad_lib ... 형태로 지정해야 합니다.")
        sys.exit(1)
    
    targets = [tuple(t.split(":")) for t in args.targets]

    for org, proj in targets:
        prefix = f"{org}_{proj}"
        df_filtered = load_frame_data(recent_files, organization=org, project=proj)

        if df_filtered.empty:
            print(f"⚠️ 데이터 없음: {prefix}")
            continue

        print(f"✅ 시각화 생성: {prefix}")
        plot_user_frame_by_day(df_filtered, prefix, org, proj)
        plot_daily_completed_images(df_filtered, prefix, org, proj)
        plot_user_labelcount_by_day(df_filtered, prefix, org, proj)
        plot_daily_labeled_objects(df_filtered, prefix, org, proj)

    summary_text = "👥 Annotator Report Summary:\n\n"
    summary_text += "This is a report summarizing the annotation work performed by annotators.\n"

    print(summary_text, '\n')
    send_email_via_graph(targets, "Daily CVAT Report by Annotator", summary_text)
