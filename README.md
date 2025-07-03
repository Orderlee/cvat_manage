# 📦 CVAT Manage: 어노테이션 자동화 및 분석 시스템

CVAT Manage는 CVAT 플랫폼 기반의 어노테이션 관리, 키포인트 기반 라벨 자동 생성, 결과 분석 및 시각화를 자동화하는 통합 도구입니다. 이 프로젝트는 어노테이터의 누락 프레임 관리, 라벨 정의 자동화, 일일 리포트 생성 및 시각화를 포함한 작업 흐름을 자동화합니다.

---

## 📁 프로젝트 구조

```
├── Person.svg                  # 키포인트 라벨 정의용 SVG
├── README.md
├── csv/                        # 작업별 리포트 CSV 파일
├── json/                       # 라벨 정의 JSON
├── logs/                       # 작업 로그 및 누락 로그
├── result/                     # 내보낸 결과 및 로그
├── run_keypoint.sh             # 키포인트 전용 실행 스크립트
├── run_main.sh                 # 전체 워크플로 실행 스크립트
├── src/
│   └── cvat_manage/
│       ├── analytics/          # 분석 및 리포트 발송
│       │   └── send_report.py
│       ├── core/               # 핵심 기능 구현
│       │   ├── export.py
│       │   ├── import_keypoint.py
│       │   ├── import_ops.py
│       │   ├── move_exported_file.py
│       │   └── omission.py
│       ├── main.py             # 전체 자동화 워크플로
│       └── utils/
│           └── imgae_extract.py  # YOLO 기반 프레임 추출
└── visualize/                  # 시각화 결과물 PNG
```

---

## 🔧 주요 기능

### 1. CVAT 작업 분석 및 누락 감지
- `omission.py`:
  - CVAT API를 통해 모든 작업(Job)을 수집
  - 어노테이션 누락 프레임 및 라벨 수량, 이슈 등 통계 계산
  - 사용자별 누락률 계산 및 CSV 저장

### 2. 리포트 생성 및 시각화
- `send_report.py`:
  - 최근 5일간 어노테이션 통계 기반 그래프 자동 생성
  - 사용자별/일자별 프레임 수 및 라벨 수 시각화
  - Microsoft Graph API를 통한 이메일 발송 기능 포함

### 3. 어노테이션 결과 자동 Export 및 이동
- `export.py`:
  - 승인된 작업을 기반으로 라벨 결과 자동 다운로드 (CLI 활용)
  - JSON만 포함된 ZIP 파일로 변환하여 결과 관리
- `move_exported_file.py`:
  - 파일명 기반으로 목적 폴더 탐색
  - `meta.yaml` 자동 생성 및 압축 해제 처리

### 4. 키포인트 라벨 자동 정의 및 작업 생성
- `import_keypoint.py`:
  - `Person.svg`로부터 sublabel 정의 자동 생성
  - 프로젝트 및 라벨 서버 동기화, 작업 생성 및 사용자 할당까지 자동화
  - SVG 내 `data-label-id` 동기화 및 JSON raw 필드 서버 패치 포함

### 5. YOLO 기반 프레임 추출
- `imgae_extract.py`:
  - YOLOv8을 활용한 "person" 탐지 기반 프레임 추출
  - 2초 간격 샘플링 및 GPU 병렬 처리
  - 탐지된 프레임을 카테고리별로 저장 및 ZIP 압축 수행

---

## ▶ 실행 방법

### 전체 자동화 실행
```bash
bash run_main.sh
```

### 키포인트 라벨 기반 프로젝트 생성 및 작업 등록
```bash
bash run_keypoint.sh
```

---

## ✅ 주요 결과물

- `csv/*.csv`: 작업별 어노테이션 통계
- `logs/*.log`: 누락 프레임 로그
- `visualize/*.png`: 사용자별 라벨 및 프레임 수 시각화
- `result/*`: export된 결과 및 이동 이력

---

## 📌 환경설정

`.env` 파일에 다음 항목 필요:

```dotenv
CVAT_URL=https://your.cvat.server
TOKEN=your_token
EMAIL_SENDER=your_email@domain.com
EMAIL_RECEIVER=receiver@domain.com
RESULT_DIR=...
DEST_DIR=...
INPUT_ROOT=...
OUTPUT_ROOT=...
```

---

## 🧩 의존성

- Python 3.9+
- 패키지: `requests`, `matplotlib`, `pandas`, `tqdm`, `ultralytics`, `python-dotenv`, `seaborn`, `koreanize_matplotlib`, `msal`, `BeautifulSoup4`
- 외부 도구: `cvat-cli`, `YOLOv8`