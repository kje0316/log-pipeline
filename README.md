# log-pipeline
직방 앱 로그 데이터를 활용한 실시간/배치 파이프라인 구축 

# 🏠 직방 로그 데이터 파이프라인 구축 프로젝트

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-231F20.svg?logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C.svg?logo=apachespark)](https://spark.apache.org/)
[![AWS S3](https://img.shields.io/badge/AWS-S3-569A31.svg?logo=amazons3)](https://aws.amazon.com/s3/)
[![Git](https://img.shields.io/badge/Git-Collaboration-F05032.svg?logo=git)](https://git-scm.com/)

> 직방 앱의 로그 데이터를 활용하여, 데이터의 수집부터 처리, 적재, 시각화에 이르는 End-to-End 파이프라인 구축을 목표로 합니다.

---

## 📌 1. 프로젝트 개요 (Overview)

본 프로젝트는 '직방' 앱에서 발생하는 사용자 행동 로그(e.g., 매물 조회, 찜하기, 지역 검색)를 처리하는 데이터 파이프라인을 설계하고 구현합니다.

데이터 엔지니어의 실무적 역할을 경험하기 위해, **배치(Batch) 파이프라인**을 우선적으로 구축하여 안정적인 데이터 웨어하우스(D.W) 및 데이터 마트(D.M)를 생성합니다. 이후, **실시간(Real-time) 파이프라인**으로 확장하여 실시간 대시보드를 구축하는 것을 목표로 합니다.

* **프로젝트 기간:** 2025.10.25 (토) ~ 2025.10.29 (수) (총 5일)
* **팀 구성:** 2명
* **핵심 목표:**
    1.  대용량 로그 데이터 처리를 위한 배치 파이프라인 아키텍처 설계 및 구축
    2.  (Optional) 실시간 스트리밍 아키텍처 설계 및 구축
    3.  D.W / D.M 설계를 통한 비즈니스 인사이트 도출
    4.  배치/실시간 대시보드를 통한 데이터 시각화
    5.  Git을 통한 협업 워크플로우 경험

---

## ✨ 2. 주요 기능 및 목표 (Key Features & Goals)

| 구분 | 주요 기능 (목표) | 설명 |
|---|---|---|
| 📦 **배치 파이프라인** | **D.W & D.M 구축** | Kafka로 유입된 로그를 S3 Data Lake에 적재하고, Spark Batch로 일/시간 단위로 ETL 작업을 수행하여 D.W에 적재합니다. 이후 비즈니스 요구사항(e.g., 지역별 인기 매물)에 맞춘 D.M을 구축합니다. |
| 📊 **배치 대시보드** | **주요 지표 시각화** | D.M의 데이터를 기반으로 **BI 툴(Superset, Metabase 등)**을 연동하여, DAU/WAU, 가장 많이 조회된 지역, 시간대별 트래픽 등 핵심 비즈니스 지표를 시각화합니다. |
| ⚡ **(Optional)<br>실시간 파이프라인** | **실시간 집계** | Kafka의 토픽을 Spark Streaming으로 직접 소비(Consume)하여, '최근 5분간 찜이 가장 많이 눌린 매물' 등 실시간성 지표를 집계하고 별도 DB에 저장합니다. |
| 📈 **(Optional)<br>실시간 대시보드** | **실시간 현황 모니터링** | 실시간 집계 DB의 데이터를 **시각화 툴(Grafana, Streamlit 등)**과 연동하여, 현재 서비스 현황을 실시간으로 모니터링하는 대시보드를 구현합니다. |

---

## 🧱 3. 아키텍처 (Architecture)

### A. 배치 파이프라인 (Batch Pipeline) - (Main Priority)

`[Kafka] -> [AWS S3 (Data Lake)] -> [Spark Batch (ETL)] -> [Data Warehouse (D.W)] -> [Data Mart (D.M)] -> [BI Dashboard (Superset/Metabase)]`

### B. 실시간 파이프라인 (Real-time Pipeline) - (Optional)

`[Kafka] -> [Spark Streaming] -> [Real-time DB (Redis/MongoDB)] -> [Real-time Dashboard (Grafana/Streamlit)]`

---

## 🛠️ 4. 기술 스택 (Tech Stack)

| 구성 요소 | 사용 기술 | 비고 |
|---|---|---|
| **Data Ingestion** | Apache Kafka | 로그 데이터 수집 |
| **Data Lake** | AWS S3 | 원본 로그(Raw data) 적재 |
| **Data Processing** | Apache Spark (Batch, Streaming) | ETL 및 실시간 집계 |
| **Data Warehouse** | PostgreSQL / Amazon Redshift / BigQuery | 정제된 데이터 및 Fact/Dimension 테이블 적재 |
| **Real-time DB** | Redis / MongoDB | 실시간 집계 결과 저장 (Optional) |
| **Orchestration** | Apache Airflow / Crontab | 배치 작업 스케줄링 (Optional) |
| **Dashboard (Batch)** | Apache Superset / Metabase | D.M 데이터 시각화 |
| **Dashboard (Real-time)** | Grafana / Streamlit | 실시간 지표 시각화 (Optional) |
| **Infra** | Docker, Docker-Compose | 로컬 개발 환경 구성 |
| **Version Control** | Git, GitHub | 소스 코드 관리 및 협업 |

---

## 🚀 5. 실행 가이드 (Quick Start)

### 사전 요구사항

- Docker 및 Docker Compose 설치
- 최소 8GB 메모리 권장
- 사용 포트:
  - `8080`: Airflow Webserver
  - `8501`: Streamlit Dashboard
  - `5432`: PostgreSQL
  - `9092`: Kafka
  - `2181`: Zookeeper

### 전체 파이프라인 실행 (권장)

한 번의 명령으로 배치 + 실시간 파이프라인 모두 실행:

```bash
# 1. Docker Compose로 전체 시스템 시작
docker-compose up -d

# 2. 서비스 상태 확인
docker-compose ps

# 3. 로그 확인 (각각 별도 터미널에서)
docker-compose logs -f spark-streaming  # 실시간 처리 로그
docker-compose logs -f kafka-producer   # 데이터 생성 로그
docker-compose logs -f streamlit        # 대시보드 로그
docker-compose logs -f airflow-scheduler # 배치 스케줄러 로그
```

### 서비스 접속

- **Airflow** (배치 파이프라인 관리): http://localhost:8080
  - Username: `admin`
  - Password: `admin`
  - DAG: `zigbang_daily_batch` 활성화하여 배치 실행

- **Streamlit Dashboard** (분석 결과 조회): http://localhost:8501
  - 5개 대시보드 페이지:
    1. 지역별 검색량 대시보드
    2. 시간대별 활동 분석 대시보드
    3. 실시간 스트리밍 대시보드
    4. 세션 분석 대시보드
    5. Funnel 분석 대시보드

### 서비스 관리

```bash
# 특정 서비스만 재시작
docker-compose restart spark-streaming
docker-compose restart kafka-producer

# 전체 서비스 중지
docker-compose down

# 전체 서비스 중지 및 볼륨 삭제 (데이터 초기화)
docker-compose down -v
```

### 로컬 개발 환경 (Docker 없이)

```bash
# 1. Python 가상환경 설정
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# 2. 의존성 설치
pip install pyspark==3.4.4 kafka-python boto3 psycopg2-binary streamlit pandas plotly

# 3. 인프라만 Docker로 시작
docker-compose up -d postgres kafka zookeeper

# 4. 배치 파이프라인 실행 (특정 날짜)
PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python \
  .venv/bin/python src/batch/pipeline.py 2025-10-29

# 5. 실시간 처리 실행
python src/streaming/kafka_producer.py  # 터미널 1
.venv/bin/python src/streaming/streaming_job.py  # 터미널 2

# 6. 대시보드 실행
cd src/streamlit_app && ../../.venv/bin/streamlit run Home.py
```

---

## 📁 6. 디렉토리 구조 (Directory Structure)

```
log-pipeline/
├── README.md                           # 프로젝트 설명서
├── CLAUDE.md                           # Claude Code용 프로젝트 가이드
├── docker-compose.yml                  # Docker Compose 설정 (전체 인프라)
├── Dockerfile                          # Airflow/Spark용 Dockerfile
├── Dockerfile.streamlit                # Streamlit 대시보드용 Dockerfile
├── .env                                # 환경 변수 설정
├── init-analytics-db.sql               # PostgreSQL 데이터베이스 초기화 스크립트
├── init_tables.sql                     # 테이블 생성 스크립트
│
├── dags/                               # Airflow DAG 파일들
│   └── daily_batch.py                  # 일일 배치 파이프라인 DAG
│
├── src/                                # 소스 코드
│   ├── config/                         # 설정 파일
│   │   └── config.py                   # 전역 설정 (S3, PostgreSQL, Kafka, Spark)
│   │
│   ├── batch/                          # 배치 파이프라인 (Spark Batch)
│   │   ├── pipeline.py                 # 배치 파이프라인 메인 오케스트레이션
│   │   ├── extract.py                  # 데이터 추출 (S3 또는 로컬)
│   │   ├── transform.py                # 데이터 변환 및 집계 (Spark)
│   │   └── load.py                     # 데이터 적재 (PostgreSQL)
│   │
│   ├── streaming/                      # 실시간 스트리밍 파이프라인
│   │   ├── kafka_producer.py           # Kafka 로그 생성기 (500 logs/sec)
│   │   ├── kafka_to_s3_consumer.py     # Kafka → S3/로컬 Consumer
│   │   └── streaming_job.py            # Spark Structured Streaming 작업
│   │
│   ├── streamlit_app/                  # Streamlit 대시보드 애플리케이션
│   │   ├── Home.py                     # 대시보드 홈 페이지
│   │   ├── utils.py                    # 공통 유틸리티 함수
│   │   └── pages/                      # 대시보드 페이지들
│   │       ├── 1_지역별 검색량 대시보드.py
│   │       ├── 2_시간대별 활동 분석 대시보드.py
│   │       ├── 3_실시간 스트리밍 대시보드.py
│   │       ├── 4_세션 분석 대시보드.py
│   │       └── 5_Funnel 분석 대시보드.py
│   │
│   └── utils/                          # 유틸리티 스크립트
│       └── change_log_dates.py         # 로그 날짜 변경 스크립트
│
├── data/                               # 로컬 데이터 저장소 (Data Lake)
│   └── date=YYYY-MM-DD/                # 날짜별 파티션
│       └── *.csv.gz                    # 압축된 로그 파일
│
├── results/                            # 배치 처리 결과 (중간 산출물)
│   ├── silver/                         # 정제된 데이터
│   └── gold/                           # 집계된 데이터
│
├── test_postgres.py                    # PostgreSQL 연결 테스트
├── test_spark_postgres.py              # Spark-PostgreSQL 통합 테스트
├── generate_synthetic_data.py          # 합성 데이터 생성기
└── generate_streaming_data.py          # 스트리밍 데이터 생성기
```
