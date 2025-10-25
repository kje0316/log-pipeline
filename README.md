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

## 📁 5. 디렉토리 구조 (Directory Structure)
