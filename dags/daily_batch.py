from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Spark가 사용할 드라이버 (S3, Postgres)
# (Dockerfile에서 이미 pip install 했지만, spark-submit에 패키지를 명시)
JARS = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0"

# Airflow 컨테이너 내부의 스크립트 경로
BASE_SCRIPT_PATH = "/opt/airflow/src/batch"

with DAG(
    dag_id='zigbang_daily_batch',
    start_date=datetime(2025, 10, 26),  # 데이터 시작일 (2025년으로 변경)
    # end_date=datetime(2025, 10, 29),    # 데이터 종료일 (10/26-10/29 4일치)
    schedule_interval='@daily',         # 매일 실행
    catchup=True,                       # 밀린 작업(Backfill) 모두 실행
    max_active_runs=1,                  # 동시 실행 방지
    tags=['zigbang_log', 'batch']
) as dag:

    # 1. Extract Task
    extract_job = BashOperator(
        task_id='extract',
        bash_command=f"""
        spark-submit \
            --packages {JARS} \
            {BASE_SCRIPT_PATH}/extract.py {{{{ ds }}}}
        """
    )

    # 2. Transform Task
    transform_job = BashOperator(
        task_id='transform',
        bash_command=f"""
        spark-submit \
            --packages {JARS} \
            {BASE_SCRIPT_PATH}/transform.py {{{{ ds }}}}
        """
    )

    # 3. Load Task
    load_job = BashOperator(
        task_id='load',
        bash_command=f"""
        spark-submit \
            --packages {JARS} \
            {BASE_SCRIPT_PATH}/load.py {{{{ ds }}}}
        """
    )
    
    # 4. ⭐️ 작업 흐름(종속성) 정의
    # extract가 성공하면 transform을, transform이 성공하면 load를 실행
    extract_job >> transform_job >> load_job