# src/config/config.py
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

class Config:
    # 개발/프로덕션 모드 전환
    LOCAL_MODE = os.getenv('LOCAL_MODE', 'true').lower() == 'true'

    # 로컬 데이터 경로 (Docker 환경 자동 감지)
    IS_DOCKER = os.path.exists('/opt/airflow')
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    LOCAL_RAW_PATH = '/opt/airflow/data/' if IS_DOCKER else os.path.join(PROJECT_ROOT, 'data')
    LOCAL_RESULTS_PATH = '/opt/airflow/results/' if IS_DOCKER else os.path.join(PROJECT_ROOT, 'results')

    # AWS 설정
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', '')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
    AWS_REGION = os.getenv('AWS_REGION', 'ap-northeast-2')

    # S3 경로
    S3_BUCKET = os.getenv('S3_BUCKET', 's3-flowops')
    S3_RAW_PATH = f's3a://{S3_BUCKET}/raw/'
    S3_RESULTS_PATH = f's3a://{S3_BUCKET}/batch-results/'

    @classmethod
    def get_data_path(cls):
        """현재 모드에 따라 데이터 경로 반환"""
        if cls.LOCAL_MODE:
            return cls.LOCAL_RAW_PATH
        else:
            return cls.S3_RAW_PATH

    @classmethod
    def get_results_path(cls):
        """현재 모드에 따라 결과 저장 경로 반환"""
        if cls.LOCAL_MODE:
            return cls.LOCAL_RESULTS_PATH
        else:
            return cls.S3_RESULTS_PATH
    
    # PostgreSQL 설정
    POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')  # Docker: 'postgres', 로컬: 'localhost'
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
    POSTGRES_DB = os.getenv('POSTGRES_DB', 'analytics')
    POSTGRES_USER = os.getenv('POSTGRES_USER', 'zigbang')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
    POSTGRES_JDBC_URL = f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    
    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC = 'zigbang_logs'
    KAFKA_PRODUCER_RATE = 500  # 초당 로그 생성 개수
    KAFKA_CONSUMER_BATCH_SIZE = 1000  # 배치 크기
    KAFKA_CONSUMER_FLUSH_INTERVAL = 60  # 플러시 간격(초)

    # Spark 설정
    SPARK_DRIVER_MEMORY = '4g'
    SPARK_EXECUTOR_MEMORY = '4g'
    
    @staticmethod
    def get_spark_session(app_name):
        from pyspark.sql import SparkSession
        
        # 기존 세션 종료
        try:
            SparkSession.getActiveSession().stop()
        except:
            pass
        
        # 환경변수 설정
        os.environ['AWS_ACCESS_KEY_ID'] = Config.AWS_ACCESS_KEY
        os.environ['AWS_SECRET_ACCESS_KEY'] = Config.AWS_SECRET_KEY
        
        return SparkSession.builder \
            .appName(app_name) \
            .master("local[4]") \
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                    "org.postgresql:postgresql:42.5.0") \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{Config.AWS_REGION}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.driver.memory", Config.SPARK_DRIVER_MEMORY) \
            .config("spark.default.parallelism", "8") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()