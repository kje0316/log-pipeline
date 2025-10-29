import sys
import os

# Docker 컨테이너 내부 경로 또는 로컬 경로 자동 감지
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.config.config import Config
from src.batch.extract import extract_data_from_s3
from src.batch.transform import run_all_transforms
from src.batch.load import load_all_results

def run_etl_pipeline(date):
    """
    전체 ETL 파이프라인 실행

    Args:
        date: 처리한 날짜 (YYYY-MM-DD)
    """
    
    print(f"\n{'='*70}")
    print(f"🚀 배치 ETL 파이프라인 시작: {date}")
    print(f"{'='*70}\n")
    
    # Spark 세션 생성
    spark = Config.get_spark_session(f"log_batch_etl_{date}")    
    
    try:
        # 1. Extract: 데이터 읽기
        df = extract_data_from_s3(spark, date, Config.get_data_path())
        
        # 2. Transform: 데이터 변환/집계 
        results = run_all_transforms(df)
        
        # 3. Load: PostgreSQL에 저장
        jdbc_properties = {
            "user": Config.POSTGRES_USER,
            "password": Config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }
        load_all_results(results, Config.POSTGRES_JDBC_URL, jdbc_properties, date)
        
        print(f"\n{'='*70}")
        print(f"✅ 배치 ETL 파이프라인 완료: {date}")
        print(f"{'='*70}\n")
        
        return True
        
    except Exception as e:
        print(f"\n❌ 파이프라인 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()



if __name__ == "__main__":
    # 커맨드 라인 인자로 날짜 받기
    if len(sys.argv) > 1:
        date = sys.argv[1]
    else:
        date = "2018-01-26"
    
    success = run_etl_pipeline(date)
    sys.exit(0 if success else 1)