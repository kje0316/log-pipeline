# src/batch/extract.py
from pyspark.sql.types import *
from pyspark.sql.functions import *

def parse_log_line(line):
    """로그 라인 파싱"""
    try:
        fields = line.split('|')
        if len(fields) >= 10:
            return (
                fields[0],  # timestamp_ms
                fields[1],  # user_id
                fields[2],  # session_id
                fields[3],  # event_type
                fields[4],  # user_agent
                fields[5],  # ip_address
                fields[8],  # datetime_string
                fields[9]   # json_data
            )
    except:
        return None

def extract_data_from_s3(spark, date, s3_path):
    """
    S3에서 특정 날짜의 원본 로그 데이터 읽기
    
    Args:
        spark: SparkSession
        date: 날짜 (YYYY-MM-DD)
        s3_path: S3 기본 경로
        
    Returns:
        DataFrame: 파싱된 로그 데이터
    """
    print(f"\n{'='*60}")
    print(f"📥 [EXTRACT] S3 데이터 읽기 시작: {date}")
    print(f"{'='*60}\n")
    
    # S3 경로
    full_path = f"{s3_path}date={date}/*.gz"
    print(f"경로: {full_path}")
    
    # RDD로 읽기
    rdd = spark.sparkContext.textFile(full_path)
    total_count = rdd.count()
    print(f"원본 레코드 수: {total_count:,}건\n")
    
    # 파싱
    print("🔄 로그 파싱 중...")
    parsed_rdd = rdd.map(parse_log_line).filter(lambda x: x is not None)
    
    # 스키마 정의
    schema = StructType([
        StructField("timestamp_ms", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("event_type", StringType()),
        StructField("user_agent", StringType()),
        StructField("ip_address", StringType()),
        StructField("datetime_string", StringType()),
        StructField("json_data", StringType())
    ])
    
    # DataFrame 생성
    df = spark.createDataFrame(parsed_rdd, schema)

    # JSON 필드 추출 (지역 분석용)
    df = df.withColumn("local1", get_json_object(col("json_data"), "$.local1")) \
           .withColumn("local2", get_json_object(col("json_data"), "$.local2")) \
           .withColumn("room_type", get_json_object(col("json_data"), "$.room_type")) \
           .withColumn("deal_type", get_json_object(col("json_data"), "$.deal_type")) \
           .withColumn("screen_name", get_json_object(col("json_data"), "$.screen_name"))

    # 타임스탬프 변환 (밀리초 포함 처리)
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("datetime_string"), "yyyy-MM-dd HH:mm:ss.SSS")
    )

    # timestamp가 null인 경우 밀리초 없는 형식으로 재시도
    df = df.withColumn(
        "timestamp",
        when(col("timestamp").isNull(),
             to_timestamp(col("datetime_string"), "yyyy-MM-dd HH:mm:ss")
        ).otherwise(col("timestamp"))
    ).withColumn(
        "date", lit(date)
    ).withColumn(
        "hour", hour(col("timestamp"))
    ).withColumn(
        "day_of_week", dayofweek(col("timestamp"))
    )
    
    parsed_count = df.count()
    print(f"파싱 성공: {parsed_count:,}건")
    print(f"파싱 실패: {total_count - parsed_count:,}건")
    
    print(f"\n✅ [EXTRACT] 완료\n")
    
    return df

if __name__ == "__main__":
    import sys
    import os

    # Docker 컨테이너 내부 경로 또는 로컬 경로 자동 감지
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.append(project_root)

    from src.config.config import Config

    # 커맨드라인 인자로 날짜 받기
    if len(sys.argv) > 1:
        date = sys.argv[1]
    else:
        date = "2018-01-26"

    print(f"\n🔧 [EXTRACT] 실행 날짜: {date}")
    print(f"📁 데이터 경로: {Config.get_data_path()}\n")

    spark = Config.get_spark_session(f"extract_{date}")

    try:
        # Extract: 데이터 읽기
        df = extract_data_from_s3(spark, date, Config.get_data_path())

        # Silver layer로 저장 (Parquet)
        silver_path = f"{Config.get_results_path()}silver/date={date}"
        print(f"\n💾 Silver layer 저장 중: {silver_path}")
        df.write.mode("overwrite").parquet(silver_path)
        print(f"✅ Silver layer 저장 완료\n")

        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Extract 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()