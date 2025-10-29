#!/usr/bin/env python3
"""
로그 데이터의 날짜를 2018년에서 2025년으로 변경하는 스크립트
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, expr
import os
import sys

# 프로젝트 루트 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.config.config import Config

# 날짜 매핑 (2018 → 2025)
DATE_MAPPINGS = {
    "2018-01-26": "2025-10-26",
    "2018-01-27": "2025-10-27",
    "2018-01-28": "2025-10-28",
    "2018-01-29": "2025-10-29"
}

# 밀리초 차이 계산 (2018-01-26 → 2025-10-26 = 2838일)
# 2838일 * 24시간 * 3600초 * 1000밀리초 = 245,203,200,000 밀리초
MS_OFFSET_PER_DAY = {
    "2018-01-26": 245203200000,  # +2838일
    "2018-01-27": 245203200000,  # +2838일
    "2018-01-28": 245203200000,  # +2838일
    "2018-01-29": 245203200000,  # +2838일
}


def change_date_for_partition(spark, old_date, new_date, ms_offset):
    """
    특정 날짜 파티션의 데이터를 읽어서 날짜를 변경하고 저장

    Args:
        spark: SparkSession
        old_date: 기존 날짜 (YYYY-MM-DD)
        new_date: 새 날짜 (YYYY-MM-DD)
        ms_offset: 밀리초 오프셋
    """
    print(f"\n{'='*60}")
    print(f"처리 중: {old_date} → {new_date}")
    print(f"{'='*60}\n")

    # 기존 데이터 경로
    old_path = f"{Config.LOCAL_RAW_PATH}date={old_date}"

    # CSV 읽기 (파이프 구분자)
    print(f"📥 읽기: {old_path}")
    df = spark.read.option("delimiter", "|").csv(old_path)

    original_count = df.count()
    print(f"   총 {original_count:,}건")

    # 컬럼명 지정
    df = df.toDF(
        "timestamp_ms",      # 0
        "user_id",           # 1
        "session_id",        # 2
        "event_type",        # 3
        "user_agent",        # 4
        "ip_address",        # 5
        "hash_value",        # 6
        "timestamp_ms_2",    # 7
        "datetime_string",   # 8
        "json_data"          # 9
    )

    # 날짜 변경
    print(f"🔄 날짜 변경 중...")

    # 1. timestamp_ms 변경 (밀리초 더하기)
    df_changed = df.withColumn(
        "timestamp_ms",
        (col("timestamp_ms").cast("bigint") + ms_offset).cast("string")
    )

    # 2. timestamp_ms_2도 변경 (같은 방식)
    df_changed = df_changed.withColumn(
        "timestamp_ms_2",
        (col("timestamp_ms_2").cast("bigint") + ms_offset).cast("string")
    )

    # 3. datetime_string 변경 (문자열 치환)
    df_changed = df_changed.withColumn(
        "datetime_string",
        regexp_replace(col("datetime_string"), old_date, new_date)
    )

    # 파이프 구분자로 결합
    df_output = df_changed.selectExpr(
        "concat_ws('|', " +
        "timestamp_ms, user_id, session_id, event_type, user_agent, " +
        "ip_address, hash_value, timestamp_ms_2, datetime_string, json_data) as value"
    )

    # 새 경로에 저장
    new_path = f"{Config.LOCAL_RAW_PATH}date={new_date}"
    print(f"💾 저장: {new_path}")

    # CSV.GZ로 저장 (기존과 동일한 포맷)
    df_output.write.mode("overwrite").text(new_path, compression="gzip")

    # .txt.gz 파일을 .csv.gz로 rename (나중에 처리)
    print(f"   ✅ 저장 완료\n")

    return original_count


def main():
    """메인 함수"""
    print("\n" + "="*60)
    print("로그 데이터 날짜 변경 스크립트 (2018 → 2025)")
    print("="*60 + "\n")

    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("ChangeDates_2018_to_2025") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    try:
        total_processed = 0

        # 각 날짜에 대해 처리
        for old_date, new_date in DATE_MAPPINGS.items():
            ms_offset = MS_OFFSET_PER_DAY[old_date]
            count = change_date_for_partition(spark, old_date, new_date, ms_offset)
            total_processed += count

        print("\n" + "="*60)
        print(f"✅ 완료! 총 {total_processed:,}건 처리됨")
        print("="*60 + "\n")

        # 파일 확장자 변경 안내
        print("⚠️  참고: 저장된 파일이 .txt.gz로 되어 있을 수 있습니다.")
        print("    필요시 .csv.gz로 rename 해주세요.\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
