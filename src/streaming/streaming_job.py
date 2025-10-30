#!/usr/bin/env python3
"""Spark Structured Streaming Job - Real-time aggregation"""
import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, approx_count_distinct,
    to_json, expr, lit, max, min, avg, sum, when
)

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.config.config import Config


def parse_log_line(df):
    """Parse pipe-delimited log line"""
    df = df.selectExpr("CAST(value AS STRING)")

    split_cols = [
        "split(value, '\\\\|')[0] as timestamp_ms",
        "split(value, '\\\\|')[1] as user_id",
        "split(value, '\\\\|')[2] as session_id",
        "split(value, '\\\\|')[3] as event_type",
        "split(value, '\\\\|')[4] as user_agent",
        "split(value, '\\\\|')[5] as ip_address",
        "split(value, '\\\\|')[6] as hash_value",
        "split(value, '\\\\|')[7] as timestamp_ms_2",
        "split(value, '\\\\|')[8] as datetime_string",
        "split(value, '\\\\|')[9] as json_data"
    ]

    df = df.selectExpr(*split_cols)
    df = df.withColumn("timestamp", (col("timestamp_ms").cast("bigint") / 1000).cast("timestamp"))

    return df


def aggregate_streaming_data(df):
    """Aggregate data with 5-minute tumbling windows"""
    windowed_df = df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "5 minutes")
        ) \
        .agg(
            count("*").alias("total_events"),
            approx_count_distinct("user_id").alias("active_users")
        )

    windowed_df = windowed_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_events"),
        col("active_users"),
        lit(None).cast("string").alias("top_event_types"),
        lit(None).cast("string").alias("top_screens")
    )

    return windowed_df


def aggregate_session_metrics(df):
    """Aggregate session-level metrics in 5-minute windows"""
    # Session-level aggregation first
    session_df = df \
        .groupBy("session_id", "user_id") \
        .agg(
            count("*").alias("events_per_session"),
            approx_count_distinct("event_type").alias("unique_events"),
            min("timestamp").alias("session_start"),
            max("timestamp").alias("session_end")
        ) \
        .withColumn(
            "session_duration_sec",
            (col("session_end").cast("long") - col("session_start").cast("long"))
        ) \
        .withColumn(
            "is_bounce",
            when(col("events_per_session") == 1, 1).otherwise(0)
        )

    # Window aggregation on sessions
    windowed_sessions = session_df \
        .withWatermark("session_start", "10 seconds") \
        .groupBy(
            window(col("session_start"), "5 minutes")
        ) \
        .agg(
            count("session_id").alias("total_sessions"),
            avg("events_per_session").alias("avg_events_per_session"),
            avg("session_duration_sec").alias("avg_session_duration_sec"),
            (sum("is_bounce") / count("session_id") * 100).alias("bounce_rate_pct"),
            approx_count_distinct("user_id").alias("unique_users")
        )

    windowed_sessions = windowed_sessions.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_sessions"),
        col("avg_events_per_session"),
        col("avg_session_duration_sec"),
        col("bounce_rate_pct"),
        col("unique_users")
    )

    return windowed_sessions


def aggregate_funnel_metrics(df):
    """Real-time funnel analysis in 5-minute windows (4단계: 검색 → 상세 → 관심 → 구매)"""
    # Classify events into funnel stages (4단계)
    funnel_df = df.withColumn(
        "funnel_stage",
        when(col("event_type").isin(["view_search_results", "oneroom_filter_results", "apt_filter_results"]), "1_search")
        .when(col("event_type") == "view_item", "2_detail")
        .when(col("event_type").isin(["add_to_wishlist", "add_to_cart"]), "3_interest")
        .when(col("event_type").isin(["begin_checkout", "purchase"]), "4_purchase")
        .otherwise(None)
    ).filter(col("funnel_stage").isNotNull())

    # Session-level funnel progression (4단계)
    session_funnel = funnel_df \
        .groupBy("session_id") \
        .agg(
            max(when(col("funnel_stage") == "1_search", 1).otherwise(0)).alias("reached_search"),
            max(when(col("funnel_stage") == "2_detail", 1).otherwise(0)).alias("reached_detail"),
            max(when(col("funnel_stage") == "3_interest", 1).otherwise(0)).alias("reached_interest"),
            max(when(col("funnel_stage") == "4_purchase", 1).otherwise(0)).alias("reached_purchase"),
            min("timestamp").alias("session_start")
        )

    # Window aggregation on funnel sessions (순차적 전환 추적)
    windowed_funnel = session_funnel \
        .withWatermark("session_start", "10 seconds") \
        .groupBy(
            window(col("session_start"), "5 minutes")
        ) \
        .agg(
            count("session_id").alias("total_sessions"),
            sum("reached_search").alias("search_count"),
            sum("reached_detail").alias("detail_count"),
            sum("reached_interest").alias("interest_count"),
            sum("reached_purchase").alias("purchase_count"),
            # 순차적 전환 카운트
            sum(when((col("reached_search") == 1) & (col("reached_detail") == 1), 1).otherwise(0)).alias("search_and_detail"),
            sum(when((col("reached_detail") == 1) & (col("reached_interest") == 1), 1).otherwise(0)).alias("detail_and_interest"),
            sum(when((col("reached_interest") == 1) & (col("reached_purchase") == 1), 1).otherwise(0)).alias("interest_and_purchase"),
            sum(when((col("reached_search") == 1) & (col("reached_purchase") == 1), 1).otherwise(0)).alias("search_and_purchase")
        ) \
        .withColumn(
            "search_to_detail_rate",
            when(col("search_count") > 0, (col("search_and_detail") / col("search_count") * 100)).otherwise(0)
        ) \
        .withColumn(
            "detail_to_interest_rate",
            when(col("detail_count") > 0, (col("detail_and_interest") / col("detail_count") * 100)).otherwise(0)
        ) \
        .withColumn(
            "interest_to_purchase_rate",
            when(col("interest_count") > 0, (col("interest_and_purchase") / col("interest_count") * 100)).otherwise(0)
        ) \
        .withColumn(
            "overall_conversion_rate",
            when(col("search_count") > 0, (col("search_and_purchase") / col("search_count") * 100)).otherwise(0)
        )

    windowed_funnel = windowed_funnel.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_sessions"),
        col("search_count"),
        col("detail_count"),
        col("interest_count"),
        col("purchase_count"),
        col("search_to_detail_rate"),
        col("detail_to_interest_rate"),
        col("interest_to_purchase_rate"),
        col("overall_conversion_rate")
    )

    return windowed_funnel


def write_to_postgres(df, jdbc_url, properties, table_name, checkpoint_path):
    """Write aggregated results to PostgreSQL"""
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id:
            batch_df.write.jdbc(
                jdbc_url,
                table_name,
                mode="append",
                properties=properties
            )
        ) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime='30 seconds')

    return query


def main():
    parser = argparse.ArgumentParser(description='Spark Streaming Job')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', type=str, default='zigbang_logs', help='Kafka topic name')
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Spark Structured Streaming Starting")
    print(f"{'='*60}")
    print(f"  Kafka: {args.bootstrap_servers}")
    print(f"  Topic: {args.topic}")
    print(f"  Window: 5 minutes (tumbling)")
    print(f"  Watermark: 10 seconds")
    print(f"{'='*60}\n")

    spark = SparkSession.builder \
        .appName("ZigbangStreamingJob") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,"
                "org.postgresql:postgresql:42.5.0") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.bootstrap_servers) \
            .option("subscribe", args.topic) \
            .option("startingOffsets", "latest") \
            .load()

        print("✅ Kafka stream connected\n")

        parsed_df = parse_log_line(kafka_df)
        print("✅ Log parsing configured\n")

        # Basic aggregation
        aggregated_df = aggregate_streaming_data(parsed_df)
        print("✅ Basic aggregation logic configured")

        # # Session metrics aggregation
        # session_metrics_df = aggregate_session_metrics(parsed_df)
        # print("✅ Session metrics aggregation configured")

        # # Funnel metrics aggregation
        # funnel_metrics_df = aggregate_funnel_metrics(parsed_df)
        # print("✅ Funnel metrics aggregation configured\n")

        jdbc_properties = {
            "user": Config.POSTGRES_USER,
            "password": Config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # Start 1 streaming query (basic aggregation only)
        query1 = write_to_postgres(
            aggregated_df,
            Config.POSTGRES_JDBC_URL,
            jdbc_properties,
            "streaming_stats",
            "/tmp/streaming_checkpoint_basic"
        )

        # query2 = write_to_postgres(
        #     session_metrics_df,
        #     Config.POSTGRES_JDBC_URL,
        #     jdbc_properties,
        #     "streaming_session_metrics",
        #     "/tmp/streaming_checkpoint_session"
        # )

        # query3 = write_to_postgres(
        #     funnel_metrics_df,
        #     Config.POSTGRES_JDBC_URL,
        #     jdbc_properties,
        #     "streaming_funnel_metrics",
        #     "/tmp/streaming_checkpoint_funnel"
        # )

        print("✅ PostgreSQL write configured (1 stream)")
        print("   - streaming_stats\n")
        print("🚀 Streaming started... (Ctrl+C to stop)\n")

        streaming_query1 = query1.start()
        # streaming_query2 = query2.start()
        # streaming_query3 = query3.start()

        streaming_query1.awaitTermination()

    except KeyboardInterrupt:
        print(f"\n\n{'='*60}")
        print("⏹  Stop requested")
        print(f"{'='*60}")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\n✅ Streaming job stopped\n")


if __name__ == "__main__":
    main()
