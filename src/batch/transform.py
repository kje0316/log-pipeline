from pyspark.sql.functions import *
from pyspark.sql.window import Window

def apply_data_cleaning(df):
    """데이터 클리닝: 지역명 표준화 및 시간 파생 컬럼 생성"""
    print("\n🧹 [CLEAN] 데이터 클리닝 시작...")

    # 지역명 표준화 매핑
    local1_mapping = {
        "서울": "서울특별시", "서울시": "서울특별시", "서울특별시": "서울특별시",
        "부산": "부산광역시", "부산시": "부산광역시", "부산광역시": "부산광역시",
        "대구": "대구광역시", "대구시": "대구광역시", "대구광역시": "대구광역시",
        "인천": "인천광역시", "인천시": "인천광역시", "인천광역시": "인천광역시",
        "광주": "광주광역시", "광주시": "광주광역시", "광주광역시": "광주광역시",
        "대전": "대전광역시", "대전시": "대전광역시", "대전광역시": "대전광역시",
        "울산": "울산광역시", "울산시": "울산광역시", "울산광역시": "울산광역시",
        "세종": "세종특별자치시", "세종시": "세종특별자치시", "세종특별자치시": "세종특별자치시",
        "경기": "경기도", "경기도": "경기도",
        "강원": "강원도", "강원도": "강원도",
        "충북": "충청북도", "충청북도": "충청북도",
        "충남": "충청남도", "충청남도": "충청남도",
        "전북": "전라북도", "전라북도": "전라북도",
        "전남": "전라남도", "전라남도": "전라남도",
        "경북": "경상북도", "경상북도": "경상북도",
        "경남": "경상남도", "경상남도": "경상남도",
        "제주": "제주특별자치도", "제주도": "제주특별자치도", "제주특별자치도": "제주특별자치도"
    }

    # 지역명 표준화
    result_col = when((col("local1") == "NULL") | (col("local1").isNull()), lit(None))
    for original, standardized in local1_mapping.items():
        result_col = result_col.when(trim(col("local1")) == original, lit(standardized))
    result_col = result_col.otherwise(trim(col("local1")))

    # 시간 파생 컬럼 생성
    df_cleaned = df \
        .withColumn("log_time_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("search_hour", hour(col("log_time_ts"))) \
        .withColumn("is_weekend", when(dayofweek(col("log_time_ts")).isin([1, 7]), lit("주말")).otherwise(lit("평일"))) \
        .withColumn("local1_std", result_col) \
        .withColumn("local1_original", col("local1")) \
        .drop("local1") \
        .withColumnRenamed("local1_std", "local1") \
        .drop("log_time_ts")

    print("   ✅ 지역명 표준화 및 시간 파생 컬럼 생성 완료")
    return df_cleaned

def transform_hourly_stats(df):
    """시간대별 통계 집계"""
    print("\n📊 [TRANSFORM] 시간대별 통계 집계 중...")
    
    hourly_stats = df.groupBy("date", "hour").agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("active_users"),
        countDistinct("session_id").alias("active_sessions"),
        countDistinct("ip_address").alias("unique_ips")
    ).orderBy("hour").withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: {hourly_stats.count()}개 시간대")

    return hourly_stats


def transform_user_stats(df):
    """유저별 통계 집계"""
    print("\n👥 [TRANSFORM] 유저별 통계 집계 중...")
    
    user_stats = df.groupBy("date", "user_id").agg(
        count("*").alias("total_actions"),
        countDistinct("session_id").alias("session_count"),
        countDistinct("event_type").alias("unique_event_types"),
        min("timestamp").alias("first_action"),
        max("timestamp").alias("last_action")
    ).withColumn(
        "session_duration_sec",
        unix_timestamp("last_action") - unix_timestamp("first_action")
    ).withColumn(
        "avg_actions_per_session",
        col("total_actions") / col("session_count")
    ).withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: {user_stats.count():,}명 유저")

    return user_stats


def transform_event_stats(df):
    """이벤트 타입별 통계 집계"""
    print("\n📱 [TRANSFORM] 이벤트별 통계 집계 중...")
    
    event_stats = df.groupBy("date", "event_type").agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions")
    ).withColumn(
        "avg_events_per_user",
        col("event_count") / col("unique_users")
    ).orderBy(desc("event_count")).withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: {event_stats.count()}개 이벤트 타입")

    return event_stats

def transform_top_users(df, top_n=100):
    """가장 활발한 유저 Top N"""
    print(f"\n🏆 [TRANSFORM] Top {top_n} 활성 유저 집계 중...")
    
    top_users = df.groupBy("date", "user_id").agg(
        count("*").alias("total_actions")
    ).withColumn(
        "rank",
        row_number().over(Window.partitionBy("date").orderBy(desc("total_actions")))
    ).filter(col("rank") <= top_n).withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: Top {top_n} 유저")

    return top_users


def transform_daily_summary(hourly_stats, user_stats, event_stats):
    """일별 요약 통계"""
    print("\n📈 [TRANSFORM] 일별 요약 통계 생성 중...")

    # 시간대별 통계에서 일별 합계
    daily = hourly_stats.groupBy("date").agg(
        sum("total_events").alias("daily_total_events"),
        sum("active_users").alias("daily_total_users"),
        sum("active_sessions").alias("daily_total_sessions"),
        avg("active_users").alias("avg_hourly_users")
    ).withColumn(
        "date", to_date(col("date"))
    )

    print("   생성 완료")

    return daily


# ========================================
# 지역 분석 관련 Transform Functions
# ========================================

def transform_regional_search(df):
    """1. 지역별 검색량 집계"""
    print("\n🗺️  [TRANSFORM] 지역별 검색량 집계 중...")

    regional_search = df.groupBy("date", "local1", "local2").agg(
        count("*").alias("total_searches")
    ).orderBy(col("local1").desc(), col("total_searches").desc()).withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: {regional_search.count()}개 지역")
    return regional_search


def transform_room_preference(df):
    """2. 지역별 매물 유형 선호도 분석"""
    print("\n🏠 [TRANSFORM] 지역별 매물 유형 선호도 집계 중...")

    # NULL 값 제거
    df_filtered = df.filter(
        (col("local1").isNotNull()) &
        (col("local2").isNotNull()) &
        (col("room_type").isNotNull())
    )

    # 지역 + 매물 유형별 검색 수 집계
    df_room_counts = df_filtered.groupBy("date", "local1", "local2", "room_type").agg(
        count("*").alias("type_searches")
    )

    # 지역별 총 검색 수 집계
    df_total = df_room_counts.groupBy("date", "local1", "local2").agg(
        sum("type_searches").alias("total_regional_searches")
    )

    # 조인 및 선호도 비율 계산
    room_preference = df_room_counts.join(df_total, on=["date", "local1", "local2"], how="inner") \
        .withColumn("preference_rate", (col("type_searches") * 100.0 / col("total_regional_searches"))) \
        .orderBy("local1", "local2", col("preference_rate").desc()).withColumn(
            "date", to_date(col("date"))
        )

    print(f"   집계 완료: {room_preference.count()}개 지역-유형 조합")
    return room_preference


def transform_hourly_activity(df):
    """3. 지역별 시간대별 활동량 집계"""
    print("\n⏰ [TRANSFORM] 지역별 시간대별 활동량 집계 중...")

    # NULL 값 제거
    df_filtered = df.filter(
        (col("local1").isNotNull()) &
        (col("local2").isNotNull()) &
        (col("search_hour").isNotNull()) &
        (col("is_weekend").isNotNull())
    )

    # 집계
    df_grouped = df_filtered.groupBy("date", "local1", "local2", "is_weekend", "search_hour").agg(
        count("*").alias("hourly_activity_count")
    )

    # 활동량이 적은 그룹 제거 (count > 100)
    hourly_activity = df_grouped.filter(col("hourly_activity_count") > 100) \
        .orderBy("local1", "local2", "is_weekend", col("hourly_activity_count").desc()).withColumn(
            "date", to_date(col("date"))
        )

    print(f"   집계 완료: {hourly_activity.count()}개 시간대")
    return hourly_activity


def transform_engagement_index(df):
    """4. 지역별 사용자 몰입도 지수"""
    print("\n💡 [TRANSFORM] 지역별 사용자 몰입도 집계 중...")

    # 세션별 이벤트 수 집계
    session_event_counts = df.filter(
        (col("local2").isNotNull()) & (col("user_id").isNotNull())
    ).groupBy("date", "local1", "local2", "user_id").agg(
        count("*").alias("event_count_per_session")
    )

    # 지역별 통계
    engagement_index = session_event_counts.groupBy("date", "local1", "local2").agg(
        count("user_id").alias("total_sessions"),
        avg("event_count_per_session").alias("avg_events_per_session"),
        (sum(when(col("event_count_per_session") < 5, 1).otherwise(0)) * 100.0 / count("user_id")).alias("low_engagement_rate")
    ).filter(col("total_sessions") > 500) \
      .orderBy(col("avg_events_per_session").desc()).withColumn(
          "date", to_date(col("date"))
      )

    print(f"   집계 완료: {engagement_index.count()}개 지역")
    return engagement_index


def transform_daily_activity_peak(df):
    """5. 시간대별 전체 활동 집계"""
    print("\n📅 [TRANSFORM] 시간대별 전체 활동 집계 중...")

    # 유효한 시간 로그만 필터
    valid_logs = df.filter(col("search_hour").isNotNull())

    # 시간대별 집계
    daily_activity_peak = valid_logs.groupBy("date", "search_hour").agg(
        count("*").alias("hourly_activity_count"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("local1").alias("unique_local1"),
        countDistinct("local2").alias("unique_local2")
    ).orderBy(col("hourly_activity_count").desc()).withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: {daily_activity_peak.count()}개 시간대")
    return daily_activity_peak


def transform_hourly_deal(df):
    """6. 시간대별 거래 유형 선호도 변화"""
    print("\n💰 [TRANSFORM] 시간대별 거래 유형 선호도 집계 중...")

    # 필터: 유효한 시간, 거래 유형, 검색 활동 로그만
    filtered_logs = df.filter(
        (col("search_hour").isNotNull()) &
        (col("deal_type").isNotNull()) &
        (col("screen_name").like("%필터%"))
    )

    # 시간대 + 거래 유형별 집계
    hourly_deal = filtered_logs.groupBy("date", "search_hour", "deal_type", "screen_name").agg(
        count("*").alias("searches_by_deal_type"),
        countDistinct("user_id").alias("unique_users")
    ).orderBy(col("search_hour").asc(), col("searches_by_deal_type").desc()).withColumn(
        "date", to_date(col("date"))
    )

    print(f"   집계 완료: {hourly_deal.count()}개 시간대-거래 유형 조합")
    return hourly_deal


def transform_hourly_engagement(df):
    """7. 시간대별 사용자 몰입도 변화"""
    print("\n⚡ [TRANSFORM] 시간대별 사용자 몰입도 집계 중...")

    # 세션별, 시간대별 이벤트 수 집계
    hourly_event_counts = df.filter(
        (col("search_hour").isNotNull()) & (col("user_id").isNotNull())
    ).groupBy("date", "search_hour", "user_id").agg(
        count("*").alias("event_count_per_session")
    )

    # 시간대별 통계 계산
    hourly_engagement = hourly_event_counts.groupBy("date", "search_hour").agg(
        count("user_id").alias("total_sessions"),
        avg("event_count_per_session").alias("avg_events_per_session")
    ).filter(col("total_sessions") > 10) \
      .orderBy(col("avg_events_per_session").desc()).withColumn(
          "date", to_date(col("date"))
      )

    print(f"   집계 완료: {hourly_engagement.count()}개 시간대")
    return hourly_engagement


def transform_session_analysis(df):
    """세션 분석: 세션당 이벤트 수, 체류시간, 바운스율"""
    print("\n📊 [TRANSFORM] 세션 분석 집계 중...")

    # 세션별 집계
    session_stats = df.groupBy("date", "session_id", "user_id").agg(
        count("*").alias("events_per_session"),
        countDistinct("event_type").alias("unique_events"),
        (unix_timestamp(max("timestamp")) - unix_timestamp(min("timestamp"))).alias("session_duration_sec"),
        min("timestamp").alias("session_start"),
        max("timestamp").alias("session_end")
    )

    # 바운스 세션 식별 (1개 이벤트만 발생)
    session_stats = session_stats.withColumn(
        "is_bounce",
        when(col("events_per_session") == 1, 1).otherwise(0)
    )

    # 일별 세션 통계
    daily_session_stats = session_stats.groupBy("date").agg(
        count("session_id").alias("total_sessions"),
        avg("events_per_session").alias("avg_events_per_session"),
        avg("session_duration_sec").alias("avg_session_duration_sec"),
        (sum("is_bounce") / count("session_id") * 100).alias("bounce_rate_pct"),
        countDistinct("user_id").alias("unique_users")
    ).withColumn("date", to_date(col("date")))

    print(f"   집계 완료: {daily_session_stats.count()}일")
    return daily_session_stats


def transform_user_behavior_patterns(df):
    """사용자 행동 패턴 분석: 파워유저, 관심 지역, 매물 선호도"""
    print("\n👤 [TRANSFORM] 사용자 행동 패턴 분석 중...")

    # 사용자별 활동 집계
    user_behavior = df.groupBy("date", "user_id").agg(
        count("*").alias("total_events"),
        countDistinct("session_id").alias("session_count"),
        countDistinct("event_type").alias("unique_event_types"),
        countDistinct("local1").alias("interested_regions"),
        collect_set("room_type").alias("room_types_viewed"),
        (countDistinct(when(col("event_type") == "view_item", col("event_type"))) / count("*") * 100).alias("view_item_rate")
    )

    # 사용자 세그먼트 분류
    user_behavior = user_behavior.withColumn(
        "user_segment",
        when(col("total_events") >= 50, "파워유저")
        .when(col("total_events") >= 20, "활성유저")
        .when(col("total_events") >= 5, "일반유저")
        .otherwise("신규유저")
    )

    # 일별 사용자 세그먼트별 통계
    segment_stats = user_behavior.groupBy("date", "user_segment").agg(
        count("user_id").alias("user_count"),
        avg("total_events").alias("avg_events"),
        avg("session_count").alias("avg_sessions"),
        avg("interested_regions").alias("avg_regions")
    ).withColumn("date", to_date(col("date")))

    print(f"   집계 완료: {segment_stats.count()}개 세그먼트")
    return segment_stats


def transform_hourly_behavior_analysis(df):
    """시간대별 행동 분석: 피크 타임, 시간대별 전환율"""
    print("\n⏰ [TRANSFORM] 시간대별 행동 분석 중...")

    # 시간대별 이벤트 집계
    hourly_behavior = df.groupBy("date", "hour").agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("active_users"),
        countDistinct("session_id").alias("total_sessions"),
        (count(when(col("event_type").isin(["view_search_results", "oneroom_filter_results", "apt_filter_results"]), 1)) / count("*") * 100).alias("search_rate"),
        (count(when(col("event_type") == "view_item", 1)) / count("*") * 100).alias("view_rate"),
        (count(when(col("event_type").isin(["add_to_wishlist", "add_to_cart"]), 1)) / count("*") * 100).alias("interest_rate")
    )

    # 피크 타임 여부 (평균 대비 120% 이상)
    window_spec = Window.partitionBy("date")
    hourly_behavior = hourly_behavior.withColumn(
        "avg_events_per_hour", avg("total_events").over(window_spec)
    ).withColumn(
        "is_peak_time",
        when(col("total_events") >= col("avg_events_per_hour") * 1.2, 1).otherwise(0)
    ).drop("avg_events_per_hour") \
     .withColumn("date", to_date(col("date")))

    print(f"   집계 완료: {hourly_behavior.count()}개 시간대")
    return hourly_behavior


def transform_funnel_analysis(df):
    """Funnel 분석: 검색 → 상세 → 관심 → 구매 (4단계)"""
    print("\n🔍 [TRANSFORM] Funnel 분석 중...")

    # Funnel 단계별 이벤트 정의 (view_item_list 제외 - 실제 데이터에서 거의 사용되지 않음)
    funnel_events = df.withColumn(
        "funnel_stage",
        when(col("event_type").isin(["view_search_results", "oneroom_filter_results", "apt_filter_results"]), "1_search")
        .when(col("event_type") == "view_item", "2_detail")
        .when(col("event_type").isin(["add_to_wishlist", "add_to_cart"]), "3_interest")
        .when(col("event_type").isin(["begin_checkout", "purchase"]), "4_purchase")
        .otherwise(None)
    ).filter(col("funnel_stage").isNotNull())

    # 세션별 Funnel 단계 도달 여부
    session_funnel = funnel_events.groupBy("date", "session_id").agg(
        max(when(col("funnel_stage") == "1_search", 1).otherwise(0)).alias("reached_search"),
        max(when(col("funnel_stage") == "2_detail", 1).otherwise(0)).alias("reached_detail"),
        max(when(col("funnel_stage") == "3_interest", 1).otherwise(0)).alias("reached_interest"),
        max(when(col("funnel_stage") == "4_purchase", 1).otherwise(0)).alias("reached_purchase")
    )

    # 일별 Funnel 통계 (순차적 전환 추적)
    daily_funnel = session_funnel.groupBy("date").agg(
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
    )

    # 전환율 계산 (순차적 전환 기반)
    daily_funnel = daily_funnel.withColumn(
        "search_to_detail_rate",
        when(col("search_count") > 0, (col("search_and_detail") / col("search_count") * 100)).otherwise(0)
    ).withColumn(
        "detail_to_interest_rate",
        when(col("detail_count") > 0, (col("detail_and_interest") / col("detail_count") * 100)).otherwise(0)
    ).withColumn(
        "interest_to_purchase_rate",
        when(col("interest_count") > 0, (col("interest_and_purchase") / col("interest_count") * 100)).otherwise(0)
    ).withColumn(
        "overall_conversion_rate",
        when(col("search_count") > 0, (col("search_and_purchase") / col("search_count") * 100)).otherwise(0)
    ).withColumn("date", to_date(col("date")))

    print(f"   집계 완료: {daily_funnel.count()}일")
    return daily_funnel


def run_all_transforms(df):
    """모든 변환 실행 (기존 5개 + 지역 분석 7개 + 고도화 4개 = 총 16개)"""
    print(f"\n{'='*60}")
    print("🔄 [TRANSFORM] 데이터 변환 시작")
    print(f"{'='*60}")

    # 1. 데이터 클리닝 (지역명 표준화 및 시간 파생 컬럼 생성)
    df_cleaned = apply_data_cleaning(df)

    # 2. 기존 집계 실행 (5개)
    hourly_stats = transform_hourly_stats(df_cleaned)
    user_stats = transform_user_stats(df_cleaned)
    event_stats = transform_event_stats(df_cleaned)
    top_users = transform_top_users(df_cleaned, top_n=100)
    daily_summary = transform_daily_summary(hourly_stats, user_stats, event_stats)

    # 3. 지역 분석 집계 실행 (7개)
    regional_search = transform_regional_search(df_cleaned)
    room_preference = transform_room_preference(df_cleaned)
    hourly_activity = transform_hourly_activity(df_cleaned)
    engagement_index = transform_engagement_index(df_cleaned)
    daily_activity_peak = transform_daily_activity_peak(df_cleaned)
    hourly_deal = transform_hourly_deal(df_cleaned)
    hourly_engagement = transform_hourly_engagement(df_cleaned)

    # 4. 고도화 분석 집계 실행 (4개)
    session_analysis = transform_session_analysis(df_cleaned)
    user_behavior_patterns = transform_user_behavior_patterns(df_cleaned)
    hourly_behavior_analysis = transform_hourly_behavior_analysis(df_cleaned)
    funnel_analysis = transform_funnel_analysis(df_cleaned)

    print(f"\n✅ [TRANSFORM] 완료 (총 16개 집계)\n")

    return {
        # 기존 5개
        'hourly_stats': hourly_stats,
        'user_stats': user_stats,
        'event_stats': event_stats,
        'top_users': top_users,
        'daily_summary': daily_summary,
        # 지역 분석 7개
        'regional_search': regional_search,
        'room_preference': room_preference,
        'hourly_activity': hourly_activity,
        'engagement_index': engagement_index,
        'daily_activity_peak': daily_activity_peak,
        'hourly_deal': hourly_deal,
        'hourly_engagement': hourly_engagement,
        # 고도화 분석 4개
        'session_analysis': session_analysis,
        'user_behavior_patterns': user_behavior_patterns,
        'hourly_behavior_analysis': hourly_behavior_analysis,
        'funnel_analysis': funnel_analysis
    }

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

    print(f"\n🔧 [TRANSFORM] 실행 날짜: {date}\n")

    spark = Config.get_spark_session(f"transform_{date}")

    try:
        # Silver layer에서 데이터 읽기
        silver_path = f"{Config.get_results_path()}silver/date={date}"
        print(f"📥 Silver layer 읽기: {silver_path}")
        df = spark.read.parquet(silver_path)
        print(f"✅ Silver layer 로드 완료: {df.count():,}건\n")

        # Transform: 데이터 변환
        results = run_all_transforms(df)

        # Gold layer로 저장 (각 테이블별로 Parquet)
        gold_base_path = f"{Config.get_results_path()}gold/date={date}"
        print(f"\n💾 Gold layer 저장 중: {gold_base_path}")

        for table_name, df_result in results.items():
            gold_path = f"{gold_base_path}/{table_name}"
            df_result.write.mode("overwrite").parquet(gold_path)
            print(f"   ✅ {table_name} 저장 완료")

        print(f"\n✅ Gold layer 저장 완료\n")

        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Transform 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
