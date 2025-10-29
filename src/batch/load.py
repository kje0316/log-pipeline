from pyspark.sql import DataFrame
import psycopg2


def delete_date_data(table_name, jdbc_url, properties, date):
    """
    특정 날짜의 데이터를 PostgreSQL 테이블에서 삭제

    Args:
        table_name: 테이블명
        jdbc_url: JDBC URL (예: jdbc:postgresql://localhost:5432/analytics)
        properties: JDBC 속성
        date: 삭제할 날짜 (YYYY-MM-DD)
    """
    # JDBC URL에서 PostgreSQL 연결 정보 추출
    # jdbc:postgresql://localhost:5432/analytics -> localhost:5432/analytics
    url_parts = jdbc_url.replace("jdbc:postgresql://", "").split("/")
    host_port = url_parts[0].split(":")
    host = host_port[0]
    port = int(host_port[1]) if len(host_port) > 1 else 5432
    database = url_parts[1]

    # PostgreSQL 연결
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=properties["user"],
        password=properties["password"]
    )

    try:
        cursor = conn.cursor()

        # 테이블 존재 여부 확인
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (table_name,))

        table_exists = cursor.fetchone()[0]

        if table_exists:
            delete_query = f"DELETE FROM {table_name} WHERE date = %s"
            cursor.execute(delete_query, (date,))
            deleted_count = cursor.rowcount
            conn.commit()

            if deleted_count > 0:
                print(f"   🗑️  기존 데이터 삭제: {deleted_count:,}건")
        else:
            print(f"   ℹ️  테이블 {table_name} 생성 예정 (첫 실행)")

    finally:
        cursor.close()
        conn.close()


def load_to_postgres(df, table_name, jdbc_url, properties, date=None, mode="append"):
    """
    DataFrame을 PostgreSQL에 저장

    Args:
        df: Spark DataFrame
        table_name: 테이블명
        jdbc_url: JDBC URL
        properties: JDBC 속성
        date: 날짜 (재처리를 위해 기존 데이터 삭제)
        mode: 저장 모드(append, overwrite)
    """
    print(f"\n💾 [LOAD] {table_name} 테이블 저장 중...")

    # 재처리를 위해 기존 날짜 데이터 삭제
    if date:
        delete_date_data(table_name, jdbc_url, properties, date)

    record_count = df.count()

    df.write.jdbc(jdbc_url, table_name, mode=mode, properties=properties)

    print(f"   ✅ {record_count:,}건 저장 완료")



def load_all_results(results, jdbc_url, properties, date=None):
    """
    모든 변환 결과를 PostgreSQL에 저장 (기존 5개 + 지역 분석 7개 + 고도화 4개 = 총 16개)

    Args:
        results: 변환 결과 딕셔너리
        jdbc_url: JDBC URL
        properties: JDBC 속성
        date: 처리 날짜 (재처리를 위해 기존 데이터 삭제)
    """
    print(f"\n{'='*60}")
    print("💾 [LOAD] PostgreSQL 저장 시작")
    print(f"{'='*60}")

    # 기존 5개 테이블 (date 컬럼 포함)
    load_to_postgres(results['hourly_stats'], 'hourly_stats', jdbc_url, properties, date)
    load_to_postgres(results['user_stats'], 'user_stats', jdbc_url, properties, date)
    load_to_postgres(results['event_stats'], 'event_stats', jdbc_url, properties, date)
    load_to_postgres(results['top_users'], 'top_users', jdbc_url, properties, date)
    load_to_postgres(results['daily_summary'], 'daily_summary', jdbc_url, properties, date)

    # 지역 분석 7개 테이블 (date 컬럼 포함 - 일별 집계)
    load_to_postgres(results['regional_search'], 'regional_search', jdbc_url, properties, date)
    load_to_postgres(results['room_preference'], 'room_preference', jdbc_url, properties, date)
    load_to_postgres(results['hourly_activity'], 'hourly_activity', jdbc_url, properties, date)
    load_to_postgres(results['engagement_index'], 'engagement_index', jdbc_url, properties, date)
    load_to_postgres(results['daily_activity_peak'], 'daily_activity_peak', jdbc_url, properties, date)
    load_to_postgres(results['hourly_deal'], 'hourly_deal', jdbc_url, properties, date)
    load_to_postgres(results['hourly_engagement'], 'hourly_engagement', jdbc_url, properties, date)

    # 고도화 분석 4개 테이블 (date 컬럼 포함 - 일별 집계)
    load_to_postgres(results['session_analysis'], 'session_analysis', jdbc_url, properties, date)
    load_to_postgres(results['user_behavior_patterns'], 'user_behavior_patterns', jdbc_url, properties, date)
    load_to_postgres(results['hourly_behavior_analysis'], 'hourly_behavior_analysis', jdbc_url, properties, date)
    load_to_postgres(results['funnel_analysis'], 'funnel_analysis', jdbc_url, properties, date)

    print(f"\n✅ [LOAD] 완료 (총 16개 테이블)\n")



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

    print(f"\n🔧 [LOAD] 실행 날짜: {date}\n")

    spark = Config.get_spark_session(f"load_{date}")

    try:
        # Gold layer에서 데이터 읽기
        gold_base_path = f"{Config.get_results_path()}gold/date={date}"
        print(f"📥 Gold layer 읽기: {gold_base_path}\n")

        results = {}
        table_names = [
            # 기존 5개
            'hourly_stats', 'user_stats', 'event_stats', 'top_users', 'daily_summary',
            # 지역 분석 7개
            'regional_search', 'room_preference', 'hourly_activity', 'engagement_index',
            'daily_activity_peak', 'hourly_deal', 'hourly_engagement',
            # 고도화 분석 4개
            'session_analysis', 'user_behavior_patterns', 'hourly_behavior_analysis', 'funnel_analysis'
        ]

        for table_name in table_names:
            gold_path = f"{gold_base_path}/{table_name}"
            results[table_name] = spark.read.parquet(gold_path)
            print(f"   ✅ {table_name} 로드 완료: {results[table_name].count():,}건")

        # JDBC 속성
        jdbc_properties = {
            "user": Config.POSTGRES_USER,
            "password": Config.POSTGRES_PASSWORD,
            "driver": "org.postgresql.Driver"
        }

        # PostgreSQL에 저장
        load_all_results(results, Config.POSTGRES_JDBC_URL, jdbc_properties, date)

        print(f"\n✅ [LOAD] 완료! PostgreSQL에 데이터 저장됨\n")

        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Load 실패: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()