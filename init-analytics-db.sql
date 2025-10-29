-- init-analytics-db.sql
-- PostgreSQL 컨테이너 시작 시 자동 실행되는 초기화 스크립트
-- /docker-entrypoint-initdb.d/ 에 마운트됨

-- 1. analytics 데이터베이스 생성
CREATE DATABASE analytics;

-- 2. zigbang 사용자 생성
CREATE USER zigbang WITH PASSWORD 'password';

-- 3. analytics DB에 대한 모든 권한 부여
GRANT ALL PRIVILEGES ON DATABASE analytics TO zigbang;

-- 4. airflow DB에 대한 권한도 부여 (혹시 모르니)
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\echo '✅ analytics 데이터베이스와 zigbang 사용자 생성 완료'

-- 5. analytics DB에 연결해서 테이블 생성
\c analytics

-- 테이블 생성 (init_tables.sql 내용)
DROP TABLE IF EXISTS hourly_stats CASCADE;
DROP TABLE IF EXISTS user_stats CASCADE;
DROP TABLE IF EXISTS event_stats CASCADE;
DROP TABLE IF EXISTS top_users CASCADE;
DROP TABLE IF EXISTS daily_summary CASCADE;
DROP TABLE IF EXISTS streaming_stats CASCADE;

CREATE TABLE hourly_stats (
    date DATE NOT NULL,
    hour INT NOT NULL,
    total_events BIGINT,
    active_users INT,
    active_sessions INT,
    unique_ips INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, hour)
);

CREATE TABLE user_stats (
    date DATE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    total_actions INT,
    session_count INT,
    unique_event_types INT,
    first_action TIMESTAMP,
    last_action TIMESTAMP,
    session_duration_sec INT,
    avg_actions_per_session FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, user_id)
);

CREATE TABLE event_stats (
    date DATE NOT NULL,
    event_type VARCHAR(200) NOT NULL,
    event_count BIGINT,
    unique_users INT,
    unique_sessions INT,
    avg_events_per_user FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, event_type)
);

CREATE TABLE top_users (
    date DATE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    total_actions INT,
    rank INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, user_id)
);

CREATE TABLE daily_summary (
    date DATE NOT NULL PRIMARY KEY,
    daily_total_events BIGINT,
    daily_total_users INT,
    daily_total_sessions INT,
    avg_hourly_users FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 실시간 스트리밍 집계 테이블
CREATE TABLE streaming_stats (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    total_events BIGINT,
    active_users BIGINT,
    top_event_types TEXT,
    top_screens TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_end)
);

-- 인덱스 생성
CREATE INDEX idx_hourly_date ON hourly_stats(date);
CREATE INDEX idx_user_date ON user_stats(date);
CREATE INDEX idx_event_date ON event_stats(date);
CREATE INDEX idx_top_users_date ON top_users(date);
CREATE INDEX idx_streaming_window_start ON streaming_stats(window_start);

-- zigbang 사용자에게 테이블 권한 부여
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO zigbang;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO zigbang;

\echo '✅ analytics 테이블 생성 완료'
