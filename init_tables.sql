-- init_tables.sql
-- 파일 위치: /Users/kje/coding/log-pipeline/init_tables.sql

-- 기존 테이블 삭제 (재실행용)
DROP TABLE IF EXISTS hourly_stats CASCADE;
DROP TABLE IF EXISTS user_stats CASCADE;
DROP TABLE IF EXISTS event_stats CASCADE;
DROP TABLE IF EXISTS top_users CASCADE;
DROP TABLE IF EXISTS daily_summary CASCADE;

-- 1. 시간대별 통계
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

-- 2. 유저 통계
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

-- 3. 이벤트 통계
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

-- 4. Top 유저
CREATE TABLE top_users (
    date DATE NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    total_actions INT,
    rank INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, user_id)
);

-- 5. 일별 요약
CREATE TABLE daily_summary (
    date DATE NOT NULL PRIMARY KEY,
    daily_total_events BIGINT,
    daily_total_users INT,
    daily_total_sessions INT,
    avg_hourly_users FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX idx_hourly_date ON hourly_stats(date);
CREATE INDEX idx_user_date ON user_stats(date);
CREATE INDEX idx_event_date ON event_stats(date);
CREATE INDEX idx_top_users_date ON top_users(date);

-- 테이블 확인
\dt

-- 완료 메시지
SELECT 'Tables created successfully!' as message;