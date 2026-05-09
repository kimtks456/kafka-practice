-- system-log-sink connector 가 적재하는 테이블
-- occurredAt 은 Spring Boot 기본 Jackson 설정 기준 epoch 소수(초) 로 직렬화됨.
-- write-dates-as-timestamps=false 로 ISO-8601 문자열로 바꾸면 TIMESTAMPTZ 로 선언 가능.
CREATE TABLE IF NOT EXISTS system_log (
    id           BIGSERIAL    PRIMARY KEY,
    event_id     VARCHAR(36)  NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    service_id   VARCHAR(255) NOT NULL,
    level        VARCHAR(10)  NOT NULL,  -- INFO / WARN / ERROR
    message      TEXT         NOT NULL,
    occurred_at  VARCHAR(50)  NOT NULL   -- Instant → ISO-8601 or epoch string
);

CREATE INDEX IF NOT EXISTS idx_system_log_service    ON system_log (service_id);
CREATE INDEX IF NOT EXISTS idx_system_log_occurred   ON system_log (occurred_at);
CREATE INDEX IF NOT EXISTS idx_system_log_level      ON system_log (level);
