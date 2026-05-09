# DB Sink Connector

`prd.log.system.v1` 토픽의 시스템 로그를 PostgreSQL `system_log` 테이블에 적재한다.
별도 Consumer 서비스 없이 Kafka Connect 만으로 DB 적재를 처리한다.

---

## 배치(Bulk Insert) 동작 원리

레코드 **10개** 가 모이거나 **1초** 가 지나면 한 번의 INSERT 로 flush 한다.

```
Kafka Broker  ←─── fetch.min.bytes(1KB) / fetch.max.wait.ms(1s) 협상
     │
     ▼
Consumer.poll()  ←─── max.poll.records=10 상한
     │
     ▼  10개 묶음
JDBC Sink batch INSERT
INSERT INTO system_log (event_id, ...) VALUES
  ('uuid-1', ...),
  ('uuid-2', ...),
  ...
  ('uuid-10', ...);   ← 1문장, 10행
```

| 설정 | 값 | 역할 |
|------|----|------|
| `batch.size` | 10 | INSERT 1문장에 묶을 최대 행 수 |
| `consumer.override.max.poll.records` | 10 | poll() 한 번에 꺼낼 최대 레코드 수 |
| `consumer.override.fetch.min.bytes` | 1024 | 브로커가 응답 전 최소 버퍼 크기(≈10개 분량) |
| `consumer.override.fetch.max.wait.ms` | 1000 | fetch.min.bytes 미충족 시 최대 대기(ms) |

> `batch.size` 없이 max.poll.records=10 만 쓰면 `INSERT` 10번이 개별 실행됨.
> `batch.size=10` 이 있어야 1번의 bulk INSERT 가 됨.

---

## SMT (Single Message Transform)

`context: Map<String,String>` 필드는 JDBC 에 직접 삽입 불가 → `ReplaceField` 로 제거.
camelCase 키를 snake_case 컬럼명으로 매핑.

```
Kafka 레코드 → [rename: eventId→event_id, ...] → [drop: context] → INSERT
```

---

## 사전 조건

1. `system_log.ddl.sql` 로 테이블 생성
2. Kafka Connect worker 에 JDBC Sink Connector jar 배포
3. Connect API 로 등록:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @system-log-sink.json
```

---

## 환경변수

| 변수 | 설명 |
|------|------|
| `DB_URL` | JDBC URL (예: `jdbc:postgresql://localhost:5432/mydb`) |
| `DB_USER` | DB 사용자 |
| `DB_PASSWORD` | DB 패스워드 |
