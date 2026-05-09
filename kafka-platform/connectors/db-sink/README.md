# DB Sink Connector

JDBC Sink Connector 설정. `prd.log.system.v1` 토픽의 메시지를 PostgreSQL `kafka_system_log` 테이블에 적재한다.

## 환경변수

| 변수 | 설명 |
|---|---|
| `DB_URL` | JDBC 연결 URL (예: `jdbc:postgresql://localhost:5432/mydb`) |
| `DB_USER` | DB 사용자 |
| `DB_PASSWORD` | DB 패스워드 |
