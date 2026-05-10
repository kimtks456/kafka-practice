# Q7. POJO를 common-lib에 두는 구조 + Connect 스키마 보장

## Part A — JVM 환경에서 POJO 공유 구조 분석

### 현재 구조

```
kafka-common-lib
  └── events/
      ├── KafkaEvent.java           (interface)
      ├── order/
      │   ├── OrderCreatedEvent.java
      │   └── OrderCancelledEvent.java
      └── log/
          └── SystemLogEvent.java
```

모든 Producer/Consumer 서비스가 `kafka-common-lib`에 의존해 같은 POJO 클래스를 공유한다.

### 장점

| 항목 | 내용 |
|------|------|
| 타입 안전성 | 컴파일 타임에 이벤트 스펙이 맞는지 확인됨 |
| 중복 제거 | 이벤트 구조를 각 서비스마다 따로 정의할 필요 없음 |
| JVM 한정 환경에서의 표준 | Nexus/Maven 배포 + BOM 임포트로 버전 통제 가능 |

### 한계 — JVM이 아닌 소비자가 생기면

```
Python 서비스, Node.js 서비스 → Java POJO 사용 불가
→ 각 언어가 자체적으로 이벤트 구조를 다시 정의 → 스키마 드리프트 위험
```

이 경우 **Schema Registry + Avro/Protobuf**가 표준 해법이지만,  
현재 설계는 "JVM 환경만 사용"을 전제로 하므로 현재 구조는 적절하다.

---

## Part B — Connect 스키마 보장 문제

### 관계 정리

```
POJO (Java) → Jackson 직렬화 → JSON (Kafka topic)
                                      ↓
                            Connect JDBC Sink
                                      ↓
                              SMT (rename camelCase → snake_case)
                                      ↓
                              PostgreSQL system_log 테이블
```

Connect는 POJO를 모른다. **JSON 필드명과 DB 컬럼명의 일치**가 전부다.

### 현재 보장 메커니즘

1. **POJO가 진실의 원천**  
   `SystemLogEvent`의 필드 → Jackson이 직렬화 → JSON의 키
2. **SMT가 명시적 매핑**  
   `eventId:event_id`, `aggregateId:aggregate_id`, ... (connector JSON에 명시)
3. **DDL이 최종 목적지**  
   `system_log.ddl.sql`의 컬럼이 JSON 키(SMT 변환 후)와 일치해야 함

### 위험 시나리오

| 변경 | 결과 |
|------|------|
| POJO에 새 필드 추가 | JSON에 새 키 추가 → `auto.evolve=false`이므로 DB 컬럼 없으면 Connect가 에러 발생 |
| POJO 필드 rename | JSON 키 변경 → SMT renames 설정도 변경 필요 → 변경 누락 시 null INSERT |
| POJO 필드 제거 | JSON에 해당 키 없음 → DB 컬럼이 NOT NULL이면 에러 |

### 현재 설계에서의 실질적 위험도

**낮음.** 이유:
- `SystemLogEvent`는 로그 적재 전용 이벤트 — 비즈니스 로직과 분리되어 스키마 변경 빈도 낮음
- 단순 append-only — 읽기 패턴 없으므로 필드 추가만 발생, 삭제는 거의 없음
- Producer가 통제 범위 내에 있음 (직접 개발한 서비스)

### Schema Registry 없이 보장하는 방법

1. **POJO + DDL + SMT를 함께 검토하는 커밋 규칙**  
   `SystemLogEvent.java` 수정 시 `system_log.ddl.sql`과 `system-log-sink.json`도 같은 커밋에 포함
   
2. **통합 테스트 (Testcontainers)** — Q2 참고  
   Producer가 실제 이벤트를 발행하고 PostgreSQL에 INSERT가 성공하는지 테스트

3. **`auto.evolve=false` 유지**  
   스키마 변경 시 Connect가 조용히 ALTER TABLE하는 것을 방지. 의도치 않은 변경을 에러로 노출.

### Schema Registry 도입 시점

JVM이 아닌 소비자가 생기거나, 여러 팀이 독립적으로 이벤트를 발행하는 상황이 오면 그때 고려.  
현재 단계에서는 POJO + 규율(커밋 규칙) + 통합 테스트로 충분하다.

---

## 결론

- POJO를 common-lib에 두는 구조: JVM 환경 한정으로 **적절하다**
- Connect 스키마 보장: Schema Registry 없이도 **POJO-DDL-SMT 3종 동시 변경 규칙 + 통합 테스트**로 보장 가능
- 핵심은 `auto.evolve=false`를 유지해서 스키마 미스매치가 조용히 넘어가지 않게 하는 것
