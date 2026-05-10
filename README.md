# kafka-practice

실무에서 재사용 가능한 Kafka 공통 플랫폼을 설계·구현하는 실습 프로젝트.

도메인 서비스가 공통 라이브러리를 가져다 쓰기만 해도 **안정적인 메시지 발행·소비·로그 적재**가 되는 구조를 목표로 한다.

---

## 1. 목표 기능

| # | 기능 | 설명 |
|---|------|------|
| 1 | **Pub/Sub** | 도메인 이벤트(OrderCreated 등)를 Kafka로 발행하고 다른 서비스가 소비 |
| 2 | **로그 적재 (Sink Connector)** | 별도 Consumer 코드 없이 Kafka Connect DB Sink로 시스템 로그를 DB에 적재 |
| 3 | **멱등적 소비** | `@IdempotentConsumer` 어노테이션 한 줄로 Redis 기반 중복 처리 방지 보장 |

---

## 2. 모듈 구성

| 모듈 | 역할 |
|------|------|
| **kafka-platform** | 인프라 설정 저장소. docker-compose(Kafka·Redis·Nexus), 토픽 YAML, Connector JSON 관리 |
| **kafka-common-lib** | 공통 라이브러리. 이벤트 레코드, Hard 설정 강제 적용, `@IdempotentConsumer` AOP, DLT 발행, Spring AutoConfiguration으로 자동 등록 |
| **order-service** | 샘플 도메인 서비스. `kafka-common-lib`을 의존성으로 추가해 발행·소비 구현 |

---

## 3. 주요 설계 원칙

**Hard / Soft 설정 분리**
- `kafka-common-lib`이 `acks=all`, `enable.idempotence=true`, `isolation.level=read_committed` 등 안전성 필수 설정을 `DefaultKafkaProducerFactoryCustomizer`로 강제 적용
- 도메인 서비스는 `application.yaml`에서 `linger.ms`, `batch.size` 등 성능 튜닝(Soft 설정)만 담당

**AutoConfiguration으로 Zero-Config 연동**
- `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 등록으로 의존성 추가만 해도 공통 빈 자동 주입

**이벤트 스키마는 코드로 관리**
- Schema Registry 없이 `kafka-common-lib` 내 Java record를 스키마로 사용
- 버전 변경 시 라이브러리 버전업 → Nexus 재배포

---

## 4. 로컬 실행

> **사전 조건 — Colima**  
> 이 프로젝트는 Docker Desktop 대신 [Colima](https://github.com/abiosoft/colima)를 사용한다.  
> Docker Desktop은 상업적 환경에서 유료 라이선스가 필요하므로 Apache 2.0 라이선스의 Colima로 대체한다.
>
> ```bash
> brew install colima docker docker-compose
> ```

### 4.1 Colima 시작

```bash
colima start
```

이미 실행 중이면 건너뛴다 (`colima status`로 확인).

### 4.2 Testcontainers 환경변수 설정

Testcontainers가 Colima 소켓을 찾을 수 있도록 `~/.zshrc`에 아래 두 줄이 있어야 한다.

```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

적용:

```bash
source ~/.zshrc
```

### 4.3 인프라 기동

```bash
docker compose -f kafka-platform/test/docker-compose.yml up -d
```

### 4.4 토픽 생성

토픽은 `init-kafka` 컨테이너가 `kafka-platform/topics/` 하위 YAML을 읽어 자동 생성한다.  
수동으로 만들거나 확인할 때:

```bash
# 목록 조회
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# 단건 생성
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic order.created --partitions 6 --replication-factor 1
```

### 4.5 order-service 실행

```bash
./gradlew :order-service:bootRun
```

### 4.6 메시지 발행 예시

#### order.created

order-service가 orderId(UUID)를 key로 자동 설정한다.

**curl**
```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "c001",
    "items": [
      {"productId": "p1", "productName": "노트북", "quantity": 1, "unitPrice": 1200000}
    ]
  }'
```

**Kafka UI** (http://localhost:8989 → Topics → order.created → Produce Message)

| 필드 | 예시 |
|------|------|
| Key | `550e8400-e29b-41d4-a716-446655440001` |
| Value | 아래 JSON |

```json
{
  "eventId":    "550e8400-e29b-41d4-a716-446655440000",
  "aggregateId": "550e8400-e29b-41d4-a716-446655440001",
  "customerId": "c001",
  "items": [
    {"productId": "p1", "productName": "노트북", "quantity": 1, "unitPrice": 1200000}
  ],
  "totalAmount": 1200000,
  "occurredAt": "2026-05-11T00:00:00Z"
}
```

---

#### log.created

log.created는 order-service API가 없으므로 Kafka UI로 직접 발행한다.  
(curl로 Kafka에 직접 publish하려면 Kafka REST Proxy 필요)

**Kafka UI** (http://localhost:8989 → Topics → log.created → Produce Message)

| 필드 | 예시 |
|------|------|
| Key | `550e8400-e29b-41d4-a716-446655440002` |
| Value | 아래 JSON |

```json
{
  "eventId":    "550e8400-e29b-41d4-a716-446655440002",
  "aggregateId": "order-service",
  "serviceId":  "order-service",
  "level":      "INFO",
  "message":    "주문 처리 완료",
  "context":    {"traceId": "trace-001", "orderId": "order-abc"},
  "occurredAt": "2026-05-11T00:00:00Z"
}
```

발행 후 log-service가 소비해 PostgreSQL `system_log` 테이블에 적재된다.

---

## 5. 로그 / 디버깅

### 5.1 브로커 로그

```bash
docker logs -f kafka
```

### 5.2 Kafka 브로커 버전 확인

```bash
docker logs kafka 2>&1 | grep "metadata.version"
```

출력 예: `Publishing initial metadata ... with metadata.version Optional[4.2-IV1]`

### 5.3 토픽 메시지 실시간 조회

```bash
# 처음부터
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order.created \
  --from-beginning

# 최신 메시지만
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order.created
```

### 5.4 Consumer Group Lag 확인

```bash
# 전체 그룹 목록
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# 특정 그룹 상세 (LAG 포함)
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group order-service
```

---

## 6. Nexus 배포 (kafka-common-lib)

```bash
# ~/.gradle/gradle.properties 에 nexusUsername / nexusPassword 설정 후
./gradlew :kafka-common-lib:publish
```

SNAPSHOT은 재배포 가능, Release는 불변. `build.gradle.kts`의 `version` 값 끝이 `-SNAPSHOT`이면 자동으로 snapshots 레포로 올라간다.
