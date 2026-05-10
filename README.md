# kafka-practice

실무에서 재사용 가능한 Kafka 공통 플랫폼을 설계·구현하는 실습 프로젝트.

도메인 서비스가 공통 라이브러리를 가져다 쓰기만 해도 **안정적인 메시지 발행·소비·로그 적재**가 되는 구조를 목표로 한다.

---

## 목표 기능

| # | 기능 | 설명 |
|---|------|------|
| 1 | **Pub/Sub** | 도메인 이벤트(OrderCreated 등)를 Kafka로 발행하고 다른 서비스가 소비 |
| 2 | **로그 적재 (Sink Connector)** | 별도 Consumer 코드 없이 Kafka Connect DB Sink로 시스템 로그를 DB에 적재 |
| 3 | **멱등적 소비** | `@IdempotentConsumer` 어노테이션 한 줄로 Redis 기반 중복 처리 방지 보장 |

---

## 모듈 구성

| 모듈 | 역할 |
|------|------|
| **kafka-platform** | 인프라 설정 저장소. docker-compose(Kafka·Redis·Nexus), 토픽 YAML, Connector JSON 관리 |
| **kafka-common-lib** | 공통 라이브러리. 이벤트 레코드, Hard 설정 강제 적용, `@IdempotentConsumer` AOP, DLT 발행, Spring AutoConfiguration으로 자동 등록 |
| **order-service** | 샘플 도메인 서비스. `kafka-common-lib`을 의존성으로 추가해 발행·소비 구현 |

---

## 주요 설계 원칙

**Hard / Soft 설정 분리**
- `kafka-common-lib`이 `acks=all`, `enable.idempotence=true`, `isolation.level=read_committed` 등 안전성 필수 설정을 `DefaultKafkaProducerFactoryCustomizer`로 강제 적용
- 도메인 서비스는 `application.yaml`에서 `linger.ms`, `batch.size` 등 성능 튜닝(Soft 설정)만 담당

**AutoConfiguration으로 Zero-Config 연동**
- `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` 등록으로 의존성 추가만 해도 공통 빈 자동 주입

**이벤트 스키마는 코드로 관리**
- Schema Registry 없이 `kafka-common-lib` 내 Java record를 스키마로 사용
- 버전 변경 시 라이브러리 버전업 → Nexus 재배포

---

## 로컬 실행

> **사전 조건 — Colima**  
> 이 프로젝트는 Docker Desktop 대신 [Colima](https://github.com/abiosoft/colima)를 사용한다.  
> Docker Desktop은 상업적 환경에서 유료 라이선스가 필요하므로 Apache 2.0 라이선스의 Colima로 대체한다.
>
> ```bash
> brew install colima docker docker-compose
> ```

### 1. Colima 시작

```bash
colima start
```

이미 실행 중이면 건너뛴다 (`colima status`로 확인).

### 2. Testcontainers 환경변수 설정

Testcontainers가 Colima 소켓을 찾을 수 있도록 `~/.zshrc`에 아래 두 줄이 있어야 한다.

```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

적용:

```bash
source ~/.zshrc
```

### 3. 인프라 기동

```bash
docker compose -f kafka-platform/test/docker-compose.yml up -d
```

### 4. 토픽 생성

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic prd.order.created.v1 --partitions 6 --replication-factor 1
```

### 5. order-service 실행

```bash
./gradlew :order-service:bootRun
```

### 6. 이벤트 발행 테스트

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"c001","items":[{"productId":"p1","quantity":2,"price":9900}]}'
```

Kafka UI → http://localhost:8989 에서 메시지 확인 가능.

---

## Nexus 배포 (kafka-common-lib)

```bash
# ~/.gradle/gradle.properties 에 nexusUsername / nexusPassword 설정 후
./gradlew :kafka-common-lib:publish
```

SNAPSHOT은 재배포 가능, Release는 불변. `build.gradle.kts`의 `version` 값 끝이 `-SNAPSHOT`이면 자동으로 snapshots 레포로 올라간다.
