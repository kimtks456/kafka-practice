# Q6. Kafka Header로 필수 파라미터 분리 — 분석

## 핵심 질문 정리

1. event body에서 `eventId`, `aggregateId`를 header로 빼면 body에는 없어도 되나?
2. header로 빼면 body 역직렬화 전에 idempotency 체크가 가능한가?
3. 그게 아니라면 `BaseEvent` 상속이 필요한가?

---

## 현재 구조

```
Producer: KafkaTemplate.send(topic, key, OrderCreatedEvent)
         → Spring Kafka가 POJO → JSON 직렬화 (body)
         → header: 없음 (plain JSON 그대로)

Consumer: @KafkaListener void handle(OrderCreatedEvent event)
         → Spring Kafka가 JSON → POJO 역직렬화 먼저
         → @IdempotencyAspect: event.eventId() 읽어 Redis 체크
```

역직렬화가 **항상 먼저** 일어난다. 중복 이벤트라도 일단 POJO를 만든다.

---

## Header 방식 — 무엇이 달라지는가

### Producer 쪽

```java
// KafkaTemplate에 ProducerRecord 직접 생성
ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC, null, orderId, event);
record.headers().add("eventId", event.eventId().getBytes(StandardCharsets.UTF_8));
record.headers().add("aggregateId", event.aggregateId().getBytes(StandardCharsets.UTF_8));
kafkaTemplate.send(record);
```

또는 common-lib에서 `ProducerInterceptor`로 `KafkaEvent` 구현체의 필드를 자동 header로 추가.

### Consumer 쪽 — 역직렬화 전 체크가 가능하려면

```java
// 방법 A: 메서드 파라미터에서 header 분리 접근 (body는 여전히 역직렬화됨)
@KafkaListener(...)
public void handle(
    @Header("eventId") String eventId,
    OrderCreatedEvent event  // Spring Kafka가 역직렬화
) { ... }
// → header 접근은 편리하지만 역직렬화는 여전히 일어남

// 방법 B: 역직렬화 완전 지연
@KafkaListener(containerFactory = "rawBytesFactory")
public void handle(ConsumerRecord<String, byte[]> record) {
    String eventId = new String(record.headers().lastHeader("eventId").value());
    if (!store.setIfAbsent(eventId, 86400)) return; // 중복 → body 역직렬화 없이 skip
    OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
    // 처리
}
```

방법 B가 "body 역직렬화 없이 idempotency 체크"를 실현하는 유일한 방법이다.

---

## 방법 B의 실제 이득과 비용

### 이득

```
중복 이벤트 처리 시:
  현재: JSON → POJO 역직렬화 → Redis 체크 → skip
  Header B: header 읽기 → Redis 체크 → skip (JSON 파싱 없음)
```

### 비용

| 항목 | 내용 |
|------|------|
| AOP 패턴 포기 | `@IdempotentConsumer` 어노테이션 기반 처리가 불가. consumer마다 수동으로 체크 코드 작성 |
| `ContainerFactory` 커스텀 | raw bytes factory 별도 등록 필요 |
| ObjectMapper 수동 주입 | consumer 클래스마다 직접 역직렬화 |
| 타입 안전성 감소 | 컴파일 타임에 이벤트 타입 체크 불가 |

### 현실적 판단

JSON 메시지 1개 역직렬화 비용(마이크로초 단위)보다 **AOP 패턴 포기로 생기는 유지보수 부채**가 더 크다.  
특히 이 설계의 목표가 "Consumer 서비스마다 `@IdempotentConsumer` 한 줄로 멱등성을 보장"인 상황에서 그 이점을 버리는 트레이드오프가 맞지 않는다.

---

## BaseEvent 상속 필요한가

### Interface vs Abstract Class

Java records는 abstract class를 extend할 수 없다.  
현재 `KafkaEvent`는 interface이고 records가 구현한다. 이 구조가 올바르다.

```java
// ✅ 올바른 구조
public interface KafkaEvent {
    String eventId();
    String aggregateId();
}

public record OrderCreatedEvent(...) implements KafkaEvent { ... }

// ❌ records는 class를 extend할 수 없음
// public record OrderCreatedEvent(...) extends BaseEvent { ... }  // 컴파일 에러
```

`BaseEvent`를 abstract class로 만들 경우 → records를 포기하고 일반 class로 전환해야 한다.  
현재 `KafkaEvent` interface로도 `eventId()`와 `aggregateId()`가 모든 이벤트에 강제되므로 충분하다.

---

## 권장 결론

| 선택지 | 판단 |
|--------|------|
| body에 eventId/aggregateId 유지 + 현재 AOP 방식 | **권장** — 단순하고 일관성 있음 |
| header 추가 (body에도 유지) + AOP 방식 유지 | 트레이드오프 없는 중복 정보. 필요 없으면 하지 않아도 됨 |
| header만 사용 + raw bytes consumer | 역직렬화 절약이 실질적으로 의미 없는 규모에서 복잡도만 증가 |
| BaseEvent abstract class 상속 | records 포기 필요 → 현재 인터페이스 방식이 더 낫다 |

**현재 구조(interface + body에 eventId/aggregateId)는 이 규모에서 최선이다.**  
`aggregateId`가 모든 이벤트에 의미 있는지 여부는 Q8 참고.
