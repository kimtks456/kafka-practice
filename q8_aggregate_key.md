# Q8. Kafka 메시지 Key — aggregateId 설계 재검토

## 현재 구조

```java
// KafkaEvent interface
String eventId();
String aggregateId();

// Producer
kafkaTemplate.send(TOPIC, orderId, event);  // key = aggregateId (orderId)
```

Kafka 메시지 key가 파티셔닝을 결정한다. 같은 key → 같은 파티션 → **순서 보장**.

---

## aggregateId가 의미 있는 케이스와 의미 없는 케이스

### 의미 있는 경우 — 상태 머신 이벤트

```
ORDER_CREATED → ORDER_CONFIRMED → ORDER_CANCELLED
→ 모두 같은 orderId (aggregateId)
→ 같은 파티션 → 순서 보장 필수
```

Consumer가 이벤트 순서를 신뢰해야 할 때 aggregateId가 key가 되어야 한다.

### 의미 없는 경우 — 독립적 이벤트

```
SystemLogEvent: 각 로그 항목은 서로 독립, 순서 무관
USER_LOGIN, PAGE_VIEW 등 stateless 이벤트
```

이런 케이스에서 `aggregateId`를 key로 쓰면:
- 같은 serviceId의 모든 로그가 같은 파티션으로 몰림 → **파티션 불균형**
- 순서를 신경 쓸 이유가 없는데 파티셔닝 부작용만 발생

---

## 설계 옵션 분석

### 옵션 A — 현재 방식 (aggregateId 필수)

```java
public interface KafkaEvent {
    String eventId();
    String aggregateId();  // 모든 이벤트에 강제
}
```

- **장점**: 단순, 일관성
- **단점**: 의미 없는 도메인에서 aggregateId를 억지로 채워야 함 (SystemLogEvent의 `serviceId = aggregateId`처럼)

### 옵션 B — aggregateId Optional

```java
public interface KafkaEvent {
    String eventId();
    default String partitionKey() { return eventId(); } // 기본값: eventId로 분산
}
```

각 이벤트가 파티션 key를 직접 제공. 순서가 중요한 도메인은 aggregateId를 반환, 나머진 eventId 반환.

```java
// 순서 중요한 이벤트
public record OrderCreatedEvent(...) implements KafkaEvent {
    @Override public String partitionKey() { return orderId; }
}

// 순서 무관 이벤트
public record SystemLogEvent(...) implements KafkaEvent {
    // partitionKey()는 default → eventId로 랜덤 분산
}
```

Producer에서 `kafkaTemplate.send(TOPIC, event.partitionKey(), event)` 통일.

### 옵션 C — 도메인별 인터페이스 분리

```java
public interface KafkaEvent { String eventId(); }
public interface AggregateEvent extends KafkaEvent { String aggregateId(); }

public record OrderCreatedEvent(...) implements AggregateEvent { ... }
public record SystemLogEvent(...)    implements KafkaEvent { ... }
```

타입 계층으로 표현. Producer에서 타입 체크로 key 결정.

---

## 권장 방향

### 단기 (지금 당장) — 옵션 A 유지

현재 이벤트 종류가 적고(Order, SystemLog), `aggregateId`가 어색하더라도 명시적이다.  
`SystemLogEvent.aggregateId = serviceId`처럼 의미를 붙이면 충분히 사용 가능.

### 중장기 — 옵션 B 도입

이벤트 종류가 늘어나면 `partitionKey()`로 의도를 명시하는 방식이 낫다.  
`aggregateId`라는 이름이 도메인에 따라 어색해지는 시점에 전환.

```
도메인 이벤트 (상태 변경): partitionKey() → aggregateId
로그/지표 이벤트 (stateless): partitionKey() → eventId (UUID → 균등 분산)
알림 이벤트: partitionKey() → userId (사용자별 순서 보장)
```

---

## 현재 설계에 대한 판단

| 이벤트 | key | 파티셔닝 의도 | 평가 |
|--------|-----|--------------|------|
| `OrderCreatedEvent` | orderId | 같은 주문의 이벤트 순서 보장 | **적절** |
| `OrderCancelledEvent` | orderId | 동일 | **적절** |
| `SystemLogEvent` | serviceId | 서비스별로 파티션 고정 | **불필요한 쏠림 가능성** |

`SystemLogEvent`의 경우 partitions=3인데 서비스가 2개면 한 파티션은 비게 된다.  
null key나 eventId(UUID)를 쓰면 라운드로빈으로 균등 분산된다.

---

## 결론

`aggregateId`가 필요한 도메인: 상태 머신 이벤트 (Order 계열) → **반드시 필요**  
`aggregateId`가 불필요한 도메인: stateless 로그/지표 → **eventId나 null이 더 적합**  

지금은 옵션 A(전부 aggregateId 강제)로 유지해도 무방하다.  
이벤트 수가 늘고 도메인이 다양해지면 `partitionKey()` 방식(옵션 B)으로 전환을 검토.
