package com.example.kafka.idempotency;

public enum IdempotencyKey {
    /**
     * Kafka at-least-once 중복 수신 방어용. 프로듀서 재시도 시 새로운 eventId가 생성되므로
     * 재시도 메시지는 정상 처리된다. 대부분의 경우 이 타입을 사용한다.
     */
    EVENT_ID,

    /**
     * 동일 aggregateId에 대한 비즈니스 레벨 중복 방지용.
     * 프로듀서가 재시도 시 새 eventId로 같은 aggregateId를 발행하면 재시도가 차단된다.
     * "동일 주문 ID 이벤트는 TTL 내 단 한 번만 처리" 같은 의도적 중복 방지에만 사용할 것.
     */
    AGGREGATE_ID
}
