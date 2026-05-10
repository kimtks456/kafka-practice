package com.example.order.kafka;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.kafka.events.order.OrderItem;
import com.example.kafka.idempotency.IdempotencyKey;
import com.example.kafka.idempotency.IdempotentConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @IdempotentConsumer 어노테이션 유무에 따른 중복 처리 동작 차이 검증.
 *
 * 핵심 시나리오:
 * - 어노테이션 있음: 동일 eventId 재발행 시 한 번만 처리됨
 * - 어노테이션 없음: 동일 eventId 재발행 시 매번 처리됨 (멱등성 미보장)
 */
@SpringBootTest
@Testcontainers
@Import({
        OrderIdempotencyTest.IdempotentHandler.class,
        OrderIdempotencyTest.NonIdempotentHandler.class,
        OrderIdempotencyTest.AggregateKeyHandler.class
})
class OrderIdempotencyTest {

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka:latest")
    );

    @Container
    @ServiceConnection
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    IdempotentHandler idempotentHandler;

    @Autowired
    NonIdempotentHandler nonIdempotentHandler;

    @Autowired
    AggregateKeyHandler aggregateKeyHandler;

    private OrderCreatedEvent event;

    @BeforeEach
    void setUp() {
        event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "customer-1",
                List.of(new OrderItem("p1", "테스트상품", 1, BigDecimal.valueOf(10000))),
                BigDecimal.valueOf(10000),
                Instant.now()
        );
        idempotentHandler.reset(1);
        nonIdempotentHandler.reset(2);
        aggregateKeyHandler.reset(1);
    }

    @Test
    void 어노테이션_있을때_동일_eventId_두번_발행_한번만_처리() throws InterruptedException {
        kafkaTemplate.send("test.idempotency.with.v1", event.eventId(), event);
        kafkaTemplate.send("test.idempotency.with.v1", event.eventId(), event);

        assertTrue(idempotentHandler.getLatch().await(5, SECONDS), "첫 번째 이벤트가 처리되어야 한다");
        Thread.sleep(500); // 두 번째 메시지가 skip 처리될 시간
        assertEquals(1, idempotentHandler.getCount(), "중복 eventId는 한 번만 처리되어야 한다");
    }

    @Test
    void 어노테이션_없을때_동일_eventId_두번_발행_두번_모두_처리() throws InterruptedException {
        kafkaTemplate.send("test.idempotency.without.v1", event.eventId(), event);
        kafkaTemplate.send("test.idempotency.without.v1", event.eventId(), event);

        assertTrue(nonIdempotentHandler.getLatch().await(5, SECONDS), "두 이벤트 모두 처리되어야 한다");
        assertEquals(2, nonIdempotentHandler.getCount(), "어노테이션 없으면 중복도 모두 처리된다");
    }

    @Test
    void 어노테이션_있을때_다른_eventId_각각_처리() throws InterruptedException {
        OrderCreatedEvent event2 = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "customer-2",
                List.of(new OrderItem("p2", "상품2", 2, BigDecimal.valueOf(5000))),
                BigDecimal.valueOf(10000),
                Instant.now()
        );

        idempotentHandler.reset(2);
        kafkaTemplate.send("test.idempotency.with.v1", event.eventId(), event);
        kafkaTemplate.send("test.idempotency.with.v1", event2.eventId(), event2);

        assertTrue(idempotentHandler.getLatch().await(5, SECONDS), "두 이벤트 모두 처리되어야 한다");
        assertEquals(2, idempotentHandler.getCount(), "다른 eventId는 각각 처리된다");
    }

    @Test
    void AGGREGATE_ID_키타입_동일_aggregateId_두번_발행_한번만_처리() throws InterruptedException {
        // 동일 aggregateId(orderId)지만 다른 eventId — AGGREGATE_ID 기준이라 두 번째는 skip
        OrderCreatedEvent event2 = new OrderCreatedEvent(
                UUID.randomUUID().toString(), // 다른 eventId
                event.aggregateId(),          // 동일 aggregateId
                "customer-1",
                List.of(new OrderItem("p1", "테스트상품", 1, BigDecimal.valueOf(10000))),
                BigDecimal.valueOf(10000),
                Instant.now()
        );

        kafkaTemplate.send("test.idempotency.aggregate.v1", event.aggregateId(), event);
        kafkaTemplate.send("test.idempotency.aggregate.v1", event2.aggregateId(), event2);

        assertTrue(aggregateKeyHandler.getLatch().await(5, SECONDS), "첫 번째 이벤트가 처리되어야 한다");
        Thread.sleep(500);
        assertEquals(1, aggregateKeyHandler.getCount(), "동일 aggregateId는 한 번만 처리되어야 한다");
    }

    // ─── 내부 테스트용 Consumer 빈 ───────────────────────────────────────────

    @Component
    static class IdempotentHandler {
        private final AtomicInteger count = new AtomicInteger(0);
        private volatile CountDownLatch latch = new CountDownLatch(1);

        @KafkaListener(topics = "test.idempotency.with.v1", groupId = "test-idempotent")
        @IdempotentConsumer(keyType = IdempotencyKey.EVENT_ID, ttlSeconds = 60)
        public void handle(OrderCreatedEvent event) {
            count.incrementAndGet();
            latch.countDown();
        }

        public int getCount() { return count.get(); }
        public CountDownLatch getLatch() { return latch; }
        public void reset(int expectedCount) {
            count.set(0);
            latch = new CountDownLatch(expectedCount);
        }
    }

    @Component
    static class NonIdempotentHandler {
        private final AtomicInteger count = new AtomicInteger(0);
        private volatile CountDownLatch latch = new CountDownLatch(2);

        @KafkaListener(topics = "test.idempotency.without.v1", groupId = "test-non-idempotent")
        public void handle(OrderCreatedEvent event) {
            count.incrementAndGet();
            latch.countDown();
        }

        public int getCount() { return count.get(); }
        public CountDownLatch getLatch() { return latch; }
        public void reset(int expectedCount) {
            count.set(0);
            latch = new CountDownLatch(expectedCount);
        }
    }

    @Component
    static class AggregateKeyHandler {
        private final AtomicInteger count = new AtomicInteger(0);
        private volatile CountDownLatch latch = new CountDownLatch(1);

        @KafkaListener(topics = "test.idempotency.aggregate.v1", groupId = "test-aggregate-key")
        @IdempotentConsumer(keyType = IdempotencyKey.AGGREGATE_ID, ttlSeconds = 60)
        public void handle(OrderCreatedEvent event) {
            count.incrementAndGet();
            latch.countDown();
        }

        public int getCount() { return count.get(); }
        public CountDownLatch getLatch() { return latch; }
        public void reset(int expectedCount) {
            count.set(0);
            latch = new CountDownLatch(expectedCount);
        }
    }
}
