package com.example.kafka.idempotency;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.kafka.events.order.OrderItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
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

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
class IdempotencyAspectTest {

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
    SampleConsumer consumer;

    private OrderCreatedEvent event;

    @BeforeEach
    void setUp() {
        consumer.reset();
        event = new OrderCreatedEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "customer-1",
                List.of(new OrderItem("product-1", "테스트상품", 1, BigDecimal.valueOf(10000))),
                BigDecimal.valueOf(10000),
                Instant.now()
        );
    }

    @Test
    void 첫번째_이벤트_정상_처리() {
        consumer.handle(event);
        assertEquals(1, consumer.processedCount);
    }

    @Test
    void 동일_eventId_두번째_호출_skip() {
        consumer.handle(event);
        consumer.handle(event);
        assertEquals(1, consumer.processedCount);
    }

    @Test
    void 예외_발생시_키_삭제_후_재시도_성공() {
        consumer.shouldThrow = true;
        assertThrows(RuntimeException.class, () -> consumer.handle(event));

        consumer.shouldThrow = false;
        consumer.handle(event);
        assertEquals(1, consumer.processedCount);
    }

    @Component
    static class SampleConsumer {
        int processedCount = 0;
        boolean shouldThrow = false;

        @IdempotentConsumer(keyType = IdempotencyKey.EVENT_ID, ttlSeconds = 60)
        public void handle(OrderCreatedEvent event) {
            if (shouldThrow) throw new RuntimeException("test error");
            processedCount++;
        }

        void reset() {
            processedCount = 0;
            shouldThrow = false;
        }
    }
}
