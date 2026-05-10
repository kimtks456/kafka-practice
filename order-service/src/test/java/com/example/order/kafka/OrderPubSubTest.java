package com.example.order.kafka;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.kafka.events.order.OrderItem;
import com.example.order.domain.CreateOrderRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Kafka Pub/Sub 메커니즘 검증.
 *
 * 핵심 시나리오:
 * - 여러 이벤트 발행 시 모두 수신
 * - 다른 consumer group은 독립적으로 동일 메시지 수신 (fan-out)
 * - 수신된 이벤트의 payload 정확성
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(OrderPubSubTest.SecondGroupConsumer.class)
class OrderPubSubTest {

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
    OrderEventProducer producer;

    @MockitoSpyBean
    OrderEventConsumer consumer;

    @Autowired
    SecondGroupConsumer secondConsumer;

    @BeforeEach
    void setUp() {
        Mockito.clearInvocations(consumer);
    }

    @Test
    void 여러_이벤트_연속_발행_모두_수신() {
        // 각 publish 마다 새 UUID로 eventId 생성 → @IdempotentConsumer가 모두 통과
        CreateOrderRequest request = new CreateOrderRequest(
                "customer-1",
                List.of(new CreateOrderRequest.OrderItemRequest("p1", "상품", 1, BigDecimal.valueOf(10000)))
        );

        producer.publish(request);
        producer.publish(request);
        producer.publish(request);

        verify(consumer, timeout(10000).times(3)).handle(any(OrderCreatedEvent.class));
    }

    @Test
    void 발행된_이벤트_payload_정확성() {
        String orderId = producer.publish(new CreateOrderRequest(
                "customer-payload-test",
                List.of(new CreateOrderRequest.OrderItemRequest("p1", "페이로드상품", 2, BigDecimal.valueOf(5000)))
        ));

        org.mockito.ArgumentCaptor<OrderCreatedEvent> captor =
                org.mockito.ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(consumer, timeout(5000)).handle(captor.capture());

        OrderCreatedEvent received = captor.getValue();
        assertEquals("customer-payload-test", received.customerId());
        assertEquals(orderId, received.aggregateId());
        assertEquals(BigDecimal.valueOf(10000), received.totalAmount()); // 5000 × 2
        assertNotNull(received.eventId());
        assertNotNull(received.occurredAt());
        assertEquals(1, received.items().size());
        assertEquals("p1", received.items().get(0).productId());
    }

    @Test
    void 다른_consumerGroup_fan_out_수신() throws InterruptedException {
        int countBefore = secondConsumer.getCount();

        producer.publish(new CreateOrderRequest(
                "customer-fanout",
                List.of(new CreateOrderRequest.OrderItemRequest("p1", "팬아웃상품", 1, BigDecimal.valueOf(9000)))
        ));

        // order-service 그룹 수신 확인
        verify(consumer, timeout(5000).atLeastOnce()).handle(any(OrderCreatedEvent.class));

        // test-second-group 독립 수신 확인
        long deadline = System.currentTimeMillis() + 5000;
        while (secondConsumer.getCount() <= countBefore && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertTrue(secondConsumer.getCount() > countBefore,
                "다른 consumer group도 동일 메시지를 수신해야 한다");
    }

    @Test
    void 단일_이벤트_항목_포함_전체_필드_검증() {
        List<CreateOrderRequest.OrderItemRequest> items = List.of(
                new CreateOrderRequest.OrderItemRequest("sku-001", "노트북", 1, BigDecimal.valueOf(1200000)),
                new CreateOrderRequest.OrderItemRequest("sku-002", "마우스", 2, BigDecimal.valueOf(50000))
        );

        producer.publish(new CreateOrderRequest("customer-multi-item", items));

        org.mockito.ArgumentCaptor<OrderCreatedEvent> captor =
                org.mockito.ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(consumer, timeout(5000)).handle(captor.capture());

        OrderCreatedEvent received = captor.getValue();
        assertEquals(2, received.items().size());
        // totalAmount = 1200000×1 + 50000×2 = 1300000
        assertEquals(BigDecimal.valueOf(1300000), received.totalAmount());
    }

    // ─── 두 번째 consumer group 테스트용 빈 ────────────────────────────────

    @Component
    static class SecondGroupConsumer {
        private final AtomicInteger count = new AtomicInteger(0);

        @KafkaListener(topics = "${kafka.topic.order-created}", groupId = "test-second-group")
        public void handle(OrderCreatedEvent event) {
            count.incrementAndGet();
        }

        public int getCount() { return count.get(); }
    }
}
