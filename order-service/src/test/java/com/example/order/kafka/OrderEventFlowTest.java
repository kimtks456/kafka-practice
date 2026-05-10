package com.example.order.kafka;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.order.domain.CreateOrderRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Testcontainers
class OrderEventFlowTest {

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

    @SpyBean
    OrderEventConsumer consumer;

    @Test
    void 주문_이벤트_발행_후_컨슈머_수신() {
        producer.publish(new CreateOrderRequest(
                "customer-1",
                List.of(new CreateOrderRequest.OrderItemRequest(
                        "product-1", "테스트상품", 1, BigDecimal.valueOf(10000)
                ))
        ));

        verify(consumer, timeout(5000).times(1)).handle(any(OrderCreatedEvent.class));
    }
}
