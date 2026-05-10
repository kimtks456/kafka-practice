package com.example.order.kafka;

import com.example.order.domain.CreateOrderRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderEventProducerTest {

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

    @Test
    void 주문_이벤트_발행_후_orderId_반환() {
        CreateOrderRequest request = new CreateOrderRequest(
                "customer-1",
                List.of(new CreateOrderRequest.OrderItemRequest(
                        "product-1", "테스트상품", 2, BigDecimal.valueOf(10000)
                ))
        );

        String orderId = producer.publish(request);

        assertNotNull(orderId);
    }
}
