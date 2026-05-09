package com.example.order.kafka;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.kafka.idempotency.IdempotentConsumer;
import com.example.kafka.idempotency.IdempotencyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    @KafkaListener(topics = "prd.order.created.v1", groupId = "order-service")
    @IdempotentConsumer(keyType = IdempotencyKey.EVENT_ID, ttlSeconds = 86400)
    public void handle(OrderCreatedEvent event) {
        log.info("[Consumer] OrderCreated 수신. orderId={} totalAmount={}",
            event.aggregateId(), event.totalAmount());
        // 실제 비즈니스 로직 자리
    }
}
