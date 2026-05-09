package com.example.order.kafka;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.kafka.events.order.OrderItem;
import com.example.order.domain.CreateOrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "prd.order.created.v1";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String publish(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();

        List<OrderItem> items = request.items().stream()
            .map(i -> new OrderItem(i.productId(), i.productName(), i.quantity(), i.unitPrice()))
            .toList();

        BigDecimal total = items.stream()
            .map(i -> i.unitPrice().multiply(BigDecimal.valueOf(i.quantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        OrderCreatedEvent event = new OrderCreatedEvent(
            eventId, orderId, request.customerId(), items, total, Instant.now()
        );

        kafkaTemplate.send(TOPIC, orderId, event);
        log.info("[Producer] OrderCreated 발행. orderId={} eventId={}", orderId, eventId);
        return orderId;
    }
}
