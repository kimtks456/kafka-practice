package com.example.order.kafka;

import com.example.kafka.events.order.OrderCreatedEvent;
import com.example.kafka.events.order.OrderItem;
import com.example.order.domain.CreateOrderRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;

    public OrderEventProducer(KafkaTemplate<String, Object> kafkaTemplate,
                              @Value("${kafka.topic.order-created}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
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

        kafkaTemplate.send(topic, orderId, event);
        log.info("[Producer] OrderCreated 발행. orderId={} eventId={}", orderId, eventId);
        return orderId;
    }
}
