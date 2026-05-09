package com.example.kafka.events.order;

import com.example.kafka.events.KafkaEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreatedEvent(
    String eventId,
    String aggregateId,    // orderId
    String customerId,
    List<OrderItem> items,
    BigDecimal totalAmount,
    Instant occurredAt
) implements KafkaEvent {}
