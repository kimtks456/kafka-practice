package com.example.kafka.events.order;

import com.example.kafka.events.KafkaEvent;
import java.time.Instant;

public record OrderCancelledEvent(
    String eventId,
    String aggregateId,    // orderId
    String reason,
    Instant occurredAt
) implements KafkaEvent {}
