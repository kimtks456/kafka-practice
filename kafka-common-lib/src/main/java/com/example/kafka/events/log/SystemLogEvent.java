package com.example.kafka.events.log;

import com.example.kafka.events.KafkaEvent;
import java.time.Instant;
import java.util.Map;

public record SystemLogEvent(
    String eventId,
    String aggregateId,    // serviceId (로그는 집계 단위 = 서비스)
    String serviceId,
    String level,          // INFO / WARN / ERROR
    String message,
    Map<String, String> context,
    Instant occurredAt
) implements KafkaEvent {}
