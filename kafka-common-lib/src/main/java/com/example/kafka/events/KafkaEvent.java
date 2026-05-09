package com.example.kafka.events;

public interface KafkaEvent {
    String eventId();
    String aggregateId();
}
