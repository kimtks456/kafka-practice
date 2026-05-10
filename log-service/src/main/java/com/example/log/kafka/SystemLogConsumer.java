package com.example.log.kafka;

import com.example.kafka.events.log.SystemLogEvent;
import com.example.kafka.idempotency.IdempotencyKey;
import com.example.kafka.idempotency.IdempotentConsumer;
import com.example.log.domain.SystemLog;
import com.example.log.repository.SystemLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SystemLogConsumer {

    private static final Logger log = LoggerFactory.getLogger(SystemLogConsumer.class);

    private final SystemLogRepository repository;

    public SystemLogConsumer(SystemLogRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "${kafka.topic.system-log}", groupId = "${kafka.consumer.system-log.group-id}")
    @IdempotentConsumer(keyType = IdempotencyKey.EVENT_ID, ttlSeconds = 86400)
    public void handle(SystemLogEvent event) {
        repository.save(SystemLog.from(event));
        log.info("[Consumer] SystemLog 저장. eventId={} serviceId={} level={}",
            event.eventId(), event.serviceId(), event.level());
    }
}
