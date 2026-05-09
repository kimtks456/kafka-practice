package com.example.kafka.error;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public class DltPublisher {

    private static final Logger log = LoggerFactory.getLogger(DltPublisher.class);
    private static final String DLT_SUFFIX = ".DLT";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public DltPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String originalTopic, Object payload, Exception cause) {
        String dltTopic = originalTopic + DLT_SUFFIX;
        log.error("[DLT] {} → {} cause={}", originalTopic, dltTopic, cause.getMessage());
        kafkaTemplate.send(dltTopic, payload);
    }
}
