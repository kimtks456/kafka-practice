package com.example.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;

public class KafkaProducerConfig {

    // Hard 설정: 도메인 서비스가 application.yml 로 오버라이드해도 이 Bean 이 마지막에 덮어씀
    public DefaultKafkaProducerFactoryCustomizer hardSettingsCustomizer() {
        return factory -> factory.updateConfigs(java.util.Map.of(
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
            ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"
        ));
    }
}
