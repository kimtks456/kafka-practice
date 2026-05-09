package com.example.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;

public class KafkaConsumerConfig {

    public DefaultKafkaConsumerFactoryCustomizer hardSettingsCustomizer() {
        return factory -> factory.updateConfigs(java.util.Map.of(
            ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"
        ));
    }
}
