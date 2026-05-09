package com.example.kafka.config;

import com.example.kafka.error.DltPublisher;
import com.example.kafka.idempotency.IdempotencyAspect;
import com.example.kafka.idempotency.IdempotencyRedisStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration
@ConditionalOnClass(KafkaTemplate.class)
public class KafkaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "kafkaHardProducerCustomizer")
    public DefaultKafkaProducerFactoryCustomizer kafkaHardProducerCustomizer() {
        return new KafkaProducerConfig().hardSettingsCustomizer();
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaHardConsumerCustomizer")
    public DefaultKafkaConsumerFactoryCustomizer kafkaHardConsumerCustomizer() {
        return new KafkaConsumerConfig().hardSettingsCustomizer();
    }

    @Bean
    @ConditionalOnMissingBean
    public IdempotencyAspect idempotencyAspect(IdempotencyRedisStore store) {
        return new IdempotencyAspect(store);
    }

    @Bean
    @ConditionalOnMissingBean
    public DltPublisher dltPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DltPublisher(kafkaTemplate);
    }
}
