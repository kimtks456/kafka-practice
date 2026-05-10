package com.example.log.kafka;

import com.example.kafka.events.log.SystemLogEvent;
import com.example.log.domain.SystemLog;
import com.example.log.repository.SystemLogRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(properties = "spring.jpa.hibernate.ddl-auto=create-drop")
@Testcontainers
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SystemLogFlowTest {

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka:latest")
    );

    @Container
    @ServiceConnection
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Value("${kafka.topic.system-log}")
    String topic;

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    SystemLogRepository repository;

    private SystemLogEvent event;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
        event = new SystemLogEvent(
                UUID.randomUUID().toString(),
                "order-service",
                "order-service",
                "INFO",
                "주문 처리 완료",
                Map.of("traceId", "trace-001", "orderId", "order-abc"),
                Instant.now()
        );
    }

    @Test
    void 로그이벤트_발행후_DB_저장() {
        kafkaTemplate.send(topic, event.eventId(), event);

        await().atMost(10, SECONDS).until(() -> repository.count() == 1);

        SystemLog saved = repository.findAll().get(0);
        assertEquals(event.eventId(), saved.getEventId());
        assertEquals(event.serviceId(), saved.getServiceId());
        assertEquals("INFO", saved.getLevel());
        assertEquals(event.message(), saved.getMessage());
    }

    @Test
    void 중복_eventId_한번만_저장() {
        kafkaTemplate.send(topic, event.eventId(), event);
        kafkaTemplate.send(topic, event.eventId(), event);

        // 첫 번째 메시지 저장 대기
        await().atMost(10, SECONDS).until(() -> repository.count() >= 1);
        // 두 번째(중복)가 처리될 시간을 주고 카운트가 1 유지되는지 확인
        with().pollDelay(1, SECONDS).await().atMost(3, SECONDS)
                .untilAsserted(() -> assertEquals(1, repository.count()));
    }

    @Test
    void 다른_serviceId_이벤트_각각_저장() {
        SystemLogEvent paymentLog = new SystemLogEvent(
                UUID.randomUUID().toString(),
                "payment-service",
                "payment-service",
                "ERROR",
                "결제 처리 실패",
                Map.of("traceId", "trace-002"),
                Instant.now()
        );

        kafkaTemplate.send(topic, event.eventId(), event);
        kafkaTemplate.send(topic, paymentLog.eventId(), paymentLog);

        await().atMost(10, SECONDS).until(() -> repository.count() == 2);

        List<SystemLog> all = repository.findAll();
        assertEquals(2, all.size());
    }

    @Test
    void 다양한_레벨_이벤트_모두_저장() {
        SystemLogEvent warnEvent = new SystemLogEvent(
                UUID.randomUUID().toString(), "order-service", "order-service",
                "WARN", "재고 부족 경고", Map.of(), Instant.now()
        );
        SystemLogEvent errorEvent = new SystemLogEvent(
                UUID.randomUUID().toString(), "order-service", "order-service",
                "ERROR", "DB 연결 실패", Map.of("errorCode", "DB_001"), Instant.now()
        );

        kafkaTemplate.send(topic, event.eventId(), event);
        kafkaTemplate.send(topic, warnEvent.eventId(), warnEvent);
        kafkaTemplate.send(topic, errorEvent.eventId(), errorEvent);

        await().atMost(10, SECONDS).until(() -> repository.count() == 3);
    }
}
