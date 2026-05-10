package com.example.log.domain;

import com.example.kafka.events.log.SystemLogEvent;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@Entity
@Table(name = "system_log")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class SystemLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;

    @Column(name = "service_id", nullable = false)
    private String serviceId;

    @Column(nullable = false, length = 10)
    private String level;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String message;

    @Column(name = "context_json", columnDefinition = "TEXT")
    private String contextJson;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    public static SystemLog from(SystemLogEvent event) {
        SystemLog entity = new SystemLog();
        entity.eventId = event.eventId();
        entity.serviceId = event.serviceId();
        entity.level = event.level();
        entity.message = event.message();
        entity.occurredAt = event.occurredAt();
        if (event.context() != null && !event.context().isEmpty()) {
            entity.contextJson = event.context().entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));
        }
        return entity;
    }
}
