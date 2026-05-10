package com.example.kafka.idempotency;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.redis.DataRedisTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@DataRedisTest
@Testcontainers
class IdempotencyRedisStoreTest {

    @Container
    @ServiceConnection
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    @Autowired
    StringRedisTemplate redisTemplate;

    IdempotencyRedisStore store;

    @BeforeEach
    void setUp() {
        store = new IdempotencyRedisStore(redisTemplate);
    }

    @Test
    void 신규_key_true_반환() {
        assertTrue(store.setIfAbsent(randomKey(), 60));
    }

    @Test
    void 중복_key_false_반환() {
        String key = randomKey();
        store.setIfAbsent(key, 60);
        assertFalse(store.setIfAbsent(key, 60));
    }

    @Test
    void delete_후_재시도_true_반환() {
        String key = randomKey();
        store.setIfAbsent(key, 60);
        store.delete(key);
        assertTrue(store.setIfAbsent(key, 60));
    }

    private String randomKey() {
        return "test-" + UUID.randomUUID();
    }
}
