package com.example.kafka.idempotency;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

public class IdempotencyRedisStore {

    private static final String KEY_PREFIX = "idempotency:";
    private final StringRedisTemplate redis;

    public IdempotencyRedisStore(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public boolean setIfAbsent(String key, long ttlSeconds) {
        Boolean result = redis.opsForValue().setIfAbsent(
            KEY_PREFIX + key,
            "1",
            Duration.ofSeconds(ttlSeconds)
        );
        return Boolean.TRUE.equals(result);
    }

    public void delete(String key) {
        redis.delete(KEY_PREFIX + key);
    }
}
