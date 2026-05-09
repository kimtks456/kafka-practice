package com.example.kafka.idempotency;

import com.example.kafka.events.KafkaEvent;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

@Aspect
public class IdempotencyAspect {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyAspect.class);

    private final IdempotencyRedisStore store;

    public IdempotencyAspect(IdempotencyRedisStore store) {
        this.store = store;
    }

    @Around("@annotation(idempotentConsumer)")
    public Object around(ProceedingJoinPoint pjp, IdempotentConsumer idempotentConsumer) throws Throwable {
        Object[] args = pjp.getArgs();
        if (args.length == 0 || !(args[0] instanceof KafkaEvent event)) {
            return pjp.proceed();
        }

        String key = switch (idempotentConsumer.keyType()) {
            case EVENT_ID -> event.eventId();
            case AGGREGATE_ID -> event.aggregateId();
        };

        if (!store.setIfAbsent(key, idempotentConsumer.ttlSeconds())) {
            log.info("[Idempotency] 중복 이벤트 skip. key={}", key);
            return null;
        }

        try {
            return pjp.proceed();
        } catch (Exception e) {
            store.delete(key);
            throw e;
        }
    }
}
