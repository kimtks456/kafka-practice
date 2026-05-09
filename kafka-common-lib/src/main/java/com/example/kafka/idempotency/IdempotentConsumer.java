package com.example.kafka.idempotency;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface IdempotentConsumer {
    IdempotencyKey keyType() default IdempotencyKey.EVENT_ID;
    long ttlSeconds() default 86400;
}
