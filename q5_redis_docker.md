# Q5. Redis docker-compose 누락 여부 확인

## 결론

**Redis는 현재 docker-compose.yml에 포함돼 있다.** 누락 아님.

```yaml
redis:
  image: redis:7-alpine
  container_name: redis
  ports:
    - "6379:6379"
```

---

## 현재 구성 검토

`IdempotencyRedisStore`는 `StringRedisTemplate`을 통해 Redis에 `SET NX EX` 명령을 실행한다.  
이 동작이 정상이려면:

1. Redis 컨테이너가 `6379`로 떠 있어야 함 → **docker-compose에 있음** ✓
2. `order-service`의 `application-dev.yaml`에 Redis 연결 정보가 있어야 함 → **있음** ✓
   ```yaml
   spring:
     data:
       redis:
         host: localhost
         port: 6379
   ```
3. `kafka-common-lib`가 `spring-boot-starter-data-redis`를 api로 전파해야 함 → **있음** ✓
   ```kotlin
   implementation("org.springframework.boot:spring-boot-starter-data-redis")
   ```

---

## 잠재적 문제 — 헬스체크 없음

현재 Redis 서비스에 `healthcheck`가 없다.  
`order-service`가 Redis 준비 전에 연결을 시도하면 startup 실패가 발생할 수 있다.

### 개선안

```yaml
redis:
  image: redis:7-alpine
  container_name: redis
  ports:
    - "6379:6379"
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 5s
    timeout: 3s
    retries: 10
```

단, Spring Boot의 `spring.data.redis`는 기본적으로 lazy connection이라 실제 요청 전까지는 연결을 맺지 않는다.  
첫 idempotency 체크 시점까지 Redis가 준비되면 문제없으므로, 로컬 개발 환경에서는 현재 설정으로도 충분하다.

---

## 결론

Redis는 존재한다. 사용자가 이전에 봤던 버전이 헬스체크가 없어서 놓쳤을 수 있다.  
헬스체크 추가는 권장사항이지만 기능 상 누락은 아니다.
