# Q4. Nexus 크리덴셜 외부화 + common-lib application.yaml 분석

## Part A — Nexus 크리덴셜 외부화

### 현재 상태

```kotlin
// kafka-common-lib/build.gradle.kts
credentials {
    username = project.findProperty("nexusUsername") as String? ?: "admin"
    password = project.findProperty("nexusPassword") as String? ?: ""
}
```

`project.findProperty`는 **Gradle 프로젝트 프로퍼티**를 읽는다.  
값을 제공하는 방법:

1. **`~/.gradle/gradle.properties`** (권장 — 로컬 머신 전역, gitignore 불필요)
   ```properties
   nexusUsername=admin
   nexusPassword=my-secret
   ```

2. **`kafka-practice/gradle.properties`** — 프로젝트 루트. 이미 gitignore에 포함돼 있음.

3. **CLI 플래그** — `./gradlew publish -PnexusUsername=admin -PnexusPassword=...`

4. **환경변수** — Gradle은 `ORG_GRADLE_PROJECT_nexusUsername` 환경변수를 자동으로 `nexusUsername` 프로퍼티로 매핑한다.
   ```bash
   export ORG_GRADLE_PROJECT_nexusUsername=admin
   export ORG_GRADLE_PROJECT_nexusPassword=my-secret
   ./gradlew publish
   ```

### 추가 개선안 — System.getenv fallback

```kotlin
credentials {
    username = System.getenv("NEXUS_USERNAME")
        ?: project.findProperty("nexusUsername") as String?
        ?: "admin"
    password = System.getenv("NEXUS_PASSWORD")
        ?: project.findProperty("nexusPassword") as String?
        ?: ""
}
```

CI 환경(GitHub Actions, Jenkins 등)에서는 시크릿을 환경변수로 주입하는 게 표준이므로  
이 방식이 로컬(`gradle.properties`)과 CI(`env var`) 모두를 커버한다.

### 결론

`application.yml`은 Spring Boot 런타임 설정이지, Gradle 빌드 시 크리덴셜 주입 용도가 아니다.  
Gradle 크리덴셜은 `gradle.properties`(로컬) + 환경변수(CI)가 표준이고 현재 구조도 이 방향이 맞다.

---

## Part B — common-lib `application.yaml`가 왜 비어 있는가

### 라이브러리 책임 분리 원칙

Spring Boot 라이브러리(auto-configuration 기반)에서 `application.yaml`에 **넣어야 하는 것**과 **넣으면 안 되는 것**이 구분된다.

#### 넣으면 안 되는 것 (인프라 연결 정보 — 소비 앱이 결정)

```yaml
# ❌ 라이브러리가 결정하면 안 됨
spring:
  kafka:
    bootstrap-servers: localhost:9092   # 어느 브로커인지는 앱이 알아야 함
  data:
    redis:
      host: localhost                   # 어느 Redis인지는 앱이 알아야 함
```

이 값들은 환경(dev/qa/prd)마다 달라지고, 라이브러리가 고정하면 소비 앱이 오버라이드해야 하는 불편이 생긴다.

#### 넣어야 하는 것 (라이브러리가 제공하는 기본 동작값)

```yaml
# ✅ 라이브러리의 soft 기본값 — 소비 앱이 override 가능
spring:
  kafka:
    producer:
      properties:
        linger.ms: 20
        batch.size: 16384
```

이 값들은 "우리 플랫폼 표준"이고, 소비 앱이 특별한 이유 없이는 변경할 필요가 없다.

### 현재 문제

`kafka-common-lib/src/main/resources/application.yaml`이 완전히 비어 있다.  
Q3에서 분석한 soft 기본값이 여기 없기 때문에 각 서비스가 직접 넣고 있다.

### 정리

| 설정 | 책임 | 파일 |
|------|------|------|
| `bootstrap-servers`, `redis.host` | 소비 앱 (환경 의존) | `order-service/application-{env}.yaml` |
| `linger.ms`, `batch.size` (soft 기본) | common-lib (플랫폼 표준) | `kafka-common-lib/src/main/resources/application.yaml` |
| `acks=all`, `isolation.level` (hard) | common-lib (강제) | `KafkaProducerConfig.java` (Customizer) |
| `nexusUsername/Password` | Gradle 빌드 시 (빌드 도구 설정) | `~/.gradle/gradle.properties` or env var |
