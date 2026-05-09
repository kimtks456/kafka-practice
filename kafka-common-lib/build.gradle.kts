plugins {
    `java-library`
    id("io.spring.dependency-management")
    `maven-publish`
}

group = "com.example"
version = "1.0.0-SNAPSHOT"

dependencies {
    // 도메인 서비스도 직접 사용 → api 전파
    api("org.springframework.kafka:spring-kafka")

    // 라이브러리 내부 구현
    implementation("org.springframework.boot:spring-boot-starter-aop")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
    annotationProcessor("org.springframework.boot:spring-boot-autoconfigure-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testCompileOnly("org.projectlombok:lombok")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testAnnotationProcessor("org.projectlombok:lombok")
}

tasks.withType<Test> { useJUnitPlatform() }

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "nexus"
            url = uri(
                if (version.toString().endsWith("SNAPSHOT"))
                    "http://localhost:8081/repository/maven-snapshots/"
                else
                    "http://localhost:8081/repository/maven-releases/"
            )
            credentials {
                username = project.findProperty("nexusUsername") as String? ?: "admin"
                password = project.findProperty("nexusPassword") as String? ?: ""
            }
            isAllowInsecureProtocol = true
        }
    }
}
