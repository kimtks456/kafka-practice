FROM gradle:8-jdk21-alpine AS build
WORKDIR /app
COPY . .
RUN gradle :log-service:bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/log-service/build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
