FROM confluentinc/cp-kafka-connect-base:7.6.0

USER root

RUN microdnf install -y curl unzip && microdnf clean all

# Aiven JDBC Connector (Apache 2.0) — Confluent JDBC Plugin 대체
# confluent-hub에 등록되지 않아 GitHub 릴리즈에서 직접 다운로드
RUN curl -fsSL -o /tmp/aiven-jdbc-connector.zip \
    https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka/releases/download/v6.10.0/jdbc-connector-for-apache-kafka-6.10.0.zip && \
    unzip /tmp/aiven-jdbc-connector.zip -d /usr/share/confluent-hub-components/ && \
    rm /tmp/aiven-jdbc-connector.zip

# PostgreSQL JDBC Driver (BSD-2-Clause)
# unzip 후 실제 디렉토리명을 find로 탐색해 동적으로 경로 결정
RUN CONNECTOR_DIR=$(find /usr/share/confluent-hub-components -maxdepth 1 -type d -name "*jdbc*" | head -1) && \
    mkdir -p "${CONNECTOR_DIR}/lib" && \
    curl -fsSL -o "${CONNECTOR_DIR}/lib/postgresql-42.7.3.jar" \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

USER appuser
