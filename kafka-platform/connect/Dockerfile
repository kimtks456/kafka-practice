FROM confluentinc/cp-kafka-connect-base:7.6.0

# Aiven JDBC Connector (Apache 2.0) — Confluent JDBC Plugin 대체
RUN confluent-hub install --no-prompt aiven/jdbc-connector-for-apache-kafka:6.10.0

# PostgreSQL JDBC Driver (BSD-2-Clause)
RUN curl -fsSL -o \
    /usr/share/confluent-hub-components/aiven-jdbc-connector-for-apache-kafka/lib/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
