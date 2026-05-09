#!/bin/bash
# topics/ 하위의 모든 YAML을 읽어 Kafka 토픽을 생성한다.
# --if-not-exists: 이미 존재하면 skip (재시작 시 충돌 없음)
set -e

BOOTSTRAP=${KAFKA_BOOTSTRAP:-kafka:9092}

for f in $(find /topics -name "*.yaml" | sort); do
  name=$(grep '^name:'              "$f" | awk '{print $2}')
  partitions=$(grep '^partitions:'  "$f" | awk '{print $2}')
  replication=$(grep '^replication-factor:' "$f" | awk '{print $2}')
  retention=$(grep 'retention.ms:'  "$f" | awk '{print $2}' | tr -d '"')
  cleanup=$(grep 'cleanup.policy:'  "$f" | awk '{print $2}')

  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    --config "retention.ms=${retention}" \
    --config "cleanup.policy=${cleanup}"

  echo "[init] topic ready: $name (partitions=${partitions}, retention=${retention}ms)"
done
