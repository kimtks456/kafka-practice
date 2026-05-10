#!/bin/bash
# topics/ 하위의 모든 YAML을 읽어 Kafka 토픽을 생성한다.
# --if-not-exists: 이미 존재하면 skip (재시작 시 충돌 없음)
# retention.ms 없는 compact 토픽(connect 내부 토픽 등)도 처리한다.
set -e

BOOTSTRAP=${KAFKA_BOOTSTRAP:-kafka:29092}

for f in $(find /topics -name "*.yaml" -not -path "*/connect/*" | sort); do
  name=$(grep '^name:'                  "$f" | awk '{print $2}')
  partitions=$(grep '^partitions:'      "$f" | awk '{print $2}')
  replication=$(grep '^replication-factor:' "$f" | awk '{print $2}')
  retention=$(grep 'retention.ms:'     "$f" | awk '{print $2}' | tr -d '"')
  cleanup=$(grep 'cleanup.policy:'     "$f" | awk '{print $2}')

  CONFIG_ARGS="--config cleanup.policy=${cleanup}"
  if [ -n "$retention" ]; then
    CONFIG_ARGS="$CONFIG_ARGS --config retention.ms=${retention}"
  fi

  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$replication" \
    $CONFIG_ARGS

  if [ -n "$retention" ]; then
    echo "[init] topic ready: $name (partitions=${partitions}, cleanup=${cleanup}, retention=${retention}ms)"
  else
    echo "[init] topic ready: $name (partitions=${partitions}, cleanup=${cleanup})"
  fi
done
