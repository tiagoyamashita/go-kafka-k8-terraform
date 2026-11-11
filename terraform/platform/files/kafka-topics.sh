set -eu
ready=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-0.kafka:9092 --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done
if [ "$ready" -ne 1 ]; then
  echo "kafka did not become ready" >&2
  exit 1
fi
for topic in transfers.in transfers.out transfers.dlq; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-0.kafka:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-0.kafka:9092 \
  --alter --entity-type topics --entity-name transfers.in \
  --add-config max.message.bytes=65536
