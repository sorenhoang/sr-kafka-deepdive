# Lab 05 — Producer internals

One broker, six experiments, all about what the producer does before the broker ever
sees a byte. Uses port 9092 like lab 02 — stop that one first.

Follow the [worksheet](../../docs/lab-05-producer.html) for the step-by-step version.

## A caveat that is also a lesson

The Python client is `librdkafka`, and several of its defaults differ from the Java
client's:

| Setting | Java | librdkafka |
|---|---|---|
| `linger.ms` | 0 | 5 |
| `enable.idempotence` | true (since 3.0) | false |
| `max.in.flight` | 5 | 1000000 (capped at 5 with idempotence) |
| producer record-size cap | `max.request.size` | `message.max.bytes` |

Interview answers quote the Java defaults. Your code may not be running them. Every
script here sets what it depends on explicitly rather than inheriting either.

## Setup

```bash
docker compose up -d && docker compose ps
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd scripts
```

## Experiments

```bash
# 1 — batching
for t in lab05.linger0 lab05.linger50; do
  docker exec lab05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
    --create --topic $t --partitions 1 --replication-factor 1
done
python3 01_batching.py lab05.linger0  0
python3 01_batching.py lab05.linger50 50
# count records per batch
docker exec lab05-kafka /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /tmp/kraft-combined-logs/lab05.linger0-0/00000000000000000000.log | grep -c baseOffset

# 2 — compression
for c in none lz4 zstd; do
  docker exec lab05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
    --create --topic lab05.comp-$c --partitions 1 --replication-factor 1
  python3 02_compression.py $c
done
docker exec lab05-kafka du -sh /tmp/kraft-combined-logs/lab05.comp-*/

# 3 — buffer exhaustion
python3 03_buffer_block.py

# 4 — idempotence
python3 04_idempotence.py on
python3 04_idempotence.py off

# 5 — retries vs delivery.timeout.ms
docker stop lab05-kafka && python3 05_retry_timeout.py && docker start lab05-kafka

# 6 — size limits
python3 06_too_large.py 2048
```

## Tear down

```bash
docker compose down -v
```
