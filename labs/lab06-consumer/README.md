# Lab 06 — Consumer internals

One broker, one topic with four partitions, six experiments about the half of Kafka
where incidents actually happen. Port 9092 — stop labs 02 and 05 first.

Follow the [worksheet](../../docs/lab-06-consumer.html) for the step-by-step version.

## Setup

```bash
docker compose up -d && docker compose ps

docker exec lab06-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --topic lab06.events --partitions 4 --replication-factor 1

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd scripts
python3 00_seed.py 10000
```

Several steps want two or three terminals. Each needs its own
`source .venv/bin/activate`.

## Experiments

```bash
# 1 — fetch tuning: latency for the first record
python3 01_fetch_tuning.py 1 500
python3 01_fetch_tuning.py 100000 2000

# 2 — the auto-commit reprocessing window
python3 02_autocommit_window.py consume lab06-ac1   # Ctrl+C after ~10s
python3 02_autocommit_window.py recount lab06-ac1

# 3 — exceed max.poll.interval.ms (two terminals)
python3 03_poll_interval.py 12 c1
python3 03_poll_interval.py 12 c2

# 4 — static membership (two terminals, restart one)
python3 04_static_membership.py c1
python3 04_static_membership.py c2 static-2

# 5 — eager vs cooperative sticky (three terminals each run)
python3 05_assignor_compare.py range c1
python3 05_assignor_compare.py cooperative-sticky c1
```

## Inspecting the group

```bash
docker exec lab06-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group lab06-ac1

docker exec lab06-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --list

# the offsets topic itself
docker exec lab06-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic __consumer_offsets \
  --from-beginning --max-messages 10 \
  --formatter 'kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter'
```

## Tear down

```bash
docker compose down -v
```
