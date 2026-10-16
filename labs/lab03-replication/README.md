# Lab 03 — Replication, ISR and the durability line

Three brokers, and you break them on purpose. The lab answers one question in five
different ways: under which failures does Kafka lose an acknowledged write?

Container names are prefixed `lab03-` and the UI is on port 8081, but the broker ports
(19092/29092/39092) are the same as lab 01 — **stop lab 01 first**
(`cd ../lab01 && docker compose down`) or the brokers will fail to bind.

Follow the [worksheet](../../docs/lab-03-replication.html) for the step-by-step
version with predictions. This README is the command reference.

## Setup

```bash
docker compose up -d && docker compose ps     # three nodes healthy, ~45s

docker exec lab03-kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic lab03.durable --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2

python3 -m venv .venv
source .venv/bin/activate                     # once per terminal — you want two
pip install -r requirements.txt
cd scripts
```

A single partition is deliberate: one leader, two followers, and nothing else moving.

## Watching

```bash
# in its own terminal, for the whole lab
python3 03_watch_isr.py 600

# or one-shot
docker exec lab03-kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic lab03.durable

# partitions whose ISR is below min.insync.replicas
docker exec lab03-kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --under-min-isr-partitions

# partitions whose ISR is smaller than the replica list
docker exec lab03-kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --under-replicated-partitions
```

## Breaking things

```bash
docker pause lab03-kafka3      # alive but frozen: falls behind, drops out of ISR
docker unpause lab03-kafka3

docker stop lab03-kafka3       # gone
docker start lab03-kafka3
```

`pause` and `stop` are different experiments. A paused broker is still "up" to TCP for
a while and leaves the leader waiting; a stopped one disconnects immediately.

## Producing

```bash
python3 01_produce_acks.py all 5     # rejected once ISR < min.insync.replicas
python3 01_produce_acks.py 1 5       # accepted — and that is the loss window
python3 02_watch_hw.py 1             # how long until a record becomes readable
```

## Healing

```bash
docker start lab03-kafka2 lab03-kafka3

# leadership concentrates on whoever recovered first — put it back
docker exec lab03-kafka1 /opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server localhost:9092 --election-type preferred --all-topic-partitions
```

## Tear down

```bash
docker compose down -v
```
