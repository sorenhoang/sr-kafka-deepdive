# Lab 02 — What a partition actually looks like on disk

Module 1 said a partition is a log on disk. This lab opens that log, splits it into
segments, reads its index, deletes parts of it by retention, and compacts it by key.

One broker is enough — storage is a per-partition, per-broker concern.

Follow the [worksheet](../../docs/lab-02-storage.html) rather than this file if you
want the step-by-step version with predictions. This README is the command reference.

## Setup

```bash
docker compose up -d
docker compose ps          # wait for healthy, ~30s

python3 -m venv .venv
source .venv/bin/activate  # once per terminal
pip install -r requirements.txt
cd scripts
```

Two broker settings are turned down so the lab does not make you wait:
`log.retention.check.interval.ms` to 10s (default 5 minutes) and
`log.cleaner.backoff.ms` to 5s. Production leaves both alone.

## Topics used

```bash
# tiny segments, so a few hundred small records roll several of them
docker exec lab02-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --topic lab02.segments --partitions 1 --replication-factor 1 \
  --config segment.bytes=1024

# compacted, cleaned aggressively so the lab finishes in seconds
docker exec lab02-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --create --topic lab02.compacted --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.bytes=1024 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config min.compaction.lag.ms=0 \
  --config delete.retention.ms=10000
```

## Looking at the files

```bash
# the partition directory — note the naming: <topic>-<partition>
docker exec lab02-kafka ls -la /tmp/kraft-combined-logs/lab02.segments-0/

# records in a segment
docker exec lab02-kafka /opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kraft-combined-logs/lab02.segments-0/00000000000000000000.log \
  --print-data-log | head -30

# the offset index — sparse by design
docker exec lab02-kafka /opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kraft-combined-logs/lab02.segments-0/00000000000000000000.index

# the time index
docker exec lab02-kafka /opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kraft-combined-logs/lab02.segments-0/00000000000000000000.timeindex
```

## Retention

```bash
# the trap: short retention, default segment roll
docker exec lab02-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 \
  --alter --entity-type topics --entity-name lab02.segments \
  --add-config retention.ms=30000

# the fix: let segments roll, so whole segments become eligible
docker exec lab02-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 \
  --alter --entity-type topics --entity-name lab02.segments \
  --add-config segment.ms=20000

# where the log starts and ends now
docker exec lab02-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic lab02.segments --time -2   # earliest
docker exec lab02-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic lab02.segments --time -1   # latest
```

## Compaction

```bash
python3 02_compaction_demo.py

# force the active segment closed, otherwise there is nothing for the cleaner to do
docker exec lab02-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 \
  --alter --entity-type topics --entity-name lab02.compacted \
  --add-config segment.ms=5000

# wait ~20s, then look at what survived
docker exec lab02-kafka /opt/kafka/bin/kafka-dump-log.sh \
  --files $(docker exec lab02-kafka sh -c 'ls /tmp/kraft-combined-logs/lab02.compacted-0/*.log | head -1') \
  --print-data-log | grep -E 'offset|payload'
```

## Inspecting configuration

```bash
# what is actually set on a topic, and what is inherited
docker exec lab02-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 \
  --describe --entity-type topics --entity-name lab02.segments --all | head -40
```

## Tear down

```bash
docker compose down -v
```
