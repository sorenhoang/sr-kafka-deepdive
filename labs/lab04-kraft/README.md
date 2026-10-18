# Lab 04 — KRaft and the metadata plane

Three nodes in combined mode (`broker,controller`). The lab reads the controller
quorum, finds your own topic creation inside the metadata log, and kills the active
controller to time a failover.

Broker ports clash with labs 01 and 03 — stop those first. The UI is on 8082.

Follow the [worksheet](../../docs/lab-04-kraft.html) for the step-by-step version.
This README is the command reference.

## Setup

```bash
docker compose up -d && docker compose ps
```

No Python needed in this lab — everything is CLI against the cluster.

## The quorum

```bash
# who is the active controller, what offset is the quorum at
docker exec lab04-kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 --describe --status

# per-replica view: log end offset and lag for every voter and observer
docker exec lab04-kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 --describe --replication
```

`--bootstrap-controller localhost:9093` talks to the controller listener directly,
which still works when brokers are unhealthy.

## Reading the metadata log

```bash
docker exec lab04-kafka1 ls -la /tmp/kraft-combined-logs/__cluster_metadata-0/

docker exec lab04-kafka1 /opt/kafka/bin/kafka-dump-log.sh \
  --cluster-metadata-decoder \
  --files /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log \
  | tail -40
```

An interactive view of the same state:

```bash
docker exec -it lab04-kafka1 /opt/kafka/bin/kafka-metadata-shell.sh \
  --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
# then: ls /, cat /image/topics, exit
```

## Failing the controller

```bash
# find the active controller id from --describe --status, then stop that container
docker stop lab04-kafka2
docker exec lab04-kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 --describe --status
docker start lab04-kafka2
```

## Tear down

```bash
docker compose down -v
```
