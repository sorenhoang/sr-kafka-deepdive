# Lab 00 — A log and a queue, side by side

The claim in module 0 is that Kafka is a log rather than a queue, and that everything
else follows from it. This lab makes the single most consequential consequence visible:
**consuming does not remove data**.

Same five messages, one Kafka topic and one RabbitMQ queue, the same three steps
against both.

## 0. Start both brokers

```bash
docker compose up -d
docker compose ps          # wait for both to report healthy (~30s)
```

- Kafka: `localhost:9092` (single node, KRaft)
- RabbitMQ: `localhost:5672`, management UI at http://localhost:15672 (guest / guest)

```bash
docker exec lab00-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create --topic lab00.events --partitions 1 --replication-factor 1
```

```bash
cd labs/lab00-log-vs-queue        # requirements.txt lives here, not in scripts/
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd scripts
```

> `source .venv/bin/activate` applies to **one terminal**. Every new tab or window needs
> it again, or `python3` falls back to the system interpreter and the imports fail with
> `ModuleNotFoundError: No module named 'confluent_kafka'`. Check with `which python3` —
> it should print a path inside `.venv`.

## 1. Publish to both

```bash
python 01_publish_both.py
```

Five messages into each. Note the RabbitMQ queue depth it prints.

## 2. Consume once from each

```bash
python 02_consume_both.py
```

Both hand you the same five messages. Then look at what each one has left:

- **RabbitMQ** queue depth drops to **0**. Acking removed the messages. The broker was
  tracking state for every message individually, and that state is now "delivered and
  acknowledged, so delete".
- **Kafka** still holds all five records. All that changed is one number — the group's
  committed offset in `__consumer_offsets`.

Confirm it yourself rather than taking the script's word for it:

```bash
# Kafka: the records are still there
docker exec lab00-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:19092 --topic lab00.events

# Kafka: and the group's position is what moved
docker exec lab00-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group reader-a

# RabbitMQ: nothing left
docker exec lab00-rabbitmq rabbitmqctl list_queues name messages
```

`GetOffsetShell` reports the end offset, which stays at 5. `--describe` shows
`CURRENT-OFFSET 5`, `LOG-END-OFFSET 5`, `LAG 0`. The data did not move; the cursor did.

## 3. Read again, and read as somebody new

```bash
python 03_read_again.py
```

- A **new Kafka consumer group** replays all five records from offset 0, even though
  another group already consumed them.
- **RabbitMQ** has nothing for anybody. To fan out there you must decide *before
  publishing*: an exchange copies the message into one queue per consumer.

That asymmetry is why replay, reprocessing after a bug, and adding a downstream
consumer six months later are free in Kafka and are a redesign in a queue.

## 4. Exercises

1. **Make Kafka forget.** Set a one-minute retention and watch the records disappear
   without anyone consuming them:
   ```bash
   docker exec lab00-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 \
     --alter --entity-type topics --entity-name lab00.events \
     --add-config retention.ms=60000,segment.ms=60000
   ```
   Wait a couple of minutes, then re-run `03_read_again.py`. What does a new group see?
   This is the point that deletion is driven by **retention**, never by consumption.

2. **Rewind a group that already finished.** With no live members:
   ```bash
   docker exec lab00-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
     --bootstrap-server localhost:19092 --group reader-a \
     --topic lab00.events --reset-offsets --to-earliest --execute
   ```
   Then re-run `02_consume_both.py`. Why is there no RabbitMQ equivalent of this command?

3. **Make RabbitMQ fan out.** Declare a fanout exchange, bind two queues, publish once,
   and consume from both. How many copies of the message now exist, and who decided
   that number — the publisher or the consumer? Compare with how Kafka answers the same
   question.

4. **The honest counter-example.** Put one message back on the RabbitMQ queue and
   `basic_nack` it with `requeue=False` after configuring a dead-letter exchange. Kafka
   has no equivalent for a single record. What would you have to build to get the same
   behaviour, and what does that tell you about which problems belong on a queue?

## 5. Tear down

```bash
docker compose down -v
```
