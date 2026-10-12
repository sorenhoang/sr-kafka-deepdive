# Lab 01 — Partitions, consumer groups and ordering

Four things interviews ask about and candidates often get wrong, made visible.

| # | What to observe |
|---|---|
| 1 | Keyless records are **not** round-robined per record (sticky partitioner) |
| 2 | Same key → same partition; the only mechanism that gives you ordering |
| 3 | One partition per consumer in a group → a 4th consumer on 3 partitions sits idle |
| 4 | Adding partitions makes keys jump, which breaks ordering |

## 0. Start the cluster

```bash
docker compose up -d
docker compose ps          # wait for all three nodes to report healthy (~30-60s)
```

UI: http://localhost:8080 — browse topics, partitions and consumer groups visually.

Bootstrap from the host: `localhost:19092,localhost:29092,localhost:39092`

> If a node never turns healthy: `docker compose logs kafka1 | tail -50`.
> The most common cause is a stale `CLUSTER_ID` left in the volume — run
> `docker compose down -v` and start again.

## 1. Create a topic and read describe carefully

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic lab01.orders --partitions 3 --replication-factor 3

docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic lab01.orders
```

The output looks like:

```
Topic: lab01.orders  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Topic: lab01.orders  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
Topic: lab01.orders  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

**Read every column — this is an interview question:**

- `Leader` — the only broker serving reads and writes for that partition. Three
  partitions means leaders spread across three brokers, which is how Kafka scales writes.
- `Replicas` — the replica list in preference order; the first entry is the
  **preferred leader**.
- `Isr` — in-sync replicas. `Isr` equal to `Replicas` means a healthy cluster. A shorter
  `Isr` means a follower has fallen behind.

## 2. Install the Python client

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd scripts
```

## 3. Run the scripts

```bash
python 01_produce_no_key.py     # partition distribution without a key
python 02_produce_with_key.py   # same key, same partition
```

```bash
# one terminal each; open them one at a time and watch the ASSIGNED/REVOKED lines
python 03_consumer_group.py g1 c1
python 03_consumer_group.py g1 c2
python 03_consumer_group.py g1 c3
python 03_consumer_group.py g1 c4     # <-- 4th consumer: empty assignment
python 03_consumer_group.py g2 x1     # <-- different group: replays from the start
```

Meanwhile, in another terminal:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group g1
```

`CONSUMER-ID` and `HOST` show which consumer owns which partition; `LAG` shows how many
records are still unread.

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic lab01.orders.ordering --partitions 3 --replication-factor 3
python 04_ordering_demo.py      # per-partition order vs topic-wide order
python 05_key_remap.py          # no broker needed: why adding partitions breaks ordering
```

## 4. Exercises

1. **Add partitions and prove the consequence.**
   ```bash
   docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
     --alter --topic lab01.orders --partitions 6
   python 02_produce_with_key.py
   ```
   Compare the key-to-partition mapping before and after. How many keys moved? Does it
   match what `05_key_remap.py` predicted?

   *Expected:* roughly half the keys move. Their older events stay on the old partition,
   so a consumer no longer sees that key's events in order.

2. **Shrink back to 3 partitions.** Try it — what error does Kafka return, and why is
   shrinking impossible given how the log is structured?

3. **Replay a group from the beginning.** Use
   `kafka-consumer-groups.sh --reset-offsets --to-earliest --group g1 --topic lab01.orders --execute`
   (the group must have no live members). Why can Kafka do this when RabbitMQ cannot?

4. **Look at the real log files on disk:**
   ```bash
   docker exec kafka1 ls -la /tmp/kraft-combined-logs/lab01.orders-0/
   docker exec kafka1 /opt/kafka/bin/kafka-dump-log.sh \
     --files /tmp/kraft-combined-logs/lab01.orders-0/00000000000000000000.log \
     --print-data-log | head -20
   ```
   Note `baseOffset`, `lastOffset`, `isTransactional` and `producerId` — M2 and M7 come
   back to this same output.

## 5. Tear down

```bash
docker compose down -v
```
