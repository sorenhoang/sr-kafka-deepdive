# Quiz — every module

Every question from every module, Junior through Senior. Answers are collapsed: open one
only after you have answered it out loud, otherwise the recall practice is spent.

Generated from `src/quiz/*.json` by `tools/build.py` — edit the JSON, not this file.


## M0 · Positioning Kafka

### Junior

**M0.1** — Describe Kafka in one sentence. What is a consumer's unit of progress?

<details>
<summary>Answer</summary>

Kafka is a distributed, replicated, append-only commit log: the broker stores and serves the log, and the read position belongs to the consumer rather than to the broker. A consumer's unit of progress is an **offset** — one integer per partition, committed by its group. That single design decision is where every other property of Kafka comes from.

</details>

**M0.2** — Does a message disappear once it has been consumed? What actually decides when it is deleted?

<details>
<summary>Answer</summary>

No. Reading is non-destructive — consuming only moves the group's offset forward. Deletion is driven entirely by the **retention policy** on the topic: by age (`retention.ms`), by size (`retention.bytes`), or by key for a compacted topic. A record nobody ever reads is deleted on schedule, and a record every group has read stays until retention says otherwise.

</details>

**M0.3** — Name two things a message queue gives you that Kafka does not.

<details>
<summary>Answer</summary>

Per-message control — its own retry schedule, delay, priority, or dead-letter path — because the broker tracks state for every individual message. And broker-side routing: RabbitMQ exchanges can route by topic pattern or header, while a Kafka consumer reads a partition and filters for itself. Both follow from Kafka's broker holding no per-message state.

</details>

### Mid

**M0.4** — Kafka vs RabbitMQ: give the selection criteria without comparing throughput.

<details>
<summary>Answer</summary>

Compare the **unit of state**. Kafka tracks progress as a stream plus an offset — one number for a whole group. RabbitMQ tracks it per message: delivered, acked, requeued, retried how many times. So: if the problem needs control over individual messages, you are fighting Kafka's design. If it needs one event stream that several independent consumers share and can replay, you are fighting RabbitMQ's.

</details>

**M0.5** — Why does Kafka have no per-message retry, delay or priority? Tie it to the design rather than calling it a missing feature.

<details>
<summary>Answer</summary>

Those features all require the broker to hold state per message and to hand records out in an order other than the one they were written in. Kafka's broker holds one offset per partition per group and serves a file sequentially — that is exactly why it can serve reads at near disk speed with page cache and zero-copy. Per-message semantics are not missing; they were traded away for that property.

</details>

**M0.6** — Adding a new downstream consumer six months after launch: what does that cost in Kafka, and what does it cost in RabbitMQ?

<details>
<summary>Answer</summary>

In Kafka it is a new consumer group reading from offset 0 — it gets the full history still inside retention, and nobody already running is affected. In RabbitMQ the consumer list is decided at publish time: an exchange has to copy each message into one queue per consumer, so a queue bound today starts empty and everything published before it is gone. Adding a late consumer there is a producer change plus a backfill from somewhere else.

</details>

### Senior

**M0.7** — When would you advise a team against Kafka? Give three concrete situations.

<details>
<summary>Answer</summary>

**Low-latency request/reply** — Kafka is not RPC; a round trip through a log adds batching, replication and poll latency and gives you no correlation or timeout semantics. **Work needing per-task control** — its own retry, delay, priority or dead-letter path, especially with a worker count that swings; you would rebuild that machinery yourself. **Small scale, one consumer, no replay requirement** — the deciding cost is operational, not technical: a cluster to run, partition counts that cannot be undone, and someone who can diagnose a shrinking ISR at 2am. Three more worth having ready: fan-out to end clients, querying by message content, and scheduled delivery.

</details>

**M0.8** — How do Share Groups (KIP-932) differ from consumer groups, and what do they trade away?

<details>
<summary>Answer</summary>

The difference everything else follows from is the **unit of distribution**: a consumer group distributes partitions, a share group distributes individual records. So many consumers can read one partition, parallelism is no longer capped by partition count, and progress becomes a per-record state machine — acquired → acknowledged / released / rejected, with a delivery count and an acquisition lock timeout — instead of one committed offset. The trade is usually stated as losing ordering, which is true but incomplete: **the broker now keeps state per in-flight record**, which is precisely what Kafka refused to do in order to stay fast. Share groups buy queue semantics by giving back the property that made the log cheap to serve.

</details>

**M0.9** — A team needs delayed delivery — "retry this in 30 minutes" — on Kafka. Design it, and state what you give up.

<details>
<summary>Answer</summary>

The obvious design fails: you cannot publish to a delay topic and poll it checking timestamps, because a partition is a sequential log and a consumer cannot skip its head. A record due in 29 minutes blocks every record behind it — head-of-line blocking — and waiting inside the poll loop exceeds `max.poll.interval.ms`, so the consumer is evicted and the group rebalances. The working design is **tiered delay topics** (5s / 1m / 5m / 30m) so each topic holds a roughly uniform remaining wait, bounding the blocking to that tier's granularity; a consumer per tier reads the head, and when the record is not due it calls `pause()` on the partition and `seek()` back to that offset while continuing to `poll()` so heartbeats survive, then republishes to a smaller tier or the destination when due. Costs: ordering along the delay path, retention on every delay topic must exceed the longest delay or messages expire before firing, republishing is at-least-once so the terminal consumer must be idempotent, and precision is bounded by tier granularity. The simpler alternative, and usually the right one: a `due_at` column in a database plus a poller — "find the rows due now" is a query a database is built for and a log is not.

</details>

**M0.10** — Your company already runs RabbitMQ. A team wants to add Kafka for an event stream. Argue both sides of running two brokers.

<details>
<summary>Answer</summary>

**For:** an event stream is a different problem from task dispatch. On a queue the consumer list is fixed at publish time, so a consumer added later sees no history; in Kafka it is a new group from offset 0 at no cost to anyone. Reprocessing after a bug becomes the same mechanism rather than a recovery project, and retention stops being a liability and becomes the feature. **Against:** two brokers is two sets of operational knowledge, two failure modes, two upgrade paths, two monitoring stacks and two runbooks — paid continuously by everyone, not once by the team that wanted it. Kafka's failure modes are also unfamiliar and subtle: ISR shrink, rebalance storms, lag that means something other than a slow consumer, a partition count that cannot be lowered. **The option usually left unexamined:** RabbitMQ Streams has existed since 3.9 — an append-only replicated log with consumer-controlled offsets and replay, with superstreams for partitioning — so the log semantics may already be running in production. Comparing Kafka against RabbitMQ *queues* answers the wrong question. **Decide with a test, not a preference:** add Kafka only when more than one independent consumer of the data exists today, replay is a stated requirement, the history itself has value, and somebody will own it operationally by name. If Streams covers the first three, the answer is no.

</details>


## M1 · Core concepts

### Junior

**M1.1** — Define topic, partition, offset and replica, and the relationships between them.

<details>
<summary>Answer</summary>

A **topic** is purely logical — a name; no file on disk is named after it. A **partition** is the real object: one append-only log on disk, and a topic is a set of them. An **offset** is a monotonically increasing integer scoped to one partition, so a record is uniquely identified by `(topic, partition, offset)` and there is no topic-wide offset. A **replica** is a copy of one partition on another broker; `replication.factor` applies per partition, with one leader serving all reads and writes and the followers fetching from it.

</details>

**M1.2** — Why can't the number of useful consumers in a group exceed the partition count?

<details>
<summary>Answer</summary>

Within one group each partition is assigned to exactly one consumer, so that assignment can never give a fifth consumer anything on a four-partition topic — it simply sits idle. Partition count is therefore the parallelism ceiling for a group, and it has to be chosen when the topic is created because raising it later is a one-way door that breaks key ordering.

</details>

**M1.3** — What is the message key for? What happens when it is null?

<details>
<summary>Answer</summary>

The key selects the partition via `toPositive(murmur2(keyBytes)) % numPartitions`, so the same key always lands on the same partition — that is the only mechanism Kafka offers for ordering related records. With a null key the **sticky partitioner** takes over: it fills one partition until the batch is full or `linger.ms` expires and only then switches, which is batching behaviour and not per-record round-robin.

</details>

### Mid

**M1.4** — What are the consequences of increasing the partition count on a live topic? Can you decrease it, and why not?

<details>
<summary>Answer</summary>

Changing the partition count changes the divisor in the key hash, so roughly half the keys move to a different partition while their older records stay where they were — the consumer stops seeing a key's events in order across the cutover. You cannot decrease it: each partition is its own log with its own offset sequence, and merging two logs would mean inventing offsets that consumers have already committed against. The only order-preserving way to resize is a new topic plus a controlled migration.

</details>

**M1.5** — Read `Replicas: 2,3,1 · Isr: 2,3` — what does it tell you?

<details>
<summary>Answer</summary>

Three replicas are configured but only two are in sync: broker 1 has fallen behind by more than `replica.lag.time.max.ms` and has been dropped from the ISR. Broker 2 is first in the replica list, so it is the preferred leader. The practical consequence is that with `min.insync.replicas=2` you are one failure away from producers being rejected, and `acks=all` now means only those two replicas — the redundancy you think you have is not the redundancy you have.

</details>

**M1.6** — Two groups read the same topic. Where is each group's progress kept, and how does Kafka keep them from interfering?

<details>
<summary>Answer</summary>

Each group's committed offsets live in the internal compacted topic `__consumer_offsets`, keyed by `(group.id, topic, partition)`, so the groups never share a row and reading is non-destructive anyway. That is how one topic serves both shapes at once: consumers inside a group split partitions (queue), while separate groups each see the full stream (pub/sub). No exchange or copy is involved.

</details>

### Senior

**M1.7** — A topic has 3 partitions and you need 30 consumers working in parallel while preserving order per user_id. How? Give the trade-offs of each option.

<details>
<summary>Answer</summary>

**Repartition to 30+ via a new topic** — keyed by `user_id`, migrated deliberately; preserves ordering going forward but is a migration, and the old data's ordering across the cutover is lost. **Fan out inside the consumer** — one consumer per partition that dispatches to a worker pool keyed by `user_id`, so each key is handled by exactly one worker sequentially; keeps 3 partitions but you now own the in-process queueing, backpressure and offset-commit discipline (commit only up to the lowest unfinished record). **Share group** — removes the partition ceiling outright, but gives up ordering entirely, so it answers the parallelism half and fails the ordering half. **Two-stage pipeline** — consume 3 partitions and re-key into a wider internal topic; buys clean ordering at the cost of an extra hop, extra storage and at-least-once semantics between stages. The honest framing is that 3 partitions is the real constraint and only the first and fourth options actually remove it.

</details>

**M1.8** — At what scope does Kafka guarantee ordering? Name three ways to lose it even with the key set correctly.

<details>
<summary>Answer</summary>

Ordering is guaranteed **within a partition only**, and only if the producer is configured correctly — never across partitions or topic-wide, because there is no shared clock and no global sequencer. Ways to lose it with a correct key: producer retries with `max.in.flight.requests.per.connection > 1` and idempotence off, so a later request lands ahead of one being retried; raising the partition count, which moves the key while its old records stay behind; and handing records to a thread pool after `poll()`, where Kafka delivered them in order and the application reordered them. A fourth: consuming through a share group, which trades ordering for parallelism.

</details>

**M1.9** — Why is Kafka fast despite writing to disk? Answer at the mechanism level.

<details>
<summary>Answer</summary>

Appending to a log is sequential I/O, which on spinning disks and SSDs alike is an order of magnitude faster than random access — but that is only the first third of the answer. Writes go to the OS **page cache** rather than being fsynced per record, so the broker returns as soon as the data is in memory and durability comes from replication to other brokers instead of from flushing; a broker restart loses nothing because the page cache is the kernel's, not the process's. Reads of recent data are served from that same page cache, and Kafka sends them with **`sendfile`** so bytes go from page cache straight to the socket without being copied into user space or re-encoded — the broker never materialises the records at all. On top of that, records travel in **compressed batches** written to the log in the exact wire format the producer sent and the consumer expects, so the broker does no per-record work: no parsing, no re-framing, no per-message bookkeeping. The JVM heap stays small because the data lives in the kernel's cache, which also keeps GC pauses out of the path.

</details>


## M2 · Storage internals

### Junior

**M2.1** — What files make up one partition on disk, and what is each for?

<details>
<summary>Answer</summary>

A directory named `<topic>-<partition>`, holding segments. Each segment is three files sharing a base-offset name: `.log` with the record batches, `.index` mapping offsets to byte positions, and `.timeindex` mapping timestamps to offsets. The filename is the offset the segment starts at, zero-padded, which is how the broker picks the right segment by binary-searching filenames before opening anything.

</details>

**M2.2** — What is the active segment, and what makes the broker roll to a new one?

<details>
<summary>Answer</summary>

The newest segment — the only one open for writing. It rolls when either `segment.bytes` (default 1 GiB) or `segment.ms` (default 7 days) is reached. The active segment is never deleted by retention and never compacted, which is why both limits matter operationally and not just as trivia.

</details>

**M2.3** — What deletes data from a Kafka topic?

<details>
<summary>Answer</summary>

Retention policy, never consumption. `retention.ms` deletes segments older than an age; `retention.bytes` caps bytes per partition and drops the oldest first. With `cleanup.policy=compact` the question changes from age to key: the cleaner keeps only the newest record per key. In every case deletion happens at segment granularity, not per record.

</details>

### Mid

**M2.4** — Why is the offset index sparse rather than one entry per record?

<details>
<summary>Answer</summary>

One entry roughly every `log.index.interval.bytes` (4 KiB) instead of per record. A dense index would cost memory proportional to record count, and these files are memory-mapped, so it would compete for exactly the page-cache memory that makes reads fast. Sparse lookup plus a short forward scan in the `.log` is the better trade — which is why the final step of resolving an offset is a scan, not a direct hit.

</details>

**M2.5** — You set retention.ms=60000 on a low-traffic topic and nothing is deleted. Why?

<details>
<summary>Answer</summary>

Deletion works at segment granularity and never touches the active segment. On a low-traffic topic the active segment has not rolled, because rolling needs `segment.bytes` (1 GiB) or `segment.ms` (7 days) — so every record is eligible and none can be removed, since they share a file with records that are not. The fix is to lower `segment.ms` so segments close. Retention is a floor on data lifetime, never a ceiling: the real guarantee is at least `retention.ms`, plus up to one segment's worth of time, plus up to one `log.retention.check.interval.ms`.

</details>

**M2.6** — What is a tombstone, and how long does it live?

<details>
<summary>Answer</summary>

A record with a null value on a compacted topic. It marks the key as deleted; the cleaner removes both the tombstone and every earlier record for that key. It survives for `delete.retention.ms` (default 24 hours) after the key becomes cleanable, so that consumers have a window to observe the deletion — a consumer offline longer than that can miss the delete entirely and keep a stale value, which is a real correctness bug in CDC and cache-invalidation pipelines.

</details>

### Senior

**M2.7** — Explain why Kafka is fast as four distinct mechanisms, and say how the fourth makes the third possible.

<details>
<summary>Answer</summary>

**Sequential I/O** — appends are sequential and the index files keep reads sequential too, resolving an offset to a byte position rather than walking the log. **Page cache** — writes are not fsynced per record; durability comes from replication, so a broker process crash loses nothing (the cache is the kernel's), and the JVM heap stays small, keeping GC out of the request path. **Zero-copy** — recent data is already in page cache in the format the consumer expects, so `sendfile` moves it straight to the socket with no user-space copy and no deserialisation. **Batching** — records are stored and shipped as compressed batches in a format byte-identical to the wire format, so the broker does no per-record work at all. That identity is what makes zero-copy possible: any parsing or re-framing would force the broker to touch the bytes. The through-line is that Kafka is fast because the broker was given almost nothing to do, and all four follow from refusing to keep per-message state.

</details>

**M2.8** — When does Kafka not get zero-copy, and what does it cost?

<details>
<summary>Answer</summary>

When the connection is encrypted. TLS has to encrypt the bytes, which means pulling them into user space, so `sendfile` cannot be used and both throughput and broker CPU degrade measurably — a capacity-planning input rather than trivia, and the reason an encrypted cluster is not a drop-in swap for a plaintext one. Historically there was a second case: a client requesting an older message format forced the broker to down-convert, losing zero-copy and spiking CPU. Kafka 4.0 removed message formats v0 and v1 (KIP-724) along with `message.format.version`, so on a 4.x broker that case no longer exists.

</details>

**M2.9** — A compacted topic feeds a cache. What correctness problems does compaction introduce that a consumer author must handle?

<details>
<summary>Answer</summary>

**Offsets have gaps** — compaction keeps original offsets and removes records between them, so any logic assuming offset+1, or counting records to infer progress, is wrong. **Intermediate values disappear** — a consumer reading from the tail sees only the latest value per key, so it cannot be used to reconstruct a history or to react to every transition; only a consumer keeping up at the head sees them all. **Tombstones expire** — after `delete.retention.ms` the tombstone is gone, so a consumer offline longer than that never learns the key was deleted and keeps a stale entry forever. The fix is usually a periodic full rebuild from the compacted log rather than trusting an incrementally maintained cache. **The active segment is never compacted**, so the head always holds uncompacted duplicates: handlers must be idempotent, since the same key arrives repeatedly.

</details>

**M2.10** — Make the case for tiered storage, and say what it costs.

<details>
<summary>Answer</summary>

Two arguments, and the second is the stronger one. **Cost:** long retention stops dictating broker disk size — keeping a year of history becomes an object-storage bill instead of a cluster-sizing problem. **Recovery time:** replacing a broker previously meant replicating every byte it held, so rebuild time scaled with retention; with most history remote a new broker pulls only the local window, dropping rebuild from hours to minutes. That also removes a hidden constraint where long retention made clusters slow to heal and therefore riskier to operate. The costs: a read-latency cliff for consumers that fall behind into remote segments, a second storage system in the failure and security surface, and no support for compacted topics. The honest framing is that tiered storage decouples retention from cluster size — if retention is not the thing sizing your cluster, it buys you little.

</details>
