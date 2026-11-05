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


## M3 · Replication & ISR

### Junior

**M3.1** — What is the ISR, and what puts a replica in or out of it?

<details>
<summary>Answer</summary>

The in-sync replica set: the replicas currently keeping up with the leader. A follower stays in while it has fetched up to the leader's log end offset within `replica.lag.time.max.ms` (default 30s). A GC pause, slow disk, saturated network or restart pushes it out; catching up puts it back. It is not the same as the replica list, and it changes continuously — almost every durability question reduces to how large the ISR was at the moment of failure.

</details>

**M3.2** — What do acks=0, acks=1 and acks=all mean?

<details>
<summary>Answer</summary>

`acks=0`: the producer does not wait at all, so a network blip loses records with no retry possible. `acks=1`: the leader responds once it has written to its own log, so a leader failure before replication loses an acknowledged write silently. `acks=all`: the leader responds once every replica **in the ISR** has the record. Since Kafka 3.0 the producer defaults are `acks=all` and `enable.idempotence=true` (KIP-679) — older material saying the default is `acks=1` is out of date.

</details>

**M3.3** — What does replication.factor=3 actually give you?

<details>
<summary>Answer</summary>

Three copies of each partition on three brokers: one leader serving all reads and writes, two followers fetching from it. It lets the cluster lose one broker and still have a choice of in-sync replica to promote. It does not by itself guarantee no data loss — that depends on `acks` and `min.insync.replicas`, since a replica that exists but has fallen out of the ISR protects nothing.

</details>

### Mid

**M3.4** — What is the high watermark, and why can't a consumer read past it?

<details>
<summary>Answer</summary>

The high watermark is the minimum log end offset across all in-sync replicas — the furthest point consumers may read. A record that exists only on the leader is not durable yet: if the leader dies, a follower that never received it becomes leader and the record is gone. Serving it would let a consumer read something that later ceases to exist, making the log appear to go backwards. Holding reads at the HW means anything visible has reached every in-sync replica. The cost is latency: a record becomes readable one replication round-trip after it is produced.

</details>

**M3.5** — Why does acks=all on its own not guarantee durability?

<details>
<summary>Answer</summary>

Because it means all replicas *in the ISR*, not all replicas. If the followers fall behind and drop out, the ISR is just the leader, and `acks=all` is satisfied by the leader alone — it has silently degraded to `acks=1`, with no error, at exactly the moment the cluster is least healthy. `min.insync.replicas=2` closes this: the write is rejected with `NOT_ENOUGH_REPLICAS` rather than being accepted under-replicated. The pair is the configuration; either one alone is not.

</details>

**M3.6** — Reading `Replicas: 2,3,1 · Isr: 2,3` with min.insync.replicas=2 — what is the operational situation?

<details>
<summary>Answer</summary>

Broker 1 has fallen more than `replica.lag.time.max.ms` behind and been removed from the ISR, so the partition is under-replicated. Writes still succeed, because the ISR size equals `min.insync.replicas` — but there is no margin left: one more failure and `acks=all` producers start getting `NOT_ENOUGH_REPLICAS`. Broker 2 is first in the replica list, so it is the preferred leader and currently holds leadership. This is the state `UnderReplicatedPartitions` is non-zero for, and it is a warning rather than an outage.

</details>

### Senior

**M3.7** — Walk through the configurations under which Kafka can lose an acknowledged write, and say which one Kafka's design actually intends.

<details>
<summary>Answer</summary>

**acks=1** — the leader acknowledges before replicating; it dies, a follower is promoted, the record is gone and the producer was told it succeeded. **acks=all with min.insync.replicas=1** — the ISR shrinks to the leader alone and `acks=all` silently becomes `acks=1`, so the same loss happens with a configuration that looks safe. **Unclean leader election enabled** — the whole ISR is lost, an out-of-sync replica is promoted, and every record beyond its log end offset is gone permanently; consumers that already read them see the log move backwards. **Correlated power loss across the ISR** — Kafka does not fsync per record, so durability is replication rather than flushing; if every replica is in one rack or availability zone and loses power together, acknowledged data can go. That is what `broker.rack` and rack-aware placement exist for. The intended configuration is RF 3, `min.insync.replicas=2`, `acks=all`, unclean election off, replicas spread across racks — and what it says is that **Kafka's durability story is to refuse writes rather than lose them**. Every setting above either upholds that or deliberately opts out.

</details>

**M3.8** — A partition's whole ISR is gone and only an out-of-sync replica survives. Walk the decision.

<details>
<summary>Answer</summary>

The choice is between the partition staying offline until an in-sync replica returns (`unclean.leader.election.enable=false`, the default) and promoting the stale replica immediately (`true`). Offline means no acknowledged write is lost but the partition may be unavailable for a long time — possibly until a specific dead broker is recovered. Promoting means writes resume instantly and every record the old leader held beyond the stale replica's log end offset is permanently gone, with consumers that already read them seeing the log go backwards. There is no third option and no setting that avoids the trade. The senior answer is per topic class, not a preference: clickstream, metrics and logs where a gap is survivable and downtime is not — enable it; a payment ledger or anything another system reconciles against — never, take the outage and escalate. Saying "it depends" without naming the dimension is not an answer; the dimension is whether a downstream consumer treats the log as a source of truth.

</details>

**M3.9** — Why was leader epoch introduced, and what was broken before it?

<details>
<summary>Answer</summary>

When a failed replica rejoins it may hold records the new leader never had, so it must truncate before following — the question is to what offset. Originally it truncated to its own high watermark, but the HW propagates to followers one round-trip after it advances, so during a double failure two replicas could disagree about it: committed records could be lost, or the two logs could silently diverge while both believed they were in sync. Leader epoch (KIP-101, Kafka 0.11) replaced the guess with a fact: every leader election increments an epoch, each replica keeps a `leader-epoch-checkpoint` mapping epochs to the offsets they began at, and a recovering follower asks the leader where epoch N ended and truncates to exactly that offset. It is not configurable. It is worth knowing because it is the honest answer to whether Kafka has ever lost data in normal operation — it has, before 0.11, through exactly this path.

</details>

**M3.10** — A pager fires: UnderMinIsrPartitionCount is 4 and producers are erroring. Diagnose it.

<details>
<summary>Answer</summary>

`UnderMinIsrPartitionCount > 0` means the ISR has fallen below `min.insync.replicas` on those partitions, so `acks=all` producers are being rejected with `NOT_ENOUGH_REPLICAS` right now — the cluster is working as designed, refusing writes rather than losing them. First establish whether brokers are down (`OfflinePartitionsCount`, broker liveness) or whether brokers are alive but lagging, which `IsrShrinksPerSec` distinguishes. Alive-but-lagging points at the follower fetch loop: GC pauses, a saturated network link, a disk that has slowed, or a single broker holding too many leaders after an earlier failover. Mitigate by restoring replicas, not by lowering `min.insync.replicas` — that converts a loud failure into a silent one and is the wrong instinct under pressure. Afterwards, check leadership distribution, because leaders concentrate on whichever brokers recovered first; `auto.leader.rebalance.enable` handles it eventually, and `kafka-leader-election.sh --election-type preferred` forces it. The thing to say out loud: rejected writes are the system doing its job, and the incident is the under-replication, not the errors.

</details>


## M4 · KRaft & the metadata plane

### Junior

**M4.1** — What did ZooKeeper store for Kafka, and what did it not touch?

<details>
<summary>Answer</summary>

It held the control plane: which brokers are alive, which topics exist, partition assignments, who leads each partition, the ISR, topic configs and ACLs. One broker was elected controller and was the only component allowed to write that state. It never touched the data plane — producing and consuming went broker to broker — which is why large clusters ran fine on a modest ZooKeeper ensemble.

</details>

**M4.2** — In KRaft, where does cluster metadata live?

<details>
<summary>Answer</summary>

In `__cluster_metadata`, a single-partition internal Kafka topic replicated by Raft across the controller nodes. The Raft leader is the active controller; the other controllers are followers holding the same log. Brokers are observers: they fetch that log like consumers and apply the records in order.

</details>

**M4.3** — What is the difference between combined and isolated mode, and how many controllers should a quorum have?

<details>
<summary>Answer</summary>

`process.roles` sets it. Isolated means a node is either `controller` or `broker`; dedicated controllers are small, cheap and insulated from data-plane load, and this is the production shape. Combined means `broker,controller` on one node — fewer machines, fine for development, with the risk that data-plane load starves the controller. Quorum sizing follows Raft: N controllers tolerate (N-1)/2 failures, so 3 tolerates one and 5 tolerates two. Even numbers buy nothing.

</details>

### Mid

**M4.4** — How do brokers learn about metadata changes in KRaft, and why is that better than the push model?

<details>
<summary>Answer</summary>

They fetch the metadata log and apply records in order, exactly as a consumer does. Previously the controller pushed updates by RPC, and a broker that missed one had no way to notice — there was no version or offset to compare against, so divergence was possible and undetectable. With a log, each broker has an offset, so being behind becomes a measurable quantity: `MetadataLag` is a number you can graph and alert on. Metadata is eventually consistent with known lag instead of pushed with unknown reliability.

</details>

**M4.5** — Why is controller failover so much faster under KRaft?

<details>
<summary>Answer</summary>

Under ZooKeeper a new controller had to load the entire cluster state out of ZooKeeper before it could act, so failover time was O(partitions) — minutes on a large cluster, during which no leader elections or topic changes could happen. Under KRaft the standby controllers have been applying the same metadata log all along and already hold the state in memory, so promotion is a Raft election rather than a state transfer. It is milliseconds, and it no longer scales with partition count — which is also what removed the practical ~200k partition ceiling.

</details>

**M4.6** — Why does the metadata log need snapshots?

<details>
<summary>Answer</summary>

It is an append-only log of every metadata change, so it would grow without bound and replaying it from the beginning would get slower forever. Periodic snapshots capture the complete state at an offset, letting earlier log segments be discarded; a new controller or a far-behind broker loads the latest snapshot and replays only the records after it. It is the same problem log compaction solves for user topics, solved for the control plane.

</details>

### Senior

**M4.7** — Why did Kafka drop ZooKeeper? Answer at the mechanism level.

<details>
<summary>Answer</summary>

"Operational simplicity" is the consequence, not the reason. The mechanism: controller failover required loading the entire cluster state out of ZooKeeper before the new controller could act, so recovery time scaled with partition count — minutes on a large cluster, with no leader elections possible meanwhile — and that put a practical ceiling of roughly 200k partitions on a cluster, set by failover time rather than by disk or throughput. Metadata propagation was also an RPC push with no version or offset, so a broker that missed an update could not detect it. KRaft stores metadata in a Raft-replicated Kafka log, so standby controllers already hold the state and failover is an election rather than a state transfer, and brokers consume metadata as a log with an offset, which turns "behind" from an invisible condition into a measurable one. Removing the second system to operate, secure and upgrade follows from that — it is the benefit, not the design rationale.

</details>

**M4.8** — A cluster is running Kafka 3.4 on ZooKeeper and the team wants to be on 4.x. Plan it.

<details>
<summary>Answer</summary>

The constraint that shapes everything: 4.0 removed ZooKeeper support outright, so there is **no direct upgrade path**. The cluster must reach a 3.x version that supports migration, complete the KRaft migration there, verify, and only then upgrade to 4.x — two separate changes that must not be combined. Shape: upgrade 3.4 to a late 3.x on ZooKeeper first, since mixing a version upgrade with a metadata-plane migration makes any failure ambiguous. Then provision the controller quorum, start it in migration mode, let it copy metadata out of ZooKeeper, roll brokers into KRaft mode one at a time, and run in dual-write long enough to be confident before finalising — finalisation is the irreversible step, and the rollback plan exists only before it. Verify with `kafka-metadata-quorum.sh`, by watching metadata lag across brokers, and by confirming `ActiveControllerCount` is exactly 1. Only then do the 4.x upgrade. The thing to say that shows judgment: the risk is not the migration mechanics, it is combining two changes so that a problem cannot be attributed to either.

</details>

**M4.9** — ActiveControllerCount is 2 across the cluster. What does that mean and how bad is it?

<details>
<summary>Answer</summary>

Exactly one node should report 1 and all others 0, so a cluster-wide sum of 2 means two nodes each believe they are the active controller — split brain in the control plane. Raft is designed to make this impossible, since becoming leader requires a majority of the quorum and two leaders in the same epoch cannot both have one, so the realistic explanations are a partial network partition where a stale leader has not yet learned it was superseded, a metrics-collection artifact summing across a window during an election, or a node that has been excluded from the quorum but still reports its old state. Severity depends on whether both are accepting writes: metadata writes go through Raft, so a stale leader cannot commit without a majority, which means genuine divergent metadata is unlikely and the real exposure is confusion and delayed elections rather than corruption. Diagnose with `kafka-metadata-quorum.sh --describe --status` from several nodes — the quorum's own view of leader and epoch is authoritative, where the per-node metric is only that node's belief. A brief 2 during an election is noise; a sustained 2 is a partition to find.

</details>

**M4.10** — Does KRaft change the durability story from M3 at all?

<details>
<summary>Answer</summary>

For user data, no — partition replication, ISR, `acks` and `min.insync.replicas` are unchanged, and KRaft is the control plane only. What changes is the failure behaviour *around* durability. Losing the controller quorum does not lose data or stop in-flight produce and consume on existing leaders, but it does stop every metadata operation: no leader elections, no ISR changes being recorded, no topic creation, no broker joining. So a partition leader failing while the quorum is down means that partition stays leaderless until the quorum returns — the data plane keeps running on what it already knows and cannot recover from anything new. The faster failover matters here for the same reason: under ZooKeeper, a controller failover window of minutes was a window in which no partition could recover from a broker failure, so the two failure modes compounded. Being able to separate "the data plane is unaffected" from "recovery is frozen" is the answer; treating controller loss as either harmless or catastrophic are both wrong.

</details>


## M5 · Producer internals

### Junior

**M5.1** — What happens when you call send()?

<details>
<summary>Answer</summary>

Nothing is sent. The record is serialized, assigned a partition, appended to an in-memory batch in the RecordAccumulator, and a future is returned. A separate Sender thread drains ready batches, groups them per broker, issues the requests, and completes the futures. Two consequences: callbacks run on the Sender thread, so blocking in one stalls all sending; and a producer killed without `close()` or `flush()` loses whatever is still in the accumulator, even though `send()` appeared to accept it.

</details>

**M5.2** — What do batch.size and linger.ms control?

<details>
<summary>Answer</summary>

`batch.size` (16 KiB) is the maximum bytes **per partition** in one batch — not a global size and not a record count. `linger.ms` (0 in the Java client) is how long to wait for more records before sending a partially full batch. A batch goes when it is full or linger expires, whichever comes first.

</details>

**M5.3** — Which compression codec would you pick, and why does it belong on the producer rather than the topic?

<details>
<summary>Answer</summary>

`lz4` when CPU matters most, `zstd` when storage and cross-AZ bandwidth matter — which is usually the right default now; `gzip` is rarely worth its CPU any more. It belongs on the producer because compression is applied per batch before the record is sent, and the broker stores those bytes untouched. Setting a different codec on the topic forces the broker to decompress and recompress every batch, costing CPU and losing zero-copy.

</details>

### Mid

**M5.4** — Does linger.ms=0 mean no batching?

<details>
<summary>Answer</summary>

No. While a request is in flight to a broker, records keep accumulating, and the next request takes everything that arrived meanwhile — so under load you get large batches with zero configured linger. Batching is driven by the in-flight window, not only by the timer. Raising `linger.ms` buys bigger batches specifically at low load, where there is nothing in flight to accumulate behind, at the cost of a latency floor.

</details>

**M5.5** — A service using a Kafka producer gets slow, with no increase in its own workload. What would you check first?

<details>
<summary>Answer</summary>

Whether `send()` is blocking on `buffer.memory`. The accumulator is a fixed pool (32 MiB by default); when it fills because the broker is slow, a partition leader is unavailable, or the application outpaces the network, `send()` stops being asynchronous and blocks for up to `max.block.ms` (60 s) before throwing. That propagates broker backpressure straight into application latency through a call everyone assumes is non-blocking. `max.block.ms` also covers waiting for topic metadata, so producing to a topic that does not exist looks identical.

</details>

**M5.6** — Someone sets retries=3 to make the producer give up quickly. What actually happens?

<details>
<summary>Answer</summary>

Almost nothing they intended. `retries` defaults to `Integer.MAX_VALUE` and has not been the limit since `delivery.timeout.ms` was introduced — that setting (120 s by default) bounds the total time from `send()` to success or failure, retries included. Setting `retries=3` can make things worse by giving up on a transient leader election that would have resolved, while the producer still waits out the timeout structure around it. To give up after ten seconds, set `delivery.timeout.ms=10000`.

</details>

### Senior

**M5.7** — Explain the idempotent producer's mechanism, and say precisely what it does not give you.

<details>
<summary>Answer</summary>

The producer obtains a **producer id** (PID) and an **epoch**. Every batch carries the PID, the epoch and a **per-partition sequence number**. The broker tracks the last sequence per (PID, partition): a batch whose sequence it has already seen is acknowledged and discarded, so the duplicate never reaches the log, and a batch arriving ahead of the expected sequence is rejected with `OutOfOrderSequenceException`. Because the broker enforces both dedupe and order, `max.in.flight` up to 5 stays safe instead of being forced to 1 — out-of-order arrivals are rejected and retried in order rather than written in the wrong order. What it does **not** give you: it does not survive a producer restart, since a new instance gets a new PID and the broker cannot recognise what the old one wrote, so a crash-and-retry duplicates; it is not cross-partition atomicity, so two records to two partitions can still end with one written and one not — that is transactions; and it is not end-to-end exactly-once, since an application that calls `send()` twice has sent two records and the broker is right to keep both. The line worth having: idempotence makes a retry safe, not a system exactly-once.

</details>

**M5.8** — Trace how the null-key partitioner has changed, and why it changed twice.

<details>
<summary>Answer</summary>

**Before 2.4** it was round-robin per record, so a modest produce rate spread records across every partition and produced many tiny batches — bad for throughput, bad for compression, and more requests per byte. **2.4 (KIP-480)** introduced the sticky partitioner: fill one partition's batch, send it, then switch. Far fewer and far larger batches for the same traffic. **3.3 (KIP-794)** replaced it with the strictly uniform sticky partitioner, because the 2.4 version allocated *batches* equally while slow brokers drained slower — so a struggling broker received proportionally more bytes, exactly backwards. The current partitioner assigns by how much each partition has actually accepted, and `partitioner.adaptive.partitioning.enable` steers away from slow brokers. `DefaultPartitioner` is deprecated; leaving `partitioner.class` unset gets the current behaviour. The reason this is worth knowing is the second change: the first fix optimised the wrong quantity, and noticing that took production experience rather than design review.

</details>

**M5.9** — A producer reports RecordTooLargeException for some records but not others, intermittently. Diagnose it.

<details>
<summary>Answer</summary>

Two limits in different places, and the error can come from either. `max.request.size` (producer, 1 MiB) is checked client-side and fails immediately. `message.max.bytes` (broker or topic) is checked after the record crosses the network. If only one was raised, failures appear at whichever is lower — and a topic-level override means it can differ per topic, which is one source of the intermittency. The other is that the limit applies to the **compressed batch**, not the individual record, so a record that fits alone can fail when batched with others, and whether it does depends on timing and on how well that particular batch compressed. That is why it looks intermittent for the same payload size. Check both settings and any topic override, compare against the compressed batch size rather than the record size, and remember the consumer side: `fetch.max.bytes` and `max.partition.fetch.bytes` must also accommodate it, or the symptom moves from an error to a consumer that silently stalls on a message it cannot fetch.

</details>

**M5.10** — You need the lowest possible end-to-end latency from a producer. What do you change, and what is the floor you cannot get past?

<details>
<summary>Answer</summary>

Producer side: `linger.ms=0`, a smaller `batch.size` so nothing waits to fill, `compression.type=none` or `lz4`, and keep idempotence on since it costs nothing in latency. Avoid the mistakes that add latency while looking like tuning: a large `buffer.memory` does not help latency and hides backpressure, and `max.in.flight=1` to "be safe" serialises requests and adds a round-trip per batch for a guarantee idempotence already provides. The floor is not on the producer at all. With `acks=all` the leader cannot respond until every in-sync replica has the record, so a replication round-trip is in the critical path — and from M3, a consumer cannot read past the high watermark, which needs that same round-trip to advance. So end-to-end latency has a hard floor of roughly one replication round-trip plus the consumer's `fetch.max.wait.ms` for an empty fetch. Dropping to `acks=1` removes the first half and buys a silent data-loss window, which is almost never the right trade. The senior move is to say which half of the latency is yours to tune and which is the durability guarantee showing itself, and to ask whether the latency requirement is really end-to-end or just produce-ack — people routinely measure the second and specify the first.

</details>


## M6 · Consumer internals

### Junior

**M6.1** — What does poll() actually do?

<details>
<summary>Answer</summary>

It returns records from a local buffer that a background fetcher keeps filling with in-flight requests to the brokers leading the assigned partitions. If the buffer has data, `poll()` returns immediately with no network call. Since 0.10.1 heartbeats are sent from a separate background thread rather than from `poll()`, which is why liveness and progress became two different things with two different timeouts.

</details>

**M6.2** — Where are consumer offsets stored, and what does a committed offset mean?

<details>
<summary>Answer</summary>

In `__consumer_offsets`, an internal compacted topic with 50 partitions, keyed by (group, topic, partition). The partition a group's offsets land in also picks its group coordinator — the broker leading that partition. A committed offset is **the next record the group wants**, not the last one processed; getting that off by one reprocesses or skips exactly one record per commit, forever.

</details>

**M6.3** — When does auto.offset.reset apply, and which value is dangerous?

<details>
<summary>Answer</summary>

In exactly two situations: the group has no committed offset, or its committed offset is out of range because retention deleted that data. `earliest` reprocesses from the start, `latest` jumps to the end, `none` throws. `latest` is the dangerous one because its failure is silent — a consumer that was down longer than retention quietly skips everything it missed and reports no error.

</details>

### Mid

**M6.4** — Explain session.timeout.ms versus max.poll.interval.ms.

<details>
<summary>Answer</summary>

`session.timeout.ms` (45 s) is process liveness: no heartbeat within it and the coordinator declares the member dead. Heartbeats come from a background thread, so this fires when the process is gone or unreachable. `max.poll.interval.ms` (5 min) is application progress: no call to `poll()` within it and the member is evicted even though it has been heartbeating normally. The second exists precisely because the first cannot catch a consumer that is alive but wedged in processing. Note that `session.timeout.ms` was raised from 10 s to 45 s in Kafka 3.0 (KIP-735) because the old value evicted healthy consumers during ordinary GC pauses.

</details>

**M6.5** — What is wrong with enable.auto.commit=true?

<details>
<summary>Answer</summary>

It commits the offsets that were **returned** by the last poll, not the ones you finished processing, on a timer during `poll()`. So a crash loses up to `auto.commit.interval.ms` (5 s) of work that was already committed as done — at-least-once with a window you did not choose and cannot see. It is not wrong for idempotent consumers that genuinely do not care; it is wrong whenever the commit is being read as a statement about processing, which is how most people read it.

</details>

**M6.6** — What does cooperative sticky rebalancing change, and what does static membership change?

<details>
<summary>Answer</summary>

Cooperative sticky (KIP-429, 2.4) makes a rebalance incremental: consumers keep partitions that are not moving and revoke only those being reassigned, over two short rounds, instead of everyone revoking everything and waiting at a join barrier. Static membership (KIP-345, 2.3) gives each consumer a stable `group.instance.id` so a restart within `session.timeout.ms` triggers no rebalance at all — the returning member reclaims its partitions. They solve different problems: the first makes rebalances cheaper, the second stops them happening for planned restarts. A 20-pod rolling deploy is 20 rebalances without it.

</details>

### Senior

**M6.7** — Walk through a rebalance storm: how it starts, why it sustains itself, and how you stop it.

<details>
<summary>Answer</summary>

**Start:** processing slows — a downstream dependency degrades, a batch is unusually large, a GC pause lands badly. One consumer exceeds `max.poll.interval.ms` and is evicted. **Sustain:** the group rebalances, so every consumer stops consuming; partitions are reassigned; work since the last commit is reprocessed, which adds load; the added load makes the next consumer exceed the interval, and the cycle repeats. The group now spends more time rebalancing than consuming and does not recover on its own — the failure feeds itself, which is what makes it a storm rather than an incident. **Stop it:** reduce `max.poll.records` so each poll-to-poll cycle is shorter and bounded; that is the lever, because the timeout measures cycle duration rather than throughput. Then find why processing slowed, since the eviction was a symptom. **The wrong fix** is raising `max.poll.interval.ms`: it ends the storm by making a genuinely stuck consumer take that much longer to detect, trading a loud failure for a slow one. Longer term: cooperative sticky so rebalances are incremental, static membership so deploys do not cause them, and idempotent processing so the reprocessing step costs correctness nothing.

</details>

**M6.8** — A consumer group shows lag of 2 million. Walk the diagnosis.

<details>
<summary>Answer</summary>

Lag is `log end offset − committed offset`, and it has at least five distinct causes that need different responses, so the first move is **lag per partition, not per group**. Even distribution across partitions with a steady climb means the consumer is genuinely slower than the producer — scale out, bounded by partition count. Even distribution with lag already falling means a producer burst and nothing to do. Lag with a high rebalance rate means the group is thrashing and barely consuming, which is the previous question. Lag with no members means nobody is assigned at all — check whether the deployment is actually running. Lag concentrated in **one partition** is a hot key or a poison message, and adding consumers will not help: one partition is one consumer. Also check whether the consumer is stalled on a message larger than `max.partition.fetch.bytes`, which produces no error and no progress — the quietest of the five. The thing to say out loud is that "lag is high, add consumers" is the right action in one of these cases and useless or harmful in the others.

</details>

**M6.9** — Why does KIP-848 move assignment to the broker, and what does that actually buy?

<details>
<summary>Answer</summary>

Under the old protocol, assignment was computed by an elected consumer — the group leader — and every membership change passed through a join barrier where the whole group waited. Two problems follow. The assignment algorithm had to be implemented in every client in every language, so behaviour differed by client and version, and a mixed-language group could not be reasoned about as one thing. And the barrier made rebalancing inherently synchronous: cooperative sticky reduced what was revoked but the protocol still had everyone meet. KIP-848 moves assignment into the group coordinator and lets consumers reconcile their own assignment asynchronously against what the coordinator publishes. No elected leader, no barrier. What it buys: rebalance behaviour becomes a broker-side property you can reason about and upgrade centrally, rather than a property of whichever client each team happened to deploy; and membership changes stop being stop-the-world at the protocol level rather than by careful convention. GA in 4.0, enabled with `group.protocol=consumer`. The honest caveat is that client support lags the broker, so in practice this is a migration rather than a switch.

</details>

**M6.10** — You need at-least-once with the smallest possible reprocessing window, and the consumer calls a non-idempotent downstream API. Design it.

<details>
<summary>Answer</summary>

The reprocessing window is the gap between the last commit and the crash, so shrinking it means committing more often — and committing after processing, never before. Disable auto-commit, process each batch, then `commitSync()`; with `max.poll.records` tuned down the batch is small and the window is bounded by one batch rather than by a 5-second timer. The cost is a commit round-trip per batch, which is the trade being made explicitly. **But the window cannot reach zero**, and that is the part the question is really about: a crash between the downstream call and the commit will always replay that batch, and no Kafka setting changes it, because the downstream system is not participating in Kafka's commit. So the design has to make replay safe rather than make it impossible. Push idempotency to the boundary — an idempotency key derived from `(topic, partition, offset)` so the downstream can recognise a replay, or a dedupe table keyed on the same thing if the API will not take one. Commit in `onPartitionsRevoked` so a planned rebalance does not replay, and do **not** commit in `onPartitionsLost`, where the partitions are already gone and the commit is both pointless and misleading. The senior framing: with a non-idempotent downstream, exactly-once is not available from the consumer side at all — Kafka transactions only cover Kafka-to-Kafka — so the correct answer is at-least-once plus idempotency at the boundary, stated as a deliberate choice rather than a limitation.

</details>


## M7 · Delivery semantics & transactions

### Junior

**M7.1** — Define at-most-once, at-least-once and exactly-once, and say which is Kafka's default.

<details>
<summary>Answer</summary>

At-most-once never duplicates and may lose — you get it by committing offsets before processing. At-least-once never loses and may duplicate — commit after processing, with `acks=all` and retries. Exactly-once neither loses nor duplicates, and in Kafka it exists only via transactions and only within Kafka. At-least-once is the default and the honest baseline; most systems that believe they have exactly-once actually have at-least-once with an idempotent sink, which is a fine answer and a different one.

</details>

**M7.2** — What are the three places duplicates come from?

<details>
<summary>Answer</summary>

**Producer retry** — the write landed but the ack was lost, so the producer sent it again; the idempotent producer solves this, within one session. **Producer restart** — a new instance gets a new producer id, so the broker cannot recognise what the old one wrote; idempotence does not help, transactions do. **Consumer reprocessing** — a crash or rebalance between processing and committing replays the batch; transactions solve it only if the output is also Kafka. Knowing which mechanism addresses which source is the distinction worth having.

</details>

**M7.3** — What does isolation.level control?

<details>
<summary>Answer</summary>

What a consumer sees of transactional writes. `read_uncommitted` (the default) returns every record, including ones from transactions that later abort — transactions are effectively invisible. `read_committed` returns only records from committed transactions; aborted ones are filtered out client-side, which is why they still occupy offsets and a `read_committed` consumer legitimately sees gaps.

</details>

### Mid

**M7.4** — What is the Last Stable Offset and why does it matter operationally?

<details>
<summary>Answer</summary>

The offset of the first record belonging to a still-open transaction. A `read_committed` consumer cannot read past it, because everything before it has a decided outcome and everything after might still abort. Operationally this means an open transaction **blocks** — not slows — every `read_committed` consumer on those partitions until it commits, aborts, or times out. A producer that crashes mid-transaction stalls downstream consumers for up to `transaction.timeout.ms` (60 s default, capped by the broker's 15-minute `transaction.max.timeout.ms`). Your transaction duration becomes downstream latency, which is the strongest argument for keeping transactions short.

</details>

**M7.5** — Walk through what happens between beginTransaction() and commitTransaction().

<details>
<summary>Answer</summary>

The producer registers each partition it writes to with the **transaction coordinator** — a broker chosen by hashing `transactional.id` to a partition of `__transaction_state`. Records are written to their partitions as normal but are not visible to `read_committed` consumers. `sendOffsetsToTransaction` adds the consumer's offsets to the same transaction. On commit the coordinator performs a two-phase commit: it writes `PREPARE_COMMIT` to `__transaction_state`, which decides the outcome durably; then writes commit **markers** — control records — into every participating partition; then writes `COMPLETE_COMMIT`. Because the state is in a replicated Kafka log, a coordinator failure does not lose the decision: the new coordinator reads the log and finishes.

</details>

**M7.6** — In a consume-transform-produce loop, which call makes it exactly-once, and what must change on the consumer?

<details>
<summary>Answer</summary>

`sendOffsetsToTransaction(offsets, groupMetadata)`. It writes the consumer's offsets into `__consumer_offsets` **as part of the transaction**, so the output records and the offset commit share one atomic outcome — crash anywhere and either both happened or neither did. The consumer must have `enable.auto.commit=false` and must stop committing its own offsets entirely: the producer now commits on its behalf. That is what closes the reprocessing window that no consumer-side setting can close on its own.

</details>

### Senior

**M7.7** — Explain zombie fencing, and the configuration mistake that silently disables it.

<details>
<summary>Answer</summary>

`initTransactions()` bumps the **epoch** associated with that `transactional.id`. Any older producer instance still holding the previous epoch is fenced: its writes are rejected with `ProducerFenced`. This solves what idempotence cannot — a process partitioned from the cluster but still running, whose replacement has already started, cannot write once the replacement has initialised, so the split-brain duplicate is refused at the broker rather than deduplicated afterwards. **The mistake:** a random, UUID-based or hostname-derived `transactional.id`. The zombie and its replacement then have different ids, so neither fences the other, and a mechanism added specifically for correctness does nothing while appearing configured. For consume-transform-produce the id must be bound to the **input partitions** an instance owns, so that whoever takes over those partitions takes over that identity. This is also why Kafka Streams' EOS matters: before KIP-447 it needed one producer per input partition, so a task over 100 partitions meant 100 producers; `exactly_once_v2` uses one producer per instance and derives fencing from consumer group metadata instead. `exactly_once` and `exactly_once_beta` were removed in 4.0, leaving `exactly_once_v2` as the only value.

</details>

**M7.8** — Someone says their pipeline is exactly-once end to end, ending in Postgres. What do you ask?

<details>
<summary>Answer</summary>

Whether Postgres participates in the Kafka transaction — because it cannot. Kafka's exactly-once boundary is **Kafka-to-Kafka**: the atomic unit is output records plus the input offset commit, both inside Kafka. The moment an external system is in the path there is no distributed transaction across the two, and a crash between the database write and the offset commit replays the batch. So the real question is what makes the replay safe. Acceptable answers: an idempotency key derived from `(topic, partition, offset)` that the database enforces with a unique constraint; a dedupe table keyed the same way; or a naturally idempotent operation such as an upsert on a business key. The failure mode to probe for is a pipeline that uses Kafka transactions, concludes it has exactly-once, and writes non-idempotent inserts to Postgres — which has all the cost of transactions and none of the protection where it is actually needed. The honest framing: at-least-once plus idempotency at the boundary is the correct design, and calling it that rather than exactly-once is what keeps the next person from assuming protection that is not there.

</details>

**M7.9** — Explain the dual-write problem and why the outbox pattern solves it.

<details>
<summary>Answer</summary>

An application that writes to its database and publishes to Kafka is writing to two systems with no shared transaction. Publish first and the database write can fail, leaving an event for something that never happened. Write first and the publish can fail, leaving state nobody downstream knows about. Retries do not fix this — they only change which inconsistency you get, and a crash between the two leaves no record that the second step was owed. **Outbox:** in one database transaction, write the business row and a row to an `outbox` table. A separate process publishes outbox rows to Kafka — a poller, or CDC reading the write-ahead log with Debezium — and marks them sent. The dual write is gone because there is now one transaction in one system, and publishing becomes a retriable at-least-once delivery of something already durably recorded. Consumers must be idempotent, which they should have been regardless. CDC is usually preferable to polling: no polling lag, no load on the table, and the WAL already has exactly the ordering you want. The principle worth stating, because it generalises well past Kafka: **you cannot make two systems atomic, so arrange for only one of them to need to be.**

</details>

**M7.10** — A read_committed consumer group's lag stops falling and sits flat, with no errors anywhere. Diagnose it.

<details>
<summary>Answer</summary>

Flat lag with no errors on a `read_committed` consumer points first at a **hanging transaction**: a producer began a transaction, wrote to these partitions, and died or stalled without committing or aborting. The consumer is pinned at the **Last Stable Offset** and cannot advance past the first record of that open transaction — by design, not by failure, which is why nothing is logging an error. Confirm by comparing the LSO against the high watermark on those partitions, and check `__transaction_state` and the transactional producers for one that is stuck. Expect it to resolve itself when `transaction.timeout.ms` expires (60 s by default), so lag that is flat for *minutes* means either a long broker-side `transaction.max.timeout.ms` (up to 15 minutes) or a producer that keeps the transaction alive without completing it. Things to rule out: a `read_uncommitted` consumer on the same partitions still progressing confirms the data is there and the LSO is the cause; and remember that control records and aborted records occupy offsets, so a fully consumed transactional topic can show small non-zero lag normally — flat-and-large is the signal, not non-zero. Longer term this is the argument for short transactions, since every open one is a lower bound on downstream latency. Kafka 4.0 enables transaction version 2 (KIP-890) by default, which bumps the producer epoch per transaction and moves partition registration into a single server-side call, tightening several of the races that produced hanging transactions.

</details>


## M8 · Schemas, the registry & evolution

### Junior

**M8.1** — Why does Kafka need a schema registry at all, and is it part of Apache Kafka?

<details>
<summary>Answer</summary>

Brokers store keys and values as opaque bytes and never parse them, so nothing in Kafka checks that producers and consumers agree on what those bytes mean. A registry puts the contract somewhere: it stores versioned schemas, hands out ids, and rejects incompatible changes. It is **not** part of Apache Kafka — the common one is Confluent's Schema Registry, with Apicurio and cloud equivalents such as AWS Glue Schema Registry as alternatives. Confluent's stores its schemas in a compacted Kafka topic, `_schemas`.

</details>

**M8.2** — What does a record look like on the wire when it uses the Confluent serializers?

<details>
<summary>Answer</summary>

A magic byte `0x0`, then a 4-byte big-endian **schema id**, then the encoded payload — with Protobuf adding a small message-index array after the id to say which message in the `.proto` file it is. The deserializer reads the id, fetches the schema from the registry once and caches it (ids are immutable), then decodes. A consumer using a plain string deserializer sees five garbage bytes before each value, which is the usual first sign a topic was switched to a registry format.

</details>

**M8.3** — Name the compatibility modes and the default.

<details>
<summary>Answer</summary>

`BACKWARD` (the default) — the new schema can read data written with the previous one. `FORWARD` — the previous schema can read data written with the new one. `FULL` — both. Each has a `_TRANSITIVE` variant that checks against every earlier version rather than only the latest. `NONE` disables checking.

</details>

### Mid

**M8.4** — Which side must deploy first under BACKWARD, and under FORWARD? Why?

<details>
<summary>Answer</summary>

Under **BACKWARD**, consumers first: the guarantee is that the new reader schema handles data written with the old one, so readers must be on the new schema before any writer produces the new shape. Under **FORWARD**, producers first: old readers are guaranteed to handle new data, so writers can move ahead. Under **FULL**, either order. The mode is really a statement about deploy order — and the common mistake is choosing `BACKWARD` because it is the default and then shipping the producer first, the one order it does not protect.

</details>

**M8.5** — Give a safe and a breaking change for Avro and for Protobuf.

<details>
<summary>Answer</summary>

**Avro:** adding a field *with a default* is safe under `BACKWARD` — the new reader fills it from the default when reading old data. Adding one without a default is breaking, as is renaming without an `aliases` entry or changing a type outside Avro's defined promotions (e.g. `int` → `long` is allowed). **Protobuf:** fields are identified by number, so renaming a field or adding a new numbered field is wire-safe. Reusing a deleted field's number is the breaking change — and a silent one, since old data decodes into the wrong field — which is why deleted numbers should be marked `reserved`. Changing a field's type is generally breaking. Also worth knowing: in proto3 an absent field decodes as its zero value, so 'not sent' and 'sent as 0' are indistinguishable unless the field is `optional`.

</details>

**M8.6** — What are subject naming strategies, and when does the default become a problem?

<details>
<summary>Answer</summary>

The subject is the unit the registry versions and checks compatibility against. `TopicNameStrategy` (default) uses `<topic>-value`, so a topic has one schema lineage and therefore one record type. `RecordNameStrategy` uses the fully-qualified record name, so a type evolves independently of topics. `TopicRecordNameStrategy` uses `<topic>-<record>`, allowing several types per topic each with its own lineage. The default becomes a problem when several event types must share a topic to keep their order — `OrderCreated`, `OrderPaid`, `OrderShipped` for one order need one topic and one key, because ordering is per partition. The fix is a record-name strategy, or a single union/`oneof` envelope type with schema references under the topic subject.

</details>

### Senior

**M8.7** — Why is non-transitive compatibility a trap on Kafka in particular, and which mode would you choose?

<details>
<summary>Answer</summary>

Non-transitive modes check a new schema only against the **latest** version. v3 can be compatible with v2 and v2 with v1 while v3 cannot read v1 at all — compatibility is not transitive. On a queue that deletes on consume, nobody reads v1 data after v2 ships, so it would not matter. Kafka is a log: time-based retention keeps v1 records on disk for days, **compacted topics keep the latest value per key forever**, and new consumer groups starting from `earliest`, replays after a bug, and reprocessing all read them. So the first time a new service bootstraps from the beginning of a compacted topic, it hits records its schema cannot read — long after the change that caused it was approved by the registry. I would default to `BACKWARD_TRANSITIVE` for topics consumers own the upgrade on, or `FULL_TRANSITIVE` for widely shared topics where producers and consumers deploy independently and you cannot control the order. The cost is that it constrains evolution harder — you can never drop a default you once relied on — which is the right price for data that outlives the code that wrote it. If a genuinely breaking change is needed, it should be a new topic (or a new subject plus a migration) rather than `NONE` on the existing one.

</details>

**M8.8** — One partition of a consumer group has lag climbing steadily while all the others are at zero, and the consumer logs repeat the same error. Diagnose it.

<details>
<summary>Answer</summary>

One partition stuck with the others healthy, plus a repeating error, is a **poison pill** (M6) — and with a registry in play the most likely cause is a record that cannot be deserialized. Deserialization happens inside `poll()`, so it fails before application code runs; the Java consumer throws `RecordDeserializationException` with the partition and offset, and if nothing handles it the consumer retries the same offset forever. The other partitions keep moving, so overall throughput looks fine and only that partition's lag tells the story. Confirm by reading the error's offset and pulling the raw bytes at it: is the first byte `0x0`? Does the schema id resolve in the registry? Typical causes: a producer that bypassed the serializer and wrote raw JSON or strings; a schema id from a different registry environment (a producer pointed at staging); an incompatible schema registered with compatibility `NONE` or auto-registration from a stray build; or a schema version hard-deleted from the registry, which makes every record written with it undecodable. Immediate fix: catch the exception, `seek()` past the offset, and publish the raw bytes plus partition, offset and error to a dead-letter topic — Spring's `ErrorHandlingDeserializer` or Connect's `errors.tolerance` with a DLQ do this for you. Skipping is a decision to lose data, so the DLQ must have an owner and a replay path. Longer term: turn off `auto.register.schemas`, register schemas from CI where an incompatible change fails the build, restrict who can produce to the topic with ACLs, and alert on per-partition lag rather than group totals — the aggregate hid this.

</details>

**M8.9** — Your team wants to change the type of the message key from a string customer id to a structured Avro record. What do you say?

<details>
<summary>Answer</summary>

That it is not a schema change, it is a **repartitioning**, and it should be treated as a migration. The default partitioner hashes the key's serialized bytes, so changing how the key is serialized — even if it carries the same customer id — sends the same customer to a different partition. Ordering per customer is only guaranteed within a partition (M1), so during the switchover old and new events for one customer live in different partitions and can be consumed out of order. On a **compacted** topic it is worse: compaction keeps the latest value per key bytes (M2), so the old-format key and new-format key are different keys — the old value is never superseded, and readers see both. Stateful consumers keyed on the old form (Streams state stores, caches, joins) break too. The honest options are: keep the key as it is and put structure in the value, which is almost always right — keys should be small, stable and boring; or create a new topic with the new key, dual-write or migrate with a consumer that re-keys, move consumers over, then retire the old topic. Schema compatibility checks will not catch any of this, because a key subject can be perfectly 'compatible' while the bytes, and therefore the partitions, change.

</details>

**M8.10** — Avro, Protobuf or JSON Schema for a company's event platform — how do you decide, and what do you enforce regardless?

<details>
<summary>Answer</summary>

The format matters less than the enforcement, so I would decide on fit and then spend the effort on the process. **Avro** is the default for data platforms: smallest encoding, the strongest evolution story via writer/reader schema resolution and defaults, and first-class support in Connect, Debezium and analytics tooling. Its cost is that payloads are undecodable without the registry, and code generation is weaker in some languages. **Protobuf** fits organisations already on gRPC: one IDL for RPC and events, excellent codegen, and evolution by field number that is easy to reason about — provided nobody reuses a number, and accepting that proto3 cannot distinguish an absent field from a zero value without `optional`. **JSON Schema** gives human-readable payloads and the lowest barrier for teams, at the cost of size, slower parsing, and compatibility rules around the open/closed content model that surprise people used to Avro. Mixing formats across topics is fine; mixing within a topic is not. What I would enforce regardless: schemas in version control next to the producing service; `auto.register.schemas=false` in production, with registration and the compatibility check in CI so an incompatible change fails a build rather than a consumer; transitive compatibility because Kafka replays old data; one owning team per topic, with deprecation rather than deletion and no hard-deleting schema versions that still have data; ACLs so only the owner can produce; and a dead-letter path for records that still fail. Schemas are where the outbox pattern (M7) pays off as well — the event shape is reviewed with the business change rather than invented in a publishing helper. The point to land: a registry turns schema compatibility from a hope into a gate, but only if nothing can route around it.

</details>


## M9 · Connect, CDC & Kafka Streams

### Junior

**M9.1** — What is the difference between a connector, a task and a worker in Kafka Connect?

<details>
<summary>Answer</summary>

A **connector** is the logical job — "copy this table to that topic" — and decides how to split the work. A **task** is the unit that actually moves data; a connector creates up to `tasks.max` of them. A **worker** is the JVM process tasks run in, and workers sharing a `group.id` form a distributed Connect cluster. `tasks.max` is a ceiling: a sink on a 6-partition topic gets at most 6 useful tasks, and a Postgres CDC source reading one replication slot gets exactly one.

</details>

**M9.2** — What is the difference between a KStream, a KTable and a GlobalKTable?

<details>
<summary>Answer</summary>

A **KStream** treats each record as an independent event. A **KTable** treats each record as an update: the latest value per key wins and a `null` value deletes the key; it is materialised in a local state store over only the partitions this instance owns. A **GlobalKTable** is also a table, but every instance holds a full copy of every partition — which removes the co-partitioning requirement for joins against it, at the cost of every instance storing and reading all of it.

</details>

**M9.3** — What is CDC, and why is reading the database log better than polling a table?

<details>
<summary>Answer</summary>

Change data capture emits an event for every row change by reading the database's own replication log — the Postgres WAL via logical decoding, the MySQL binlog. Polling (`WHERE updated_at > ?`) misses deletes, misses intermediate updates between polls, needs an indexed timestamp column every writer maintains correctly, and puts query load on the database. Reading the log sees every change, including deletes, in commit order, with before and after images.

</details>

### Mid

**M9.4** — Where does a distributed Connect cluster keep its state, and what goes wrong if those topics are misconfigured?

<details>
<summary>Answer</summary>

In three compacted Kafka topics: `config.storage.topic` (connector configs, must be **one partition** because order matters), `offset.storage.topic` (source connector positions — LSN, binlog coordinates, file offsets) and `status.storage.topic` (connector and task state). Sink connectors don't use the offset topic; they are consumers and commit to `__consumer_offsets` as group `connect-<name>`. Workers are stateless and replaceable — the topics are what matter. If they use delete retention instead of compaction, a source connector's offset can expire and it restarts from scratch (for CDC, a full re-snapshot); a multi-partition config topic can reorder config updates.

</details>

**M9.5** — What is a repartition topic in Kafka Streams, when is one created, and why should you care?

<details>
<summary>Answer</summary>

When an operation changes the key — `selectKey`, `map`, `groupBy` on a new field — before a stateful operation, Streams writes the records back to an internal topic keyed by the new key, so that every record for one key reaches the same task. That is the only way an aggregation or join on the new key can be correct. You care because it is a full extra write and read of the data through the brokers, plus the extra latency and storage: a topology with three implicit repartitions moves three times the bytes it looks like it does. `Topology.describe()` shows them, and choosing the key early (or keying at the producer) often removes one.

</details>

**M9.6** — Converters versus SMTs: what does each do, and where do they stop being the right tool?

<details>
<summary>Answer</summary>

A **converter** turns bytes into Connect's internal record and back — JSON, Avro, Protobuf, String — and is where Schema Registry plugs in. An **SMT** is a stateless per-record transform: rename or drop a field, mask a value, route to another topic, extract a key, optionally gated by a predicate. SMTs stop being right as soon as you need state, lookups, joins, aggregation or one-to-many output. A long SMT chain is usually a stream processor nobody admitted to building — move that logic into Streams or Flink where it can be tested and reasoned about.

</details>

### Senior

**M9.7** — Disk on your primary Postgres is filling up, and the growth is pg_wal. You run Debezium. Diagnose it and say how you'd prevent it.

<details>
<summary>Answer</summary>

Start with the replication slots: `pg_replication_slots`, comparing each slot's `restart_lsn` / `confirmed_flush_lsn` with the current WAL position to see retained bytes per slot. Postgres keeps WAL until **every** slot has confirmed it, so one slot that stopped advancing pins WAL indefinitely. The usual causes: the Debezium task has failed or the connector is paused, and nobody alerted on task state; the connector was deleted but its slot was left behind; or — the subtle one — the connector is healthy but captures a low-traffic database on a busy server, and since WAL is server-wide, other databases' writes pile up behind a slot that never sees a change of its own to confirm. **Immediate fix:** if the connector can be restarted, restart it and watch the slot catch up; if the slot is orphaned or the disk is about to fill, drop the slot — accepting that the connector will need a re-snapshot — because the alternative is the primary database going down. **Prevention:** alert on retained WAL per slot and on Connect task state, not just on the connector existing; set Debezium `heartbeat.interval.ms` with a `heartbeat.action.query` against a heartbeat table so a quiet database still advances its slot; set `max_slot_wal_keep_size` so Postgres invalidates a runaway slot instead of filling the disk — a deliberate trade of a CDC re-snapshot for a database outage, which is the right trade; and make dropping the slot part of decommissioning a connector. The framing worth stating: CDC couples the database's availability to the consumer's health, and the slot is where that coupling lives.

</details>

**M9.8** — A Kafka Streams app with a large aggregation takes 40 minutes to recover whenever an instance dies. Why, and what are your options?

<details>
<summary>Answer</summary>

The state store is local RocksDB; its durable copy is the compacted **changelog topic**. When an instance dies, its tasks move to another instance which has no local copy, so it must replay the changelog into RocksDB before the task can process again — and recovery time is store size divided by restore throughput. Forty minutes means a big store and/or a slow restore. Options, roughly in order: **standby replicas** (`num.standby.replicas=1`) keep a warm copy on another instance so failover is a short catch-up rather than a full restore — the same trade as replication factor, paid in disk and network; Streams' assignor will also prefer moving a task to where a caught-up copy exists and use warm-up replicas when scaling. **Keep local state across restarts** — a persistent volume for the state directory, so a rolling restart or rescheduled pod reuses its store instead of restoring; stateless containers with ephemeral disks turn every deploy into a full restore. **Shrink the state**: shorter retention on windowed stores, smaller values, aggregating less. **More partitions** spread state across more tasks and restore in parallel, but only helps if decided early, since changing partition count breaks the key-to-task mapping. **Tune the restore consumer** for throughput. And if state is genuinely huge and recovery time is a hard requirement, that is the point where Flink's incremental checkpoints to object storage beat changelog replay. The senior observation: Streams makes state *look* free because there is no database to run, but the changelog replay is the bill, and it arrives during an incident.

</details>

**M9.9** — Two teams want to join an orders stream against a customers table in Kafka Streams. The join returns nulls for some customers that definitely exist. What do you check?

<details>
<summary>Answer</summary>

Co-partitioning first. A KStream–KTable join joins partition *n* of the stream with partition *n* of the table inside one task, so both topics must have the **same partition count** and records with the same key must land on the **same partition number** in both. Partition count mismatch is caught at startup; what is not caught is a different **partitioner or key serialization** — say, the orders producer keys by customer id as a string with the Java default murmur2, and the customers topic is written by a non-Java client or Connect pipeline that serializes the key differently or uses a different hash. The bytes differ, the partitions differ, and the join silently misses for every key that hashed apart. Check the keys' raw bytes on both topics, not their printed form. Second: **timing**. A stream–table join is a lookup at the moment the stream record is processed; if the customer record arrives on the table after the order, the order joins against nothing. Streams picks records by timestamp across inputs, but a table bootstrapped late, or a customer created after the order, still yields a null — `max.task.idle.ms` makes it wait for both inputs, and a stream–stream join with a window is the right model if both sides are genuinely events. Third: the key was changed upstream (a `selectKey` before the join) and the repartition is fine but the table side is keyed differently. Fixes: re-key one side so both use identical serialization, or use a **GlobalKTable** for customers if it is small enough to replicate to every instance — which removes co-partitioning entirely and lets you join on a field other than the key.

</details>

**M9.10** — Kafka Streams or Flink for a new stream-processing workload — how do you decide?

<details>
<summary>Answer</summary>

Start from what the workload touches and how it must recover, not from feature lists. **Streams** fits when input and output are Kafka, the logic belongs inside a service the team already deploys, and the team does not want another platform: there is no cluster, scaling is running more instances, and `exactly_once_v2` gives end-to-end exactly-once because everything it touches is Kafka. Its cost is that state recovery is changelog replay — tied to state size — and that every topology is a Java/Kotlin service each team operates themselves. **Flink** fits when sources or sinks go beyond Kafka (files, databases, other queues), when state is very large and recovery time matters (incremental checkpoints to object storage, not replay from a topic), when you need rich event-time handling across many inputs, when analysts want SQL, or when a platform team will run many jobs centrally. Its cost is a cluster, a job lifecycle, checkpoint storage and people who understand it. My default: Streams for service-local enrichment, aggregation and routing over Kafka with modest state; Flink when the state, the source/sink set or the organisation's shape outgrows that. Worth naming the third answer too: if the "processing" is per-record mapping with no state, neither — a plain consumer, or an SMT in Connect, is simpler than both.

</details>


## M10 · Sizing, monitoring & production incidents

### Junior

**M10.1** — How do you decide how many partitions a topic needs?

<details>
<summary>Answer</summary>

From throughput. Measure what one partition sustains for the producer and for one consumer doing the real processing, then take `max(target / producer_rate, target / consumer_rate)` and pad for growth. The consumer side usually dominates, because consumers do the work. Partition count is also the **ceiling on consumer parallelism** — one partition goes to at most one member of a group — so size for peak parallelism, not today's load.

</details>

**M10.2** — Why can't you just give every topic 1,000 partitions to be safe?

<details>
<summary>Answer</summary>

Every partition costs something. It is metadata the controller manages and a leader to elect, so failover after a broker death takes longer with more partitions per broker. Each replica holds segments with `.log`, `.index` and `.timeindex` files, so open-file and mmap limits are hit early. The same traffic over more partitions means smaller batches, more requests and worse compression. And every consumer-group rebalance has more partitions to move. Over-provision moderately; do not over-provision by an order of magnitude.

</details>

**M10.3** — Name the broker metrics you would alert on first, and what each means.

<details>
<summary>Answer</summary>

`UnderReplicatedPartitions` > 0 — a follower has dropped out of the ISR; the best general "something is wrong" signal. `UnderMinIsrPartitionCount` > 0 — `acks=all` writes are being rejected right now, which is an outage. `OfflinePartitionsCount` > 0 — partitions with no leader at all. `ActiveControllerCount` summed across the cluster should be exactly 1; 0 means no metadata changes or leader elections can happen. Plus request handler and network processor idle percentages for saturation, and consumer lag measured in time.

</details>

### Mid

**M10.4** — What happens when you increase the partition count of an existing keyed topic?

<details>
<summary>Answer</summary>

Existing data stays where it is, but `hash(key) % N` changes, so from that moment many keys map to a different partition than their history. Per-key ordering breaks across the change: old records for a key sit on one partition, new ones on another, consumed by different group members at different speeds. On a **compacted** topic it is worse, because the new value for a key lands in a different partition and never supersedes the old one, so both survive compaction. You can never decrease partitions. That is why partition count is worth over-provisioning on day one, and why a keyed topic that genuinely needs more partitions is often migrated to a new topic instead.

</details>

**M10.5** — Why is consumer lag in records a poor alerting metric, and what do you use instead?

<details>
<summary>Answer</summary>

Records lag has no unit of urgency: 50,000 records is half a second on a topic doing 100k/s and over an hour on one doing 10/s, so one threshold cannot fit every topic. Alert on **lag in time** — the age of the oldest unconsumed record, or records lag divided by the consume rate — and on the **trend**. Flat lag is a consumer keeping up with a fixed backlog; growing lag is one that will never catch up without help. And note the special case: flat, large lag on a `read_committed` group with no errors is a hanging transaction pinning the consumer at the LSO.

</details>

**M10.6** — How do you do a safe rolling restart of a Kafka cluster?

<details>
<summary>Answer</summary>

One broker at a time, with controlled shutdown on (the default), so the stopping broker tells the controller and leadership moves off it before it goes away — clients see a leader change rather than an unavailable partition. The rule that makes it safe: **do not stop the next broker until `UnderReplicatedPartitions` is back to zero**. Restarting the next broker while the previous one's replicas are still catching up can take a partition with `RF=3, min.insync.replicas=2` below min ISR, and `acks=all` writes fail. Afterwards, run a preferred leader election (or let `auto.leader.rebalance.enable` do it) so the restarted brokers lead their share again.

</details>

### Senior

**M10.7** — One consumer in a group is permanently behind while the others sit at zero lag. Diagnose it and propose fixes, including what each fix costs.

<details>
<summary>Answer</summary>

One partition lagging while its siblings are at zero is a **key-distribution** problem, not a capacity problem — the distinction matters because the obvious fix, adding consumers, does nothing here: the hot partition can only be consumed by one member. Confirm it: per-partition growth rate (log end offset or size sampled over time) shows one partition outrunning the others; per-partition lag shows the same partition behind; the leader broker for it shows disproportionate bytes-in. Then find the key — sample the partition and count keys; it is usually one tenant, one default or empty key, or one celebrity entity. Also rule out the boring alternative: a poison record or a slow downstream call specific to that partition's data, which shows as processing time per record rather than volume.

Fixes, cheapest first. **Re-key** if the key was over-specified — often you needed per-order ordering and keyed by customer. **Salt** the hot key: `key + "#" + (n % S)` spreads it over S partitions, at the price that per-key ordering now only holds within each salt, so any consumer that needs the whole key's order has to re-merge by timestamp or sequence number. **Route the hot key** to its own topic with its own consumers, which keeps ordering but adds a topic and routing logic you now own. **Speed up processing** for that consumer — batching downstream writes, for instance — which is fine if the skew is moderate but does not scale. What I would not do: add partitions, because that remaps every key, breaks ordering for all of them, and the hot key still hashes to exactly one partition.

</details>

**M10.8** — Produce p99 latency has tripled with no errors anywhere. Walk through how you find the cause.

<details>
<summary>Answer</summary>

Start from the request latency **breakdown** for Produce, because "slow" has four different causes and the phases separate them. `RequestQueueTimeMs` high means requests are waiting for a handler thread — check `RequestHandlerAvgIdlePercent`; the broker is saturated on CPU or I/O, often from a reassignment, a traffic increase, or a lagging consumer forcing cold reads that compete with writes. `LocalTimeMs` high means the leader's own append is slow — disk latency, a failing disk, page-cache pressure. `RemoteTimeMs` high means the leader is waiting for followers under `acks=all`: some follower is slow, and the slowest member of the ISR sets the write latency — find which broker, then look at its disk, GC and network, or at whether a replication throttle was left on after a reassignment. `ResponseQueueTimeMs`/`ResponseSendTimeMs` high points at network threads (`NetworkProcessorAvgIdlePercent` — TLS cost and many tiny requests both show here) or slow clients.

Then correlate in time: did it start with a deploy, a reassignment, a broker restart, a new consumer group (more fan-out), or a producer config change that shrank batches (`linger.ms`, many more partitions, compression off)? Check whether it is all brokers or one — one broker is a hardware or placement problem; all brokers is load. Also check the client side: if broker-side latency is flat but client p99 tripled, the time is in the producer — `buffer.memory` exhaustion blocking `send()` (M5), or the network between client and broker. The thing I want to avoid is tuning producer configs before knowing which phase the time is in.

</details>

**M10.9** — Two brokers have filled their disks and gone offline. Talk through the incident and the follow-up.

<details>
<summary>Answer</summary>

First, stabilise without making it worse. A log directory with a write error goes offline, and on a single-log-dir broker that takes the broker down, so its partitions are now under-replicated — and with two brokers gone, any partition with replicas on both is likely below `min.insync.replicas` (`acks=all` writes failing) or offline. The rest of the cluster probably filled for the same reason and is close behind, so the first action is cluster-wide: **lower retention on the largest topics** with `kafka-configs.sh` (`retention.ms` / `retention.bytes`) so the surviving brokers delete whole segments themselves and buy headroom. For the dead brokers: add or free disk (expand the volume, or remove non-Kafka files), then start them; they recover their logs and rejoin the ISR. If that is not possible quickly, reassign their partitions to brokers with space — throttled, because an unthrottled reassignment during an incident adds a second one.

What not to do: `rm` segment files from under the broker. It leaves indexes, log start offsets and checkpoint files disagreeing with the disk and can turn a capacity incident into data loss or a broker that will not start. Also do not lower `min.insync.replicas` to get producers going without someone explicitly accepting the durability being given up.

Follow-up: find why. Common causes are retention set when traffic was a tenth of today's, a new high-volume topic created with default retention, `retention.bytes` unset so only time bounds it, a low-volume topic whose segments never roll because `segment.ms` defaults to seven days, or tiered storage uploads stuck so local segments are not being released. Then add a disk-usage alert with enough lead time to act — alerting at 85% when retention deletes in segment-sized steps is too late — capacity math as ingress × retention × RF / brokers with real headroom, and per-topic retention review as part of creating topics.

</details>

**M10.10** — Design disaster recovery across two regions with MirrorMaker 2. What does failover actually cost?

<details>
<summary>Answer</summary>

MM2 runs on Kafka Connect as three connectors: the source connector copies topics, data and configs; the checkpoint connector emits translated consumer-group offsets; the heartbeat connector proves the link is alive. MirrorMaker 1 was removed in 4.0, so MM2 is the in-tree option.

**Active-passive** is the default recommendation: producers write to the primary, MM2 replicates to the DR cluster, topics arrive prefixed with the source alias (`primary.orders`). On failover, producers are repointed to DR and consumers resume from translated offsets. The costs, stated plainly: **RPO is the replication lag** — whatever MM2 had not copied is lost or stranded in the dead region; **offsets are not shared** — the target has its own offsets, translation goes through periodic sync points and resolves to the nearest earlier one, so consumers resume somewhat *before* where they stopped and reprocess, making failover at-least-once by construction and requiring idempotent consumers; **topic names change** under the default replication policy, so consumers must know to read the prefixed name (or you use `IdentityReplicationPolicy`, which is only safe one-way); and **failback** is a project of its own — the primary has diverged, and you either replicate back and fail over again or rebuild it.

**Active-active** — both regions take writes, each mirrors the other, consumers read local plus remote topics — removes the failover step, but per-key ordering across regions is gone, conflicting writes to the same entity become the application's problem, and every consumer has to merge two topics. I would choose it only when the data is naturally partitioned by region.

What makes it work in practice is rehearsal: a documented, tested failover with known RPO and RTO numbers, monitoring of MM2 replication lag and heartbeats, and the decision of *who* declares a failover made in advance. The summary I would give: multi-cluster turns a Kafka problem into an application-design problem, and failover is never free — you pay in lost tail data and duplicates.

</details>


## M11 · Security & multi-tenancy

### Junior

**M11.1** — Name Kafka's four security protocols and say which one production normally uses.

<details>
<summary>Answer</summary>

`PLAINTEXT` — no encryption, no authentication, principal `ANONYMOUS`. `SSL` — TLS encryption, optionally authenticating the client by certificate (mTLS). `SASL_PLAINTEXT` — SASL authentication with no encryption, so credentials or tokens cross the wire in the clear. `SASL_SSL` — SASL authentication inside TLS. Production normally uses `SASL_SSL`, or `SSL` with mTLS where there is a PKI. Each protocol is bound to a named **listener**, and a broker can run several listeners at once.

</details>

**M11.2** — What is the difference between authentication and authorization in Kafka?

<details>
<summary>Answer</summary>

**Authentication** establishes who the client is and produces a **principal** such as `User:alice` — from the certificate DN with mTLS, or the username or token subject with SASL. **Authorization** decides what that principal may do, by checking ACLs through the configured authorizer (`StandardAuthorizer` in KRaft). Authentication without authorization means everyone known is allowed everything; authorization without authentication means every client is `ANONYMOUS`, so the ACLs have nothing meaningful to key on.

</details>

**M11.3** — What ACLs does a consumer in a consumer group need?

<details>
<summary>Answer</summary>

`Read` on the topic **and** `Read` on the consumer group. `Describe` on the topic is implied by `Read`. The group grant is the one people forget: without it the consumer fails with `GroupAuthorizationException` — it cannot join the group or commit offsets — while the topic ACL looks perfectly correct.

</details>

### Mid

**M11.4** — Compare PLAIN, SCRAM, GSSAPI and OAUTHBEARER. When would you pick each?

<details>
<summary>Answer</summary>

**PLAIN** sends the password as is, so it is only acceptable inside TLS, and the default implementation reads passwords from the broker's JAAS file — adding a user means a restart; production use needs a custom callback handler against an external store. **SCRAM-SHA-256/512** is salted challenge-response: the password never crosses the wire, credentials live in cluster metadata and are managed with `kafka-configs` without restarts — the sensible default when there is no identity provider. **GSSAPI (Kerberos)** fits an organisation already running AD/Kerberos and avoids static secrets in clients, at the cost of making the KDC a dependency of every connection, plus ticket-renewal and clock-skew failures. **OAUTHBEARER** uses short-lived tokens from an identity provider — the best fit for cloud IAM and central identity — but the built-in unsecured JWT handler is development-only; production needs OIDC validation configured. The choice is mostly about where identity already lives in the organisation, not about Kafka.

</details>

**M11.5** — How are ACLs evaluated? Cover deny precedence, prefixed patterns, allow.everyone.if.no.acl.found and super.users.

<details>
<summary>Answer</summary>

An ACL grants or denies a principal an operation on a resource (`Topic`, `Group`, `Cluster`, `TransactionalId`, `DelegationToken`, `User`) from a host. Patterns are `LITERAL` — including the `*` wildcard — or `PREFIXED`, which matches every resource name starting with a string and is what makes per-team or per-tenant grants manageable. **Any matching deny wins** over any number of allows, regardless of specificity. If no ACL matches, the request is **denied** — unless `allow.everyone.if.no.acl.found=true`, which makes any resource with no ACLs open to all; it exists for migration and is dangerous to leave on because every new topic starts public. Principals in `super.users` skip ACL checks entirely; broker and controller principals must be there (or hold explicit `ClusterAction`), or enabling the authorizer denies the cluster to itself. `Read`, `Write`, `Delete` and `Alter` imply `Describe`.

</details>

**M11.6** — How do quotas work, and why should they be set on users rather than client ids?

<details>
<summary>Answer</summary>

Quotas cap `producer_byte_rate` and `consumer_byte_rate` per broker, `request_percentage` — share of network and I/O thread time, which catches clients sending many small requests that byte rates never see — and `controller_mutation_rate` for partition creation and deletion. They are enforced by **throttling, not rejection**: the broker computes how long the client must wait to fall back under its quota, delays the response and returns `throttle_time_ms`, so the tenant experiences it as latency. Quotas can target `user`, `client-id`, or the pair, most specific winning. `client.id` is a string the client sets for itself, so a client-id-only quota can be dodged by changing one config value — it is a convention, not a control. Enforceable isolation needs the authenticated principal, so quotas go on `user`, with client-id used to subdivide within a user.

</details>

### Senior

**M11.7** — After enabling TLS on a fetch-heavy cluster, broker CPU roughly doubles and consumer latency rises. Nothing is misconfigured. Explain why, and what you would do.

<details>
<summary>Answer</summary>

It is the expected cost, and the cause is M2: a plaintext fetch is served with `sendfile`, moving bytes from page cache to the socket without ever entering the JVM. **TLS breaks zero-copy** — the broker must read the bytes into user space to encrypt them, so every fetch now pays a copy into a buffer, encryption CPU, and more memory and GC pressure. Produce-side impact is smaller; fetch-side impact scales with fan-out, because each consumer group re-reads and re-encrypts the same bytes, so a topic read by ten groups pays ten times. The cipher itself is cheap on a modern JDK with AES-NI and TLS 1.3; what does not go away is the lost zero-copy. What to do: first, accept that this is capacity planning that should have happened before the rollout, and size brokers for it — more cores, or more brokers. Make sure batches are compressed end to end, since compressed batches mean fewer bytes to encrypt and the broker passes them through without recompressing. Check that the JDK and TLS settings are not forcing an old protocol or slow cipher suite. Reduce redundant fan-out where several groups read the same data for overlapping purposes. What not to do: turn TLS off for internal clients to save CPU without a deliberate decision that the network is trusted — that trades a known cost for an unknown risk. A senior answer names the mechanism, because without it the CPU graph looks like a regression to chase rather than a price that was always going to be paid.

</details>

**M11.8** — Clients in the cluster's own network work. Clients outside it connect to the bootstrap server, then time out on every produce. Diagnose it.

<details>
<summary>Answer</summary>

Classic **`advertised.listeners`** failure. The bootstrap connection is only used to fetch metadata; the metadata tells the client the address of each partition leader, and that address is whatever each broker advertises for the listener the client came in on. If it advertises an internal name — a container hostname, a pod IP, a cluster-internal DNS name — internal clients resolve and reach it while external clients cannot, so bootstrap succeeds and every subsequent connection times out. That pattern, *bootstrap works, everything after fails*, is the tell. Confirm by looking at the metadata the client receives (client debug logs, or `kafka-broker-api-versions` / metadata from outside) and checking whether those hosts are resolvable and routable from the client's network. The fix is a separate listener for external traffic — `EXTERNAL://` on its own port — with an advertised address external clients can reach (a load-balancer or per-broker public DNS name; each broker needs its own, because clients must reach specific leaders, not any broker), mapped to its protocol in `listener.security.protocol.map`, while internal clients and inter-broker traffic keep the internal listener. Two related traps: with TLS, hostname verification checks the certificate against the *advertised* name, so a certificate that matches the bootstrap name can still fail after bootstrap; and in Kubernetes a single Service in front of all brokers works for bootstrap and nothing else. The reason this is senior-level is that it looks like a network, firewall or TLS problem, and people spend hours there before reading one line of broker config.

</details>

**M11.9** — Design a shared Kafka platform for twenty internal teams. How do you isolate them, and when would you give a team its own cluster?

<details>
<summary>Answer</summary>

Kafka has no tenant concept, so isolation is assembled. **Identity:** every team's services authenticate as their own principals — SCRAM or, better, OAUTHBEARER against the company IdP so onboarding and revocation follow existing identity — never a shared account. **Naming:** an enforced prefix per team for topics, consumer groups and transactional ids (`payments.`, `payments-`), created through a self-service pipeline rather than auto-creation, which is off. **Authorization:** `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`, prefixed ACLs granting each team full rights on its own prefix, and cross-team reads granted explicitly per topic so data sharing is visible and auditable. **Quotas:** byte-rate and `request_percentage` quotas on the authenticated *user*, not client id, plus `controller_mutation_rate` so nobody scripts ten thousand partitions; throttle-time metrics exposed to each team, because throttling presents as latency and teams otherwise file it as a platform bug. **Capacity guardrails:** partition-count and retention limits per team enforced at creation time. **The limits of sharing**, which are the senior part: quotas govern network and request time but not disk or page cache. One team's lagging consumer reading cold data from disk evicts everyone's hot segments from page cache, and long retention fills shared disks; a rebalance storm or a giant message still lands on shared brokers. So I would give a team its own cluster when its traffic dwarfs the others enough that its incidents become everyone's, when it needs a materially different SLO or durability profile, when compliance requires physical isolation of the data or keys, or when its usage pattern — heavy historical replays, very large messages — is hostile to page-cache sharing. The platform then means one way of provisioning many clusters rather than one cluster for everything. The judgement worth stating: shared clusters are efficient for cooperative, observable tenants; separate clusters are cheaper than the incident review about who broke the shared one.

</details>

**M11.10** — You need to turn on authentication and ACLs on a live, previously open cluster without an outage. How do you sequence it?

<details>
<summary>Answer</summary>

The rule is that each step must be reversible and must not break any existing client. **1. Add, don't replace, listeners:** add a `SASL_SSL` listener alongside the existing `PLAINTEXT` one, with advertised addresses reachable by clients, and roll the brokers. Nothing changes for existing clients. Move inter-broker and controller traffic to secured listeners in their own rolls, and make the broker principals `super.users` (or grant `ClusterAction`) before any authorizer exists. **2. Migrate clients to authenticate:** each service gets its own principal and moves to the new listener. Watch per-listener connection metrics to see who is still on plaintext. **3. Enable the authorizer permissively:** set the `StandardAuthorizer` with `allow.everyone.if.no.acl.found=true`, so resources with no ACLs stay open. Now write ACLs — prefixed per team where naming allows — resource by resource; once a resource has *any* ACL, it is closed to everyone not granted, so do it per resource with the owning team. Use authorizer logging of denials to catch missing grants, especially the forgotten ones: group `Read` for consumers, `TransactionalId` `Write` for transactional producers. **4. Close the defaults:** once denial logs are quiet, set `allow.everyone.if.no.acl.found=false`, then remove the plaintext listener after its connection count reaches zero. **5. Make revocation real:** set `connections.max.reauth.ms`, since without it a connection authenticated before a credential change stays authenticated indefinitely. Rotation is then a process — CA overlap for certificates via dynamic keystore and truststore updates, new-principal-then-cutover for SCRAM — rather than an emergency. The common failure modes are exactly the shortcuts: enabling the authorizer before the brokers are super users (the cluster denies itself), flipping `allow.everyone.if.no.acl.found` to false globally on day one, or removing plaintext while a forgotten batch job still uses it.

</details>


## M12 · System design with Kafka

### Junior

**M12.1** — How do you choose the message key for a topic?

<details>
<summary>Answer</summary>

From the **ordering requirement**: whatever entity must have its events processed in order is the key, because Kafka only orders within a partition and the key decides the partition. Order lifecycle events are keyed by `orderId`; account ledger entries by `accountId`. Everything not sharing a key is free to reorder, which is what buys parallelism. Then check the key for skew — a key with a disproportionate share of traffic becomes a hot partition — and if nothing needs ordering at all, a null key with the sticky partitioner spreads load evenly.

</details>

**M12.2** — What is the difference between event notification and event-carried state transfer?

<details>
<summary>Answer</summary>

**Event notification** says something changed and carries little more than an ID; consumers call back to the source for details, so the source must be up and must absorb every consumer's lookups — runtime coupling. **Event-carried state transfer** carries the full new state, so consumers keep their own copy and never call back; coupling is reduced to the schema. The second fits Kafka well, especially on a compacted topic keyed by entity, where the topic becomes a changelog any new consumer can bootstrap from.

</details>

**M12.3** — What is a dead-letter topic and what should go into it?

<details>
<summary>Answer</summary>

A topic where messages that cannot be processed are parked so they stop blocking or being retried forever. Each entry should carry the original payload plus headers recording the error, the source topic, partition and offset, and a timestamp, so it can be diagnosed and replayed. Permanent failures — deserialization errors, validation failures — should go there directly rather than after a round of pointless retries. A DLQ with no alert on it and no replay procedure is just a slower way to lose data.

</details>

### Mid

**M12.4** — Why does non-blocking retry with retry topics break ordering, and what are the alternatives?

<details>
<summary>Answer</summary>

A failed message is published to a retry topic and the original offset is committed so the main partition keeps flowing. The next message for the **same key** is then processed from the main topic before the failed one comes back from the retry topic — per-key order is gone. Alternatives: **blocking retry** in place with backoff, pausing the partition rather than sleeping in the poll loop so `max.poll.interval.ms` is not exceeded — order kept, partition stalled; or keep non-blocking retry but **route later messages for a key already in retry** to the retry path too, which needs a store of keys currently in retry. Which to pick depends on whether per-key order actually matters for that consumer, and separating transient from permanent failures keeps either approach from wasting time on poison messages.

</details>

**M12.5** — How do you make a consumer idempotent?

<details>
<summary>Answer</summary>

In order of preference: make the write **naturally idempotent** — an upsert on a business key, setting a value rather than incrementing it; add a **version or sequence guard** so an event is applied only if it is newer than the stored state, which also defends against reordering; record a **processed event ID** (or `topic-partition-offset`) in the same database transaction as the effect, with a unique constraint; and for an external API, pass an **idempotency key derived from the event**, never one generated at send time, because a redelivery would generate a new one. The point is that the dedupe check and the effect must commit atomically — a check in one transaction and the write in another just moves the race.

</details>

**M12.6** — Choreography or orchestration for a saga — how do you choose?

<details>
<summary>Answer</summary>

**Choreography**: each service reacts to the previous one's events. Good for two or three steps owned by different teams with no central coordinator; it degrades as steps are added, because no single place shows where a given instance is stuck and implicit cycles appear. **Orchestration**: a coordinator issues commands and tracks state per saga instance. Better for many steps, branching, and especially **timeouts** — "cancel the order if payment has not arrived in 15 minutes" needs a durable timer, which has nowhere to live in pure choreography. The risk of orchestration is the orchestrator accumulating business logic; keep it to sequencing and compensation. Either way, every step needs a compensating action, compensations must be retried until they succeed, and they are semantic undos rather than rollbacks — the intermediate state was visible.

</details>

### Senior

**M12.7** — A team proposes using Kafka as the event store for an event-sourced system. Evaluate it.

<details>
<summary>Answer</summary>

Event sourcing needs two things from its store, and Kafka provides neither well. **Per-aggregate reads:** to handle a command you load one aggregate's history. Kafka reads a partition sequentially from an offset; there is no index by key, so rebuilding order 42 means scanning a partition shared with every other order hashed there, or keeping a materialised view — at which point the view is the real store and Kafka is a feed. Topic-per-aggregate does not scale: partitions are a cluster-wide resource with real metadata and file-handle cost. **Optimistic concurrency:** a command must append only if the aggregate is still at the version it read. Kafka has no conditional append — two writers can both append against version 7 and the conflict is discovered after the fact, if at all. Smaller issues: retention and compaction semantics must be configured never to drop history (compaction would keep only the latest event per key, which destroys the point), and schema evolution must be `FULL_TRANSITIVE` because every old event is replayed forever. **What I would propose instead:** a store built for this — an event store, or a relational table of events with `(aggregate_id, version)` as a unique key, which gives both indexed reads and optimistic concurrency for free — as the system of record, publishing to Kafka via outbox or CDC. Kafka then does what it is good at: distributing events to projections, read models, analytics and other services, with replay for rebuilding them. Kafka Streams with a state store can make a narrow version work when commands for a key are always routed through the same partition and processed single-threaded — that is genuinely viable, but it is a different architecture with its own operational weight, and it should be chosen on purpose rather than arrived at because Kafka was already there.

</details>

**M12.8** — Design a payment ledger that uses Kafka. What does Kafka do and what does it not do?

<details>
<summary>Answer</summary>

**The ledger itself does not live in Kafka.** The system of record is a database enforcing double-entry invariants — debits equal credits, balances never negative where that matters — inside one transaction, which Kafka cannot do. Kafka **distributes** committed ledger entries: the ledger service writes entries and an outbox row in one database transaction, and CDC publishes them. **Key** by `accountId`, so entries for an account are ordered and downstream balance projections can hold per-account state locally. **Durability** settings are non-negotiable: RF=3, `min.insync.replicas=2`, `acks=all`, idempotent producer, unclean leader election disabled; retention long or infinite with tiered storage if needed, because the topic is audit evidence and the source for rebuilding projections. **Semantics:** Kafka-to-Kafka projections — balances, fraud features, reporting aggregates — can use transactions for exactly-once; anything external, such as calling a payment provider, carries an idempotency key derived from the entry ID, because the external system is outside the transaction boundary. **Schemas** registered with `FULL_TRANSITIVE` compatibility, since entries are replayed indefinitely. **Security:** prefixed ACLs so only the ledger service principal can produce to ledger topics, consumers get read-only access, and the topics are encrypted in transit. **Failure modes to name:** a poison entry must block rather than skip — skipping a ledger entry corrupts every balance after it, so DLQ here means stop and page, not park and continue; projection drift is detected by periodically reconciling projected balances against the database. The senior point is the first sentence: put the invariant where it can be enforced atomically, and use Kafka for what follows from it.

</details>

**M12.9** — A service needs a synchronous answer from another service, and the team wants to do request-reply over Kafka. What do you say?

<details>
<summary>Answer</summary>

That it works and is usually the wrong tool. Mechanically: a request topic, a reply topic, a correlation ID in a header, and a map of pending futures. The problems: **latency** now includes producer batching, consumer poll intervals and any rebalance on either side — a rebalance turns a 50 ms call into seconds; **routing** — the reply must reach the one instance holding the future, which means a reply partition or topic per instance, or every instance reading every reply and discarding most; **timeouts** leave orphaned work, since the responder will still process a request whose caller gave up, so requests need deadlines the responder checks; and **coupling** is as tight as RPC while debugging is harder. Questions I would ask: does the caller really need the answer before responding to its own client? Often not — return `202 Accepted` with an ID and deliver the outcome as an event, a webhook, or a status endpoint, which is where Kafka earns its place. If the answer is needed synchronously, use RPC (HTTP or gRPC) with timeouts, retries and a circuit breaker, and keep Kafka for the asynchronous consequences. The narrow case where request-reply over Kafka is defensible is when the request itself must be durable and replayable, or when the responder is a batch-oriented worker pool — and even then I would want it as an explicit, documented exception.

</details>

**M12.10** — Design multi-region for a Kafka-based order platform. What do you have to decide?

<details>
<summary>Answer</summary>

First the **RPO and RTO** the business actually needs, because they choose the topology. **Stretch cluster** across three nearby locations: synchronous replication with `acks=all` and replicas placed across locations via rack awareness, so RPO is zero, but every write pays the inter-location round trip and the controller quorum needs a majority to survive a location loss, which is why it wants three sites, not two. Viable only when latency between sites is low. **Active-passive** with MirrorMaker 2: replication is asynchronous, so RPO equals mirror lag and must be monitored as an SLO; offsets differ between clusters, so consumer failover depends on MM2's checkpointed offset translation, and consumers must be idempotent because translation is approximate and some reprocessing is expected. **Active-active**: each region produces locally and mirrors the other's topics under a prefix; consumers read both. Natural for append-only, commutative data; dangerous for orders, because two regions can modify the same order concurrently. For orders I would **pin each order to a home region** — route by customer or order ID — so each key has one writer, and the other region holds a replica for reads and failover. Then the things people forget: schema registry and ACLs must exist in both regions; producers and consumers need a tested way to switch clusters (DNS or config, not a redeploy); failover must be **rehearsed**, including failback, which is usually harder; and the sagas' timers and the outbox publishers must not run in both regions at once. The senior answer names the trade-off — synchronous costs latency, asynchronous costs data on failover — and chooses based on the stated RPO rather than defaulting to active-active because it sounds most resilient.

</details>
