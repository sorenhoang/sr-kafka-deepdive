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
