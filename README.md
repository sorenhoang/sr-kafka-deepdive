# Kafka Deep Dive

Study log for working through Apache Kafka from fundamentals to internals, aimed at
senior backend / distributed systems interviews. Targets **Kafka 4.x (KRaft-only)**.

**Notes site:** <https://sorenhoang.github.io/sr-kafka-deepdive/> — built from [`docs/`](docs/).

Each module follows the same four steps:

1. **Theory** — mechanism plus the design reasoning behind it, written up on the notes site.
2. **Hands-on lab** — a runnable Docker cluster and scripts, each with an explicit
   *what to watch for* so the behaviour is observed rather than read about.
3. **Tiered questions** — Junior → Mid → Senior. Senior answers get written out and
   graded against *missing the point / correct but shallow / senior level*.
4. **Answers** — written up under [`my-answers/`](my-answers/).

## Progress

| Module | Topic | Notes | Lab | Answers |
|---|---|:--:|:--:|:--:|
| M0 | Positioning: commit log vs queue, when not to use Kafka, Share Groups | ✅ | ✅ | |
| M1 | Topic, partition, offset, replica, consumer group, ordering | ✅ | ✅ | |
| M2 | Storage internals: segments, page cache, zero-copy, compaction | | | |
| M3 | Replication, ISR, LEO vs high watermark, unclean leader election | | | |
| M4 | KRaft: quorum controller, metadata log | | | |
| M5 | Producer internals, idempotent producer | | | |
| M6 | Consumer internals, rebalance, KIP-848 | | | |
| M7 | Delivery semantics, transactions, exactly-once | | | |
| M8 | Schema Registry, compatibility, evolution | | | |
| M9 | Connect, CDC/Debezium, Kafka Streams | | | |
| M10 | Partition sizing, monitoring, production incidents | | | |
| M11 | TLS, SASL, ACLs, multi-tenancy | | | |
| M12 | System design with Kafka | | | |
| M13 | Mock interview, incident scenarios | | | |

## Layout

```
src/                 notes source, one partial per module
  head.html          shared CSS and web fonts
  rail.html          sidebar navigation and module status
  modules/*.html     one file per module, assembled in filename order
  foot.html          partition calculator + checklist behaviour
tools/build.py       assembles src/ into both outputs
docs/index.html      generated — standalone page for GitHub Pages
dist/artifact.html   generated — page body without the HTML skeleton
labs/lab01/          three-broker KRaft cluster + Python scripts
my-answers/          written answers to the senior-tier questions
```

`docs/index.html` and `dist/artifact.html` are **generated**. Edit the partials under
`src/`, never the generated files.

## Building the notes

```bash
python3 tools/build.py
```

Adding a module: drop `src/modules/NN-name.html` in place, add its entry to
`src/rail.html`, and rebuild. Partials are plain HTML fragments — no templating engine,
no build dependencies beyond Python 3.

## Running a lab

```bash
cd labs/lab01
docker compose up -d && docker compose ps     # wait for all three nodes to be healthy
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Each lab has its own README with the commands and what to look for. Clean up with
`docker compose down -v`.
