# Kafka Deep Dive

Study log for working through Apache Kafka from fundamentals to internals, aimed at
senior backend / distributed systems interviews. Targets **Kafka 4.x (KRaft-only)**.

**Notes site:** <https://sorenhoang.github.io/sr-kafka-deepdive/> — built from [`docs/`](docs/).
**Quiz:** [`QUIZ.md`](QUIZ.md) — every question from every module, answers collapsed.

Each module follows the same four steps:

1. **Theory** — mechanism plus the design reasoning behind it, written up on the notes site.
2. **Hands-on lab** — a runnable Docker cluster and scripts, with a worksheet that
   walks the steps one at a time. Every step asks for a prediction before the command
   is run, because the prediction is where the learning is; reading the output
   afterwards only confirms what was already seen.
3. **Tiered questions** — Junior → Mid → Senior. Senior answers get written out and
   graded against *missing the point / correct but shallow / senior level*.
4. **Answers** — the senior-tier questions get answered in writing and graded. Only
   the polished model answer is committed, under `src/answers/`; the raw attempt, the
   grade and the gap list stay local under `private/` (gitignored).

## Progress

`Material` means the notes and lab exist. The other two columns are the ones that
count: a module is finished when its lab has actually been run and its senior-tier
questions have been answered and graded.

M0 is complete: lab 00 run, junior and mid tiers self-assessed, Q7-Q9 answered and
graded, Q10 answered from the model rather than attempted cold — worth retrying in M13.

M1 is complete: lab 01 run, junior and mid tiers self-assessed, all three senior
questions answered and graded.

| Module | Topic | Material | Lab run | Questions graded |
|---|---|:--:|:--:|:--:|
| M0 | Positioning: log vs queue, when not to use Kafka, Share Groups | ready | ✅ | ✅ |
| M1 | Topic, partition, offset, replica, consumer group, ordering | ready | ✅ | ✅ |
| M2 | Storage internals: segments, page cache, zero-copy, compaction | ready | | |
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
  labs/*.html        step-by-step lab worksheets, one standalone page each
  quiz/*.json        the questions and their short answers — single source of truth
  answers/*.html     long-form model answers to the senior-tier questions
  foot.html          partition calculator + checklist behaviour
tools/build.py       assembles src/ (and private/src/) into the pages below
docs/index.html      generated — standalone page for GitHub Pages
docs/lab-*.html      generated — one worksheet page per src/labs/ file
dist/artifact.html   generated — page body without the HTML skeleton
QUIZ.md              generated — all questions, answers behind <details> toggles
labs/lab00-log-vs-queue/
                     Kafka and RabbitMQ side by side: consuming does not delete
labs/lab01/          three-broker KRaft cluster + Python scripts
labs/lab02-storage/  single broker; segments, indexes, retention, compaction
private/             gitignored — raw answers, grades and gap notes, local only
```

`docs/index.html`, `dist/artifact.html`, `QUIZ.md` and everything directly under
`private/` are **generated**. Edit the sources under `src/` and `private/src/`, never the
generated files.

Each question exists once, in `src/quiz/NN-name.json`. The module page carries a
`<!--QUIZ:NN-name-->` placeholder where the checklist is injected, and the same JSON
produces that module's section of `QUIZ.md`, so a question and its answer cannot drift
apart between the two.

`private/` is deliberately not committed: the raw answers and the list of what each one
missed are only useful as a personal record, and this repository is public.

## Building the notes

```bash
python3 tools/build.py
```

Adding a module: drop `src/modules/NN-name.html` in place with a `<!--QUIZ:NN-name-->`
placeholder, write `src/quiz/NN-name.json` beside it, add an entry to `src/rail.html`,
and rebuild. Partials are plain HTML fragments — no templating engine,
no build dependencies beyond Python 3.

## Running a lab

```bash
cd labs/lab00-log-vs-queue        # or labs/lab01
docker compose up -d && docker compose ps     # wait for every node to be healthy
python3 -m venv .venv
source .venv/bin/activate                     # once per terminal
pip install -r requirements.txt
```

Each lab has its own README with the commands and what to look for. Clean up with
`docker compose down -v`.
