"""Lab 2.3 — Read the same data twice and compare.

The first pass may have to fault pages in from disk; the second is served from the
OS page cache. Measure it rather than trusting a number from a blog post: on a laptop
with a small topic the difference is often tiny, because the data was still in cache
from being written. That result is itself informative — it is the page cache working.

    python3 03_read_timing.py [topic]
"""
import sys
import time
import uuid
from confluent_kafka import Consumer
from common import BOOTSTRAP

topic = sys.argv[1] if len(sys.argv) > 1 else "lab02.segments"


def drain(label):
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"timing-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1,
    })
    c.subscribe([topic])
    started, records, empty = time.monotonic(), 0, 0
    while empty < 5:
        msg = c.poll(1.0)
        if msg is None:
            empty += 1
            continue
        if msg.error():
            continue
        records += 1
        empty = 0
    elapsed = time.monotonic() - started
    c.close()
    # Subtract the idle polls spent proving the topic is drained.
    print(f"  {label:<8} {records} records in {max(elapsed - 5.0, 0):.3f}s of active reading")
    return records


print(f"Reading {topic} twice, each with a fresh consumer group\n")
drain("pass 1")
drain("pass 2")
print("\nIf the two are close, the data never left the page cache between passes —")
print("which is the mechanism, not a measurement error.")
