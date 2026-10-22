"""Lab 5.1 — How linger.ms changes the batches that reach disk.

    python3 01_batching.py <topic> <linger_ms> [count] [size]

Watch for: the same records arriving as fewer, larger batches as linger rises. Count
the batches afterwards with kafka-dump-log.sh — the `count:` field on each batch
header is records-per-batch.
"""
import sys
import time
from confluent_kafka import Producer
from common import BOOTSTRAP

topic = sys.argv[1]
linger = int(sys.argv[2])
count = int(sys.argv[3]) if len(sys.argv) > 3 else 2000
size = int(sys.argv[4]) if len(sys.argv) > 4 else 200

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "linger.ms": linger,
    "batch.size": 16384,
    "compression.type": "none",
    "enable.idempotence": False,
    "acks": 1,
})
payload = b"x" * size

started = time.monotonic()
for i in range(count):
    p.produce(topic, value=payload)
    if i % 500 == 0:
        p.poll(0)
p.flush(30)
elapsed = time.monotonic() - started

print(f"linger.ms={linger:<4} {count} records x {size}B in {elapsed:.2f}s "
      f"({count/elapsed:,.0f} rec/s)")
