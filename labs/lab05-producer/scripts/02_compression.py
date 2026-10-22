"""Lab 5.2 — Same data, three codecs, compare bytes on disk.

    python3 02_compression.py <codec>     # none | lz4 | zstd | gzip | snappy

Watch for: the segment size afterwards. Compression is per batch, so a healthy
linger.ms matters as much as the codec choice.
"""
import sys
import time
from confluent_kafka import Producer
from common import BOOTSTRAP

codec = sys.argv[1] if len(sys.argv) > 1 else "none"
count = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
topic = f"lab05.comp-{codec}"

# Repetitive-but-not-trivial payload: realistic for JSON-ish records.
payload = (b'{"user_id":"u-00000","event":"page_view","path":"/catalog/item",'
           b'"ts":"2026-10-22T09:15:00Z","session":"abc123def456"}')

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "compression.type": codec,
    "linger.ms": 50,
    "batch.size": 65536,
    "enable.idempotence": False,
    "acks": 1,
})

started = time.monotonic()
for i in range(count):
    p.produce(topic, value=payload)
    if i % 500 == 0:
        p.poll(0)
p.flush(30)

print(f"{codec:<8} {count} records of {len(payload)}B "
      f"({count * len(payload) / 1024:,.0f} KiB raw) in {time.monotonic() - started:.2f}s")
print(f"  now measure:  docker exec lab05-kafka du -sh /tmp/kraft-combined-logs/{topic}-0/")
