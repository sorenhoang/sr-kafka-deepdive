"""Lab 2.1 — Write enough data to roll several log segments.

The topic is created with segment.bytes=1024, so the broker closes the current
segment and opens a new one roughly every kilobyte. Watch for: several .log files
in the partition directory, each named after the offset it starts at.

    python3 01_fill_segments.py [count] [payload_bytes]
"""
import sys
from confluent_kafka import Producer
from common import BOOTSTRAP

TOPIC = "lab02.segments"
count = int(sys.argv[1]) if len(sys.argv) > 1 else 200
size = int(sys.argv[2]) if len(sys.argv) > 2 else 100

p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 5})
payload = b"x" * size

print(f"Producing {count} records of ~{size}B to {TOPIC}")
for i in range(count):
    p.produce(TOPIC, key=f"k{i}".encode(), value=payload)
    if i % 50 == 0:
        p.poll(0)
p.flush(15)
print(f"Done. Roughly {count * size / 1024:.0f} KiB of payload, so expect several segments.")
