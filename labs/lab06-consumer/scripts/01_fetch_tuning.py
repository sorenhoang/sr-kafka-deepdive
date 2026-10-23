"""Lab 6.1 — fetch.min.bytes trades latency for batching.

    python3 01_fetch_tuning.py <fetch_min_bytes> <fetch_max_wait_ms>

Watch for: with fetch.min.bytes=1 the first record arrives almost immediately. Raise
it and the broker holds the response until it has that many bytes or fetch.max.wait.ms
expires — so on a quiet topic you wait the full timeout for the first record.
"""
import sys
import time
import uuid
from confluent_kafka import Consumer, Producer
from common import BOOTSTRAP, TOPIC

min_bytes = int(sys.argv[1]) if len(sys.argv) > 1 else 1
max_wait = int(sys.argv[2]) if len(sys.argv) > 2 else 500

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": f"fetch-{uuid.uuid4()}",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
    "fetch.min.bytes": min_bytes,
    "fetch.wait.max.ms": max_wait,     # librdkafka's name for fetch.max.wait.ms
})
c.subscribe([TOPIC])
# force assignment before timing
for _ in range(10):
    if c.poll(0.5) is None and c.assignment():
        break

p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0, "acks": 1,
              "enable.idempotence": False})
p.produce(TOPIC, key=b"probe", value=b"one small record")
p.flush(5)
sent = time.monotonic()

while time.monotonic() - sent < 10:
    msg = c.poll(0.1)
    if msg and not msg.error():
        print(f"fetch.min.bytes={min_bytes:<7} fetch.max.wait.ms={max_wait:<5} "
              f"first record after {(time.monotonic() - sent) * 1000:7.0f} ms")
        break
else:
    print(f"fetch.min.bytes={min_bytes}: nothing within 10s")
c.close()
