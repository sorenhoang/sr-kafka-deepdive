"""Lab 6.3 — Exceed max.poll.interval.ms and get evicted.

    python3 03_poll_interval.py <seconds_per_batch> [instance]

max.poll.interval.ms is set to 10s here. Sleep longer than that between polls and the
coordinator removes this member even though the heartbeat thread is perfectly healthy.

Watch for: REVOKED/ASSIGNED churn, and the error on the next poll. Run two copies to
see the whole group stop each time one is evicted — that is the storm.
"""
import sys
import time
from confluent_kafka import Consumer, KafkaException
from common import BOOTSTRAP, TOPIC

work = float(sys.argv[1]) if len(sys.argv) > 1 else 12.0
name = sys.argv[2] if len(sys.argv) > 2 else "c1"

def on_assign(_, parts):
    print(f"[{name}] ASSIGNED {[p.partition for p in parts]}")

def on_revoke(_, parts):
    print(f"[{name}] REVOKED  {[p.partition for p in parts]}")

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "lab06-pollinterval",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "max.poll.interval.ms": 10000,      # deliberately short
    "session.timeout.ms": 45000,        # heartbeats stay healthy throughout
    "heartbeat.interval.ms": 3000,
    "max.poll.records": 10,
})
c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{name}] max.poll.interval.ms=10000, sleeping {work}s per batch")
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[{name}] error: {msg.error()}")
            continue
        print(f"[{name}] got p{msg.partition()} off={msg.offset()} — working {work}s")
        time.sleep(work)
except KeyboardInterrupt:
    pass
except KafkaException as exc:
    print(f"[{name}] evicted: {exc}")
finally:
    c.close()
