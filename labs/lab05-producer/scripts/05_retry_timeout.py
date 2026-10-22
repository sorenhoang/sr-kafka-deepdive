"""Lab 5.5 — Retries do not end when `retries` runs out.

Run this with the broker stopped:
    docker stop lab05-kafka && python3 05_retry_timeout.py

Watch for: the producer retrying for the full delivery timeout, not for `retries`
attempts. The time to failure tracks delivery.timeout.ms (here 8s), which is the
setting people mean when they change `retries` and wonder why nothing happens.
"""
import time
from confluent_kafka import Producer
from common import BOOTSTRAP

TOPIC = "lab05.retry"

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": False,
    "retries": 3,                 # looks like the limit; is not
    "delivery.timeout.ms": 8000,  # this is the limit
    "request.timeout.ms": 2000,
    "retry.backoff.ms": 200,
    "socket.timeout.ms": 2000,
})

result = {}
started = time.monotonic()


def on_delivery(err, msg):
    result["elapsed"] = time.monotonic() - started
    result["err"] = err


p.produce(TOPIC, value=b"will this land?", on_delivery=on_delivery)
p.flush(20)

print(f"retries=3, delivery.timeout.ms=8000")
print(f"  failed after {result.get('elapsed', 0):.1f}s")
print(f"  {result.get('err')}")
print("\n  Three retries at 200ms backoff would be under a second. It took the")
print("  delivery timeout, because that is what actually bounds the attempt.")
