"""Lab 5.6 — The two record-size limits, and which one rejects you.

    python3 06_too_large.py [kib]

Watch for: a client-side rejection (max.request.size, instant, never leaves the
process) versus a broker-side one (message.max.bytes, after a round trip). Raising
only one moves the failure rather than removing it.
"""
import sys
from confluent_kafka import Producer, KafkaException
from common import BOOTSTRAP

kib = int(sys.argv[1]) if len(sys.argv) > 1 else 2048
TOPIC = "lab05.size"

for limit in (1_000_000, 4_000_000):
    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "message.max.bytes": limit,     # librdkafka's name for max.request.size
        "enable.idempotence": False,
        "acks": 1,
    })
    payload = b"z" * (kib * 1024)
    outcome = {}

    def on_delivery(err, msg):
        outcome["err"] = err

    print(f"\nclient limit {limit:,}B, record {len(payload):,}B")
    try:
        p.produce(TOPIC, value=payload, on_delivery=on_delivery)
        p.flush(15)
        err = outcome.get("err")
        print(f"  broker said: {err if err else 'accepted'}")
    except (KafkaException, BufferError) as exc:
        print(f"  rejected client-side before sending: {exc}")

print("\nThe broker's own limit is message.max.bytes (default ~1 MiB, settable per")
print("topic). Raise the producer's without raising the broker's and the rejection")
print("just moves to the far side of the network.")
