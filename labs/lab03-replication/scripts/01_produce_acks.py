"""Lab 3.1 — Produce with a chosen acks setting and report exactly what happened.

    python3 01_produce_acks.py all 5
    python3 01_produce_acks.py 1 5

Watch for: with min.insync.replicas=2 and fewer than two in-sync replicas, acks=all
fails with NOT_ENOUGH_REPLICAS while acks=1 succeeds. The acks=1 success is the
dangerous one — it is an acknowledged write with no second copy.
"""
import sys
from confluent_kafka import Producer, KafkaException
from common import BOOTSTRAP, TOPIC

acks = sys.argv[1] if len(sys.argv) > 1 else "all"
count = int(sys.argv[2]) if len(sys.argv) > 2 else 5

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "acks": acks,
    "retries": 0,               # no retries, so a rejection is visible immediately
    "delivery.timeout.ms": 10000,
    "request.timeout.ms": 5000,
}
# Idempotence requires acks=all; turn it off when demonstrating weaker settings.
if acks != "all":
    conf["enable.idempotence"] = False

p = Producer(conf)
ok, failed = [], []


def on_delivery(err, msg):
    if err:
        failed.append(str(err))
    else:
        ok.append(msg.offset())


print(f"acks={acks}: producing {count} records to {TOPIC}")
for i in range(count):
    p.produce(TOPIC, key=f"k{i}".encode(), value=f"acks-{acks}-{i}".encode(),
              on_delivery=on_delivery)
p.flush(15)

print(f"  delivered: {len(ok)}" + (f"  offsets {ok}" if ok else ""))
if failed:
    print(f"  FAILED:    {len(failed)}")
    for f in sorted(set(failed)):
        print(f"    {f}")
    print("\n  A rejection here is Kafka refusing to accept a write it cannot make")
    print("  durable. That is the design working, not an outage.")
