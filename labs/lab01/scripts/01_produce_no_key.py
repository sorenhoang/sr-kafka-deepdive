"""Lab 1.1 — Producing WITHOUT a key: which partition does a record land on?

Watch for: the sticky partitioner packs many records into the SAME partition until the
batch fills or linger.ms expires, and only then switches. It is not per-record
round-robin.
"""
from collections import Counter
from confluent_kafka import Producer
from common import BOOTSTRAP, TOPIC

p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 0})

dist = Counter()


def on_delivery(err, msg):
    if err:
        print("ERROR:", err)
        return
    dist[msg.partition()] += 1
    print(f"  value={msg.value().decode():<12} -> partition {msg.partition()} offset {msg.offset()}")


print(f"Producing 12 records with NO key to {TOPIC}")
for i in range(12):
    p.produce(TOPIC, value=f"no-key-{i}".encode(), on_delivery=on_delivery)
p.flush(10)

print("\nDistribution:", dict(sorted(dist.items())))
print("Question: why is this not an even 4/4/4? (hint: sticky partitioner)")
