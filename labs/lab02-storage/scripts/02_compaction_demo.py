"""Lab 2.2 — Give the log cleaner something to compact.

Writes several versions of each key, then a tombstone (a record with a null value)
for one of them. Watch for: after compaction only the newest value per key survives,
and the tombstone eventually removes its key entirely.

Note the offsets printed here — compaction does NOT renumber anything, so the
surviving records keep the offsets they were written with, and the sequence has holes.
"""
from confluent_kafka import Producer
from common import BOOTSTRAP

TOPIC = "lab02.compacted"

p = Producer({"bootstrap.servers": BOOTSTRAP})
written = []


def on_delivery(err, msg):
    if err:
        print("ERROR:", err)
        return
    k = msg.key().decode()
    v = msg.value().decode() if msg.value() is not None else "<tombstone>"
    written.append((msg.offset(), k, v))


print(f"Writing 3 versions of 4 keys to {TOPIC}, then a tombstone for user-2")
for version in range(1, 4):
    for n in range(1, 5):
        p.produce(TOPIC, key=f"user-{n}".encode(),
                  value=f"balance=v{version}".encode(), on_delivery=on_delivery)

# A null value is a tombstone: it marks the key for removal during compaction.
p.produce(TOPIC, key=b"user-2", value=None, on_delivery=on_delivery)
p.flush(15)

for off, k, v in sorted(written):
    print(f"  offset {off:<3} {k:<8} {v}")
print("\nAfter compaction, expect: user-1/3/4 at balance=v3, and user-2 gone.")
print("The surviving offsets stay exactly as printed above — compaction leaves gaps.")
