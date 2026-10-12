"""Lab 1.2 — Producing WITH a key: the key decides the partition.

Watch for: one key always lands on one partition. This is the only mechanism Kafka
gives you to guarantee ordering between related records.

    partition = toPositive(murmur2(key)) % num_partitions
"""
from collections import defaultdict
from confluent_kafka import Producer
from common import BOOTSTRAP, TOPIC

p = Producer({"bootstrap.servers": BOOTSTRAP})
mapping = defaultdict(set)


def on_delivery(err, msg):
    if err:
        print("ERROR:", err)
        return
    mapping[msg.key().decode()].add(msg.partition())


keys = ["user-1", "user-2", "user-3", "user-4", "user-5"]
print(f"Producing 3 records per key to {TOPIC}")
for k in keys:
    for seq in range(3):
        p.produce(TOPIC, key=k.encode(), value=f"{k}:event-{seq}".encode(), on_delivery=on_delivery)
p.flush(10)

print()
for k in keys:
    parts = mapping[k]
    flag = "ok (stable)" if len(parts) == 1 else "UNEXPECTED"
    print(f"  {k:<8} -> partition {sorted(parts)}  {flag}")
print("\nQuestion: if the topic were grown to 6 partitions now, would this mapping hold?")
