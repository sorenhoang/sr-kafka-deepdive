"""Lab 1.4 — What Kafka's ordering guarantee actually covers.

Produces two keys, five numbered events each, then consumes the whole topic and prints
records in the order the consumer received them.

Watch for:
  - Within one partition, a key's sequence numbers always increase  -> guaranteed.
  - Across the topic, the two keys' events interleave arbitrarily   -> not guaranteed.

So "Kafka preserves order" is only ever true at partition scope, never topic scope.
"""
from confluent_kafka import Producer, Consumer
from common import BOOTSTRAP, TOPIC

ORDER_TOPIC = TOPIC + ".ordering"

p = Producer({"bootstrap.servers": BOOTSTRAP})
for seq in range(5):
    for k in ("A", "B"):
        p.produce(ORDER_TOPIC, key=k.encode(), value=f"{k}-seq{seq}".encode())
p.flush(10)

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "ordering-demo",
    "auto.offset.reset": "earliest",
})
c.subscribe([ORDER_TOPIC])

print("Order as the consumer sees it (whole topic):")
seen, empty = 0, 0
per_partition = {}
while seen < 10 and empty < 10:
    msg = c.poll(1.0)
    if msg is None:
        empty += 1
        continue
    if msg.error():
        continue
    seen += 1
    v = msg.value().decode()
    per_partition.setdefault(msg.partition(), []).append(v)
    print(f"  p{msg.partition()} off={msg.offset():<3} {v}")
c.close()

print("\nOrder within each partition:")
for part, vals in sorted(per_partition.items()):
    print(f"  p{part}: {vals}")
