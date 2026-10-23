"""Lab 6.0 — Fill the topic so the other steps have something to chew on."""
import sys
from confluent_kafka import Producer
from common import BOOTSTRAP, TOPIC

count = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "acks": 1,
              "enable.idempotence": False})
for i in range(count):
    p.produce(TOPIC, key=f"user-{i % 200}".encode(), value=f"event-{i}".encode())
    if i % 1000 == 0:
        p.poll(0)
p.flush(30)
print(f"seeded {count} records into {TOPIC}")
