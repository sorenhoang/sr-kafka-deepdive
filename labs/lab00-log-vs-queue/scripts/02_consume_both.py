"""Lab 0.2 — Consume once from each side and look at what is left behind.

Watch for: both deliver the same five messages, but the queue depth in RabbitMQ drops
to zero while the Kafka topic is untouched. Kafka moved a cursor; RabbitMQ removed data.
"""
import sys
from confluent_kafka import Consumer

from common import (KAFKA_BOOTSTRAP, KAFKA_TOPIC, RABBIT_QUEUE, MESSAGES,
                    rabbit_channel, banner)

group = sys.argv[1] if len(sys.argv) > 1 else "reader-a"

banner(f"Kafka consumer group '{group}'")
c = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
})
c.subscribe([KAFKA_TOPIC])
got, empty = 0, 0
while got < len(MESSAGES) and empty < 8:
    msg = c.poll(1.0)
    if msg is None:
        empty += 1
        continue
    if msg.error():
        continue
    got += 1
    print(f"  offset {msg.offset()}  {msg.value().decode()}")
c.close()
print(f"  received {got}")

banner("RabbitMQ basic_get + ack")
conn, ch = rabbit_channel()
acked = 0
while True:
    method, _, body = ch.basic_get(queue=RABBIT_QUEUE, auto_ack=False)
    if method is None:
        break
    print(f"  delivery_tag {method.delivery_tag}  {body.decode()}")
    ch.basic_ack(method.delivery_tag)
    acked += 1
print(f"  received and acked {acked}")
depth = ch.queue_declare(queue=RABBIT_QUEUE, durable=True, passive=True).method.message_count
print(f"  queue depth now: {depth}")
conn.close()

banner("What is left on each side")
print("  Kafka:    the records are still on disk; only the group's offset moved.")
print(f"  RabbitMQ: queue depth {depth} — acking removed the messages.")
