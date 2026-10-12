"""Lab 0.1 — Publish the same five messages to a Kafka topic and a RabbitMQ queue."""
from confluent_kafka import Producer
import pika

from common import KAFKA_BOOTSTRAP, KAFKA_TOPIC, RABBIT_QUEUE, MESSAGES, rabbit_channel, banner

banner(f"Kafka -> topic {KAFKA_TOPIC}")
p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
for m in MESSAGES:
    p.produce(KAFKA_TOPIC, value=m.encode())
p.flush(10)
print(f"  published {len(MESSAGES)} records")

banner(f"RabbitMQ -> queue {RABBIT_QUEUE}")
conn, ch = rabbit_channel()
for m in MESSAGES:
    ch.basic_publish(
        exchange="",
        routing_key=RABBIT_QUEUE,
        body=m.encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
print(f"  published {len(MESSAGES)} messages")
print(f"  queue depth now: {ch.queue_declare(queue=RABBIT_QUEUE, durable=True, passive=True).method.message_count}")
conn.close()
