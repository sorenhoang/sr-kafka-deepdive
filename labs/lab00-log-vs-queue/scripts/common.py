import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "lab00.events")
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/%2F")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "lab00.events")

MESSAGES = [f"event-{i}" for i in range(1, 6)]


def rabbit_channel():
    import pika
    conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
    ch = conn.channel()
    ch.queue_declare(queue=RABBIT_QUEUE, durable=True)
    return conn, ch


def banner(title):
    print()
    print(title)
    print("-" * len(title))
