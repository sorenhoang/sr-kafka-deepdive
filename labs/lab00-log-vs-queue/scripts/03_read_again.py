"""Lab 0.3 — Read a second time, and read as a brand new reader.

Watch for:
  - A NEW Kafka consumer group replays all five records from the beginning.
  - RabbitMQ has nothing to give anyone: the messages are gone.

This is the single most consequential difference between the two models, and the
reason replay, reprocessing and adding a new downstream consumer are free in Kafka.
"""
import time
from confluent_kafka import Consumer

from common import KAFKA_BOOTSTRAP, KAFKA_TOPIC, RABBIT_QUEUE, MESSAGES, rabbit_channel, banner

group = f"reader-new-{int(time.time())}"
banner(f"Kafka — brand new consumer group '{group}'")
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
print(f"  replayed {got} record(s) that another group already consumed")

banner("RabbitMQ — another reader on the same queue")
conn, ch = rabbit_channel()
method, _, body = ch.basic_get(queue=RABBIT_QUEUE, auto_ack=False)
if method is None:
    print("  nothing to read — the messages were removed when the first consumer acked")
else:
    print(f"  {body.decode()}")
    ch.basic_nack(method.delivery_tag, requeue=True)
conn.close()

banner("Takeaway")
print("  Kafka deletes by retention policy, never by consumption, so any number of")
print("  independent readers can read the same data and read it again.")
print("  To fan out in RabbitMQ you must decide up front: an exchange has to copy the")
print("  message into one queue per consumer, before it is published.")
