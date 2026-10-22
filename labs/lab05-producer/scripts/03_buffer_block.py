"""Lab 5.3 — Exhaust buffer.memory and watch send() stop being asynchronous.

Uses a deliberately tiny buffer and a short max.block.ms, then produces faster than
the broker can drain. Watch for: send() blocking, then raising BufferError. This is
broker backpressure arriving as application latency.

Run it with the broker stopped to see the extreme version:
    docker stop lab05-kafka && python3 03_buffer_block.py
"""
import time
from confluent_kafka import Producer, KafkaException
from common import BOOTSTRAP

TOPIC = "lab05.buffer"

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    # librdkafka sizes the queue by messages and kbytes rather than one buffer.memory
    "queue.buffering.max.messages": 500,
    "queue.buffering.max.kbytes": 256,
    "max.block.ms": 3000,
    "linger.ms": 100,
    "enable.idempotence": False,
    "acks": 1,
})

payload = b"y" * 1024
sent, blocked_total, started = 0, 0.0, time.monotonic()

try:
    for i in range(20000):
        t0 = time.monotonic()
        p.produce(TOPIC, value=payload)
        waited = time.monotonic() - t0
        if waited > 0.01:
            blocked_total += waited
            print(f"  record {i}: send() blocked {waited*1000:.0f}ms")
        sent += 1
        p.poll(0)
except (BufferError, KafkaException) as exc:
    print(f"\n  stopped after {sent} records: {type(exc).__name__}: {exc}")

print(f"\n{sent} accepted in {time.monotonic() - started:.1f}s, "
      f"{blocked_total:.1f}s of that spent blocked inside send()")
print("That blocked time is broker backpressure reaching your request latency.")
p.flush(5)
