"""Lab 5.4 — What idempotence adds to the bytes on disk.

    python3 04_idempotence.py on
    python3 04_idempotence.py off

Watch for: dump the log afterwards and compare the batch headers. With idempotence
on you get a real producerId and increasing sequence numbers; with it off,
producerId is -1 and there is no sequence to deduplicate against.
"""
import sys
from confluent_kafka import Producer
from common import BOOTSTRAP

mode = sys.argv[1] if len(sys.argv) > 1 else "on"
on = mode == "on"
topic = f"lab05.idem-{mode}"

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": on,
    "linger.ms": 20,
}
if on:
    # Idempotence requires acks=all and caps max.in.flight at 5; librdkafka enforces
    # both and will refuse to start if they are set inconsistently.
    conf["acks"] = "all"
else:
    conf["acks"] = 1

p = Producer(conf)
for i in range(30):
    p.produce(topic, key=f"k{i % 5}".encode(), value=f"record-{i}".encode())
p.flush(15)

print(f"enable.idempotence={on} -> {topic}")
print("  docker exec lab05-kafka /opt/kafka/bin/kafka-dump-log.sh --print-data-log \\")
print(f"    --files /tmp/kraft-combined-logs/{topic}-0/00000000000000000000.log | head -5")
