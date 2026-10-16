"""Lab 3.2 — Make the high watermark visible.

Produces a batch, then polls for it as a consumer and reports how long it takes to
become readable.

With all replicas healthy this is milliseconds. With a follower paused
(`docker pause lab03-kafka3`) and acks=1, the record is on the leader but the high
watermark cannot advance past the paused replica's log end offset — so the record is
written and NOT readable, until replica.lag.time.max.ms expires and the leader drops
that replica from the ISR, which lets the HW move.

    python3 02_watch_hw.py [acks]
"""
import sys
import time
import uuid
from confluent_kafka import Producer, Consumer
from common import BOOTSTRAP, TOPIC

acks = sys.argv[1] if len(sys.argv) > 1 else "1"
marker = f"hw-probe-{uuid.uuid4().hex[:8]}"

conf = {"bootstrap.servers": BOOTSTRAP, "acks": acks, "request.timeout.ms": 5000}
if acks != "all":
    conf["enable.idempotence"] = False
p = Producer(conf)

state = {}


def on_delivery(err, msg):
    state["err"] = err
    state["offset"] = None if err else msg.offset()


p.produce(TOPIC, key=b"hw", value=marker.encode(), on_delivery=on_delivery)
p.flush(15)

if state.get("err"):
    print(f"produce failed: {state['err']}")
    sys.exit(1)

produced_at = time.monotonic()
print(f"produced {marker} at offset {state['offset']} with acks={acks}")
print("now polling as a consumer until it becomes readable...")

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": f"hw-{uuid.uuid4()}",
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
})
c.assign([])
from confluent_kafka import TopicPartition  # noqa: E402

c.assign([TopicPartition(TOPIC, 0, max(state["offset"], 0))])

deadline = produced_at + 90
while time.monotonic() < deadline:
    msg = c.poll(1.0)
    waited = time.monotonic() - produced_at
    if msg is None:
        print(f"  {waited:5.1f}s  not readable yet")
        continue
    if msg.error():
        continue
    if marker.encode() in (msg.value() or b""):
        print(f"\n  readable after {waited:.1f}s")
        if waited > 5:
            print("  That delay is the high watermark: the record was on the leader")
            print("  the whole time, but not on every in-sync replica.")
        break
c.close()
