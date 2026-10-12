"""Lab 1.3 — Consumer groups and partition assignment.

Run:
    python 03_consumer_group.py g1 c1
Then, in more terminals:
    python 03_consumer_group.py g1 c2
    python 03_consumer_group.py g1 c3
    python 03_consumer_group.py g1 c4     <-- 4th consumer on a 3-partition topic

Watch for:
  - Each partition is assigned to exactly one consumer within a group.
  - The 4th consumer gets no partition at all, so adding more achieves nothing.
  - Every time a consumer joins or leaves, the REVOKED/ASSIGNED lines print — that is a
    rebalance.
  - Switch to another group (g2) and it replays everything, independently of g1.
"""
import sys
from confluent_kafka import Consumer
from common import BOOTSTRAP, TOPIC

group = sys.argv[1] if len(sys.argv) > 1 else "g1"
name = sys.argv[2] if len(sys.argv) > 2 else "c1"


def on_assign(consumer, partitions):
    assigned = [p.partition for p in partitions]
    print(f"[{name}] ASSIGNED  -> {assigned or 'NO PARTITIONS (idle)'}")


def on_revoke(consumer, partitions):
    print(f"[{name}] REVOKED   -> {[p.partition for p in partitions]}")


c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    # New consumer group protocol (KIP-848). Comment out to use the old client-side
    # (eager / cooperative) protocol instead.
    # "group.protocol": "consumer",
})
c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

print(f"[{name}] group={group} topic={TOPIC} — Ctrl+C to exit")
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[{name}] error: {msg.error()}")
            continue
        k = msg.key().decode() if msg.key() else "<null>"
        print(f"[{name}] p{msg.partition()} off={msg.offset():<4} key={k:<8} value={msg.value().decode()}")
except KeyboardInterrupt:
    pass
finally:
    c.close()
    print(f"[{name}] left the group (leave group -> rebalance)")
