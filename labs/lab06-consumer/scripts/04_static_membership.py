"""Lab 6.4 — Static membership removes the restart rebalance.

    python3 04_static_membership.py c1            # dynamic member
    python3 04_static_membership.py c1 static-1   # static member

Start two copies, then Ctrl+C one and start it again. Without group.instance.id the
other consumer logs REVOKED/ASSIGNED twice (leave and rejoin). With it, and a restart
inside session.timeout.ms, the other consumer logs nothing at all.
"""
import sys
from confluent_kafka import Consumer
from common import BOOTSTRAP, TOPIC

name = sys.argv[1] if len(sys.argv) > 1 else "c1"
instance = sys.argv[2] if len(sys.argv) > 2 else None

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "lab06-static",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "session.timeout.ms": 45000,
    "partition.assignment.strategy": "cooperative-sticky",
}
if instance:
    conf["group.instance.id"] = instance

def on_assign(_, parts):
    print(f"[{name}] ASSIGNED {[p.partition for p in parts]}")

def on_revoke(_, parts):
    print(f"[{name}] REVOKED  {[p.partition for p in parts]}")

c = Consumer(conf)
c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
print(f"[{name}] group.instance.id={instance or '<none, dynamic>'} — Ctrl+C to stop")
try:
    while True:
        msg = c.poll(1.0)
        if msg and not msg.error():
            pass
except KeyboardInterrupt:
    pass
finally:
    c.close()
    print(f"[{name}] closed")
