"""Lab 6.5 — Count revoked partitions: eager vs cooperative sticky.

    python3 05_assignor_compare.py range c1
    python3 05_assignor_compare.py cooperative-sticky c1

Start two consumers with the same strategy, then add a third. Add up the REVOKED
partitions printed across the existing consumers.

Watch for: with `range` every consumer revokes everything before reassignment. With
`cooperative-sticky` only the partitions actually moving are revoked.
"""
import sys
from confluent_kafka import Consumer
from common import BOOTSTRAP, TOPIC

strategy = sys.argv[1] if len(sys.argv) > 1 else "range"
name = sys.argv[2] if len(sys.argv) > 2 else "c1"
revoked_total = {"n": 0}

def on_assign(_, parts):
    print(f"[{name}] ASSIGNED {[p.partition for p in parts]}")

def on_revoke(_, parts):
    revoked_total["n"] += len(parts)
    print(f"[{name}] REVOKED  {[p.partition for p in parts]}  "
          f"(running total {revoked_total['n']})")

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": f"lab06-assignor-{strategy}",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "partition.assignment.strategy": strategy,
})
c.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)
print(f"[{name}] strategy={strategy} — Ctrl+C to leave the group")
try:
    while True:
        c.poll(1.0)
except KeyboardInterrupt:
    pass
finally:
    c.close()
    print(f"[{name}] total partitions revoked over this session: {revoked_total['n']}")
