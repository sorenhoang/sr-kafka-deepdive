"""Lab 3.3 — Poll the topic description and print only when the ISR changes.

Leave this running in its own terminal while you pause, kill and restart brokers.
It turns the ISR from a thing you check into a thing you watch.

    python3 03_watch_isr.py [seconds]
"""
import sys
import time
from confluent_kafka.admin import AdminClient
from common import BOOTSTRAP, TOPIC

duration = int(sys.argv[1]) if len(sys.argv) > 1 else 300
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

print(f"watching {TOPIC} for {duration}s — printing only on change\n")
started, last = time.monotonic(), None
while time.monotonic() - started < duration:
    try:
        md = admin.list_topics(topic=TOPIC, timeout=5)
        t = md.topics.get(TOPIC)
        if t is None or t.error is not None:
            snapshot = f"topic unavailable: {t.error if t else 'missing'}"
        else:
            rows = []
            for pid in sorted(t.partitions):
                part = t.partitions[pid]
                rows.append(f"p{pid} leader={part.leader} "
                            f"replicas={sorted(part.replicas)} isr={sorted(part.isrs)}")
            snapshot = " | ".join(rows)
    except Exception as exc:                     # broker unreachable mid-failover
        snapshot = f"metadata error: {exc}"

    if snapshot != last:
        print(f"[{time.strftime('%H:%M:%S')}] {snapshot}")
        last = snapshot
    time.sleep(1)
