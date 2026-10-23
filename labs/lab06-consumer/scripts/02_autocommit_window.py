"""Lab 6.2 — Measure exactly how much auto-commit reprocesses.

Phase 1 consumes with auto-commit on, processes slowly, and is killed mid-stream.
Phase 2 restarts the same group and counts how many records it sees again.

    python3 02_autocommit_window.py consume <group>     # Ctrl+C after a few seconds
    python3 02_autocommit_window.py recount <group>

Watch for: the second run replays everything processed since the last 5-second
auto-commit tick — work that was finished but never recorded as finished.
"""
import sys
import time
from confluent_kafka import Consumer
from common import BOOTSTRAP, TOPIC

mode = sys.argv[1]
group = sys.argv[2] if len(sys.argv) > 2 else "lab06-autocommit"

c = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": group,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000,
    "max.poll.records": 50,
})
c.subscribe([TOPIC])

seen = 0
started = time.monotonic()
try:
    while True:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            if mode == "recount" and time.monotonic() - started > 6:
                break
            continue
        seen += 1
        if mode == "consume":
            time.sleep(0.01)          # pretend to do work
            if seen % 50 == 0:
                print(f"  processed {seen} (Ctrl+C any time)")
except KeyboardInterrupt:
    print(f"\nkilled after processing {seen} records")
    print("Now run:  python3 02_autocommit_window.py recount " + group)
    raise SystemExit(0)

print(f"replayed {seen} records that the first run had already processed")
print("That is the auto-commit window: work finished but not recorded as finished.")
c.close()
