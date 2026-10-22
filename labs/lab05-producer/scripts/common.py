import os

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
LOG_DIR = "/tmp/kraft-combined-logs"

# The Python client is librdkafka, and several of its defaults differ from the Java
# client's — linger.ms is 5 rather than 0, enable.idempotence is off rather than on.
# Every script below sets what it depends on explicitly rather than inheriting either.
