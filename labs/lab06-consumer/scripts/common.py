import os

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "lab06.events")

# librdkafka names differ from the Java client in places. Relevant here:
#   max.poll.interval.ms   same name
#   session.timeout.ms     same name
#   group.instance.id      same name (static membership)
#   enable.auto.commit     same name, default true as in Java
