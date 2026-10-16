import os

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092")
TOPIC = os.getenv("TOPIC", "lab03.durable")
