import os

# Three brokers, exposed to the host through the EXTERNAL listener
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092")
TOPIC = os.getenv("TOPIC", "lab01.orders")
