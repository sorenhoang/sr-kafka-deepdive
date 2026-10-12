"""Lab 1.5 — Why GROWING A TOPIC breaks ordering (runs offline, no broker needed).

Kafka computes partition = toPositive(murmur2(key)) % num_partitions. Changing
num_partitions changes the divisor, so a key can move to a different partition: its old
events sit on partition A while new ones go to partition B, and the consumer no longer
sees them in order.
"""
def murmur2(data: bytes) -> int:
    length = len(data)
    seed = 0x9747B28C
    m, r = 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = (data[i] & 0xFF) | ((data[i+1] & 0xFF) << 8) | ((data[i+2] & 0xFF) << 16) | (data[i+3] << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= (k >> r)
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    rem = length % 4
    base = length - rem
    if rem == 3:
        h ^= (data[base+2] & 0xFF) << 16
    if rem >= 2:
        h ^= (data[base+1] & 0xFF) << 8
    if rem >= 1:
        h ^= (data[base] & 0xFF)
        h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 13)
    h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 15)
    return h

def partition_of(key: str, n: int) -> int:
    # Kafka: toPositive(murmur2(keyBytes)) % numPartitions
    return (murmur2(key.encode()) & 0x7FFFFFFF) % n

keys = [f"user-{i}" for i in range(1, 11)]
print(f"{'key':<10}{'3 parts':>9}{'6 parts':>9}   moved?")
moved = 0
for k in keys:
    a, b = partition_of(k, 3), partition_of(k, 6)
    changed = a != b
    moved += changed
    print(f"{k:<10}{a:>9}{b:>9}   {'YES -> ordering lost' if changed else '-'}")
print(f"\n{moved}/{len(keys)} keys changed partition going from 3 -> 6.")
print("This is why growing a topic is a one-way door, and why ordering breaks at the cutover.")
