#!/usr/bin/env python3
"""Waiters: an automated producer that floods the kitchen with orders.

Sends an initial burst of 40 orders (to show off autoscaling), then keeps
producing on a steady interval until interrupted.
"""

import os
import random
import signal
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from qfk_demo import config
from qfk_demo.models import TOPIC, Order, random_order_id, random_order_items

INITIAL_BATCH_SIZE = 40


def print_header() -> None:
    print("╔═══════════════════════════════════════════╗")
    print("║         🍽️  StreamBytes Restaurant        ║")
    print("╠═══════════════════════════════════════════╣")
    print("║  Waiters send orders to the kitchen via   ║")
    print("║  Kafka Queues (KIP-932 / Share Groups).   ║")
    print("╚═══════════════════════════════════════════╝")
    print()


def verify_topic_exists(bootstrap_servers: str) -> bool:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        metadata = admin.list_topics(timeout=5)
        return TOPIC in metadata.topics
    except Exception:
        return False


def delivery_report(err, msg, order_id: str, item_count: int, progress: str = "") -> None:
    if err is not None:
        print(f"❌ Failed to send order {order_id}: {err}")
        if "UNKNOWN_TOPIC" in str(err).upper():
            print(f"   This usually means the topic '{TOPIC}' doesn't exist or the broker is unreachable.")
    else:
        print(
            f"✅ Sent: {order_id} with {item_count} items "
            f"(partition={msg.partition()} | offset={msg.offset()}){progress}"
        )


def main() -> None:
    print_header()

    bootstrap_servers = config.get("bootstrap.servers", "localhost:9092")
    order_interval_ms = config.get_int("order.interval.ms", 5000)

    print(f"🔍 Connecting to Kafka at {bootstrap_servers}...")
    if not verify_topic_exists(bootstrap_servers):
        print(file=sys.stderr)
        print(f"❌ ERROR: Topic '{TOPIC}' does not exist or broker is not reachable.", file=sys.stderr)
        print(file=sys.stderr)
        print("📋 To fix this issue:", file=sys.stderr)
        print(file=sys.stderr)
        print("   1. Make sure Kafka is running:", file=sys.stderr)
        print("      docker compose up -d", file=sys.stderr)
        print(file=sys.stderr)
        print("   2. Wait for the kafka-init container to finish (it enables share", file=sys.stderr)
        print(f"      groups and creates the '{TOPIC}' topic):", file=sys.stderr)
        print("      docker compose logs -f kafka-init", file=sys.stderr)
        print(file=sys.stderr)
        sys.exit(1)
    print(f"✅ Topic '{TOPIC}' exists and broker is reachable.")
    print()

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": f"order-producer-{os.getpid()}",
        }
    )

    running = True

    def handle_shutdown(signum, frame) -> None:
        nonlocal running
        print("\nShutdown signal received...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    rng = random.Random()

    print()
    print(f"🚀 Generating initial batch of {INITIAL_BATCH_SIZE} orders to demonstrate autoscaling...")
    print()

    for i in range(INITIAL_BATCH_SIZE):
        if not running:
            break
        order_id = random_order_id(rng)
        items = random_order_items(rng)
        order = Order(order_id=order_id, order_items=items)
        progress = f" [{i + 1}/{INITIAL_BATCH_SIZE}]"
        producer.produce(
            TOPIC,
            key=order_id,
            value=order.to_json(),
            callback=lambda err, msg, oid=order_id, n=len(items), p=progress: delivery_report(err, msg, oid, n, p),
        )
        producer.poll(0)
    producer.flush()

    print()
    print(f"✅ Initial batch of {INITIAL_BATCH_SIZE} orders sent!")
    print()
    print(f"🛎️ Starting automated order generation (interval: {order_interval_ms} ms)...")
    print("Press [Ctrl+C] to stop...")
    print()

    try:
        while running:
            order_id = random_order_id(rng)
            items = random_order_items(rng)
            order = Order(order_id=order_id, order_items=items)
            producer.produce(
                TOPIC,
                key=order_id,
                value=order.to_json(),
                callback=lambda err, msg, oid=order_id, n=len(items): delivery_report(err, msg, oid, n),
            )
            producer.flush()

            slept_ms = 0
            while running and slept_ms < order_interval_ms:
                time.sleep(0.1)
                slept_ms += 100
    finally:
        producer.flush()
        print("Producer closed.")


if __name__ == "__main__":
    main()
