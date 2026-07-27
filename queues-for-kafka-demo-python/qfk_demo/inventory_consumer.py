#!/usr/bin/env python3
"""Inventory tracker: a plain consumer group reading every order that flows

through the topic, side-by-side with the chefs' share group. Demonstrates
that queues (share groups) and classic streaming (consumer groups) can read
the same topic at the same time without interfering with each other.
"""

import os
import threading
from typing import Callable, Dict, List

from confluent_kafka import Consumer, KafkaException

from qfk_demo import config
from qfk_demo.models import INITIAL_INVENTORY, TOPIC, Order

GROUP_ID = "inventory-analytics-group"

InventoryListener = Callable[[Dict[str, int]], None]


class InventoryConsumer:
    """Tracks remaining stock by watching ACCEPTED orders go by."""

    def __init__(self) -> None:
        self._inventory: Dict[str, int] = dict(INITIAL_INVENTORY)
        self._lock = threading.Lock()
        self._listeners: List[InventoryListener] = []
        self._running = True
        self._messages_seen = 0

    def add_listener(self, listener: InventoryListener) -> None:
        self._listeners.append(listener)

    def current_inventory(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._inventory)

    def messages_seen(self) -> int:
        with self._lock:
            return self._messages_seen

    def reset_offset_baseline(self) -> None:
        with self._lock:
            self._messages_seen = 0
        print("📊 Message counter reset - queue depth will count from next message")

    def stop(self) -> None:
        self._running = False

    def _notify_listeners(self) -> None:
        snapshot = self.current_inventory()
        for listener in self._listeners:
            try:
                listener(snapshot)
            except Exception as e:
                print(f"Error notifying listener: {e}")

    def run(self) -> None:
        bootstrap_servers = config.get("bootstrap.servers", "localhost:9092")

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": GROUP_ID,
            "client.id": f"inventory-consumer-{os.getpid()}",
            # "latest" so we only count messages that arrive after this consumer starts,
            # matching the fact that chefs can only report to the dashboard while it's running.
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
        }

        consumer = Consumer(conf)
        consumer.subscribe([TOPIC])

        print("╔═══════════════════════════════════════════╗")
        print("║      📊 Inventory Analytics Consumer      ║")
        print("╠═══════════════════════════════════════════╣")
        print(f"║  Topic: {TOPIC:<33} ║")
        print(f"║  Group: {GROUP_ID:<33} ║")
        print("╚═══════════════════════════════════════════╝")
        print()
        print("✅ Inventory consumer started. Tracking new orders...")
        print()

        try:
            while self._running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())

                with self._lock:
                    self._messages_seen += 1

                order = Order.from_json(msg.value())
                if order.status == "ACCEPTED" and order.order_items:
                    with self._lock:
                        for item in order.order_items:
                            current = self._inventory.get(item.item_name, INITIAL_INVENTORY.get(item.item_name, 0))
                            self._inventory[item.item_name] = max(0, current - item.quantity)
                    print(f"📦 Order {order.order_id} (ACCEPTED): Inventory updated")
                    self._notify_listeners()
        finally:
            consumer.close()
            print("Inventory consumer closed.")


def main() -> None:
    InventoryConsumer().run()


if __name__ == "__main__":
    main()
