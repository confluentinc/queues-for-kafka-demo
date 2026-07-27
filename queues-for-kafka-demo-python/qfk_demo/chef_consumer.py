#!/usr/bin/env python3
"""Chefs: KIP-932 ShareConsumer workers competing for orders off the same queue.

Start several instances of this program (e.g. via the dashboard's autoscaler,
or by hand in separate terminals) to see queue-style delivery: each order goes
to exactly one chef, even though every chef is subscribed to the same topic
and partition.
"""

import random
import signal
import sys
import time
from typing import Optional

import requests
from confluent_kafka import AcknowledgeType, ShareConsumer

from qfk_demo import config
from qfk_demo.models import TOPIC, Order

IDLE_HEARTBEAT_SECONDS = 3


def print_header(chef_name: str) -> None:
    print("╔═══════════════════════════════════════════╗")
    print("║         🔪  StreamBytes Kitchen           ║")
    print("╠═══════════════════════════════════════════╣")
    print(f"║  Chef: {chef_name:<34} ║")
    print("╚═══════════════════════════════════════════╝")
    print()


def print_chef(chef_name: str) -> None:
    print(f"👨🏻‍🍳 {chef_name} listening for new orders...", end="", flush=True)


def report_to_dashboard(
    dashboard_url: str,
    chef_name: str,
    order_id: Optional[str],
    action: str,
    order: Optional[Order],
    delivery_count: int,
) -> None:
    payload = {"orderId": order_id, "action": action, "deliveryCount": delivery_count}
    if order is not None and order.order_items:
        payload["orderItems"] = [item.to_dict() for item in order.order_items]
    try:
        requests.post(f"{dashboard_url}/api/chefs/{chef_name}", json=payload, timeout=2)
    except requests.RequestException:
        pass


def main() -> None:
    chef_name = sys.argv[1] if len(sys.argv) > 1 else "Unnamed Chef"
    print_header(chef_name)

    bootstrap_servers = config.get("bootstrap.servers", "localhost:9092")
    dashboard_url = config.get("dashboard.url", "http://localhost:8080")

    processing_delay_ms = config.get_int("chef.processing.delay.ms", 2000)
    if processing_delay_ms < 2000:
        print("⚠️  chef.processing.delay.ms is less than 2000ms, enforcing minimum 2000ms for throttling")
        processing_delay_ms = 2000

    accept_rate = config.get_float("chef.accept.rate", 0.80)
    release_rate = (1.0 - accept_rate) * 0.75  # 75% of non-accepts are releases

    group_id = config.get("chef.group.id", "chefs-share-group")
    acknowledgement_mode = config.get("share.acknowledgement.mode", "explicit")

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": chef_name.replace(" ", "-"),
        "share.acknowledgement.mode": acknowledgement_mode,
    }

    running = True

    def handle_shutdown(signum, frame) -> None:
        nonlocal running
        print("\n🛑 Shutdown signal received, stopping consumer...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    rng = random.Random()

    print("Creating ShareConsumer...")
    consumer = ShareConsumer(conf)

    try:
        consumer.subscribe([TOPIC])
        print(f"\n✅ Subscribed to topic '{TOPIC}' (group='{group_id}')")
        print(
            f"⚙️  Accept rate: {accept_rate * 100:.0f}%, Release rate: {release_rate * 100:.0f}%, "
            f"Reject rate: {(1.0 - accept_rate - release_rate) * 100:.0f}%"
        )
        print(f"⏱️  Processing delay: {processing_delay_ms} ms")
        print()

        # Report to dashboard immediately so the chef appears before processing any orders.
        report_to_dashboard(dashboard_url, chef_name, None, "STARTED", None, 0)
        last_heartbeat = time.monotonic()

        print_chef(chef_name)

        while running:
            messages = consumer.poll(timeout=1.0)
            if not messages:
                now = time.monotonic()
                if now - last_heartbeat > IDLE_HEARTBEAT_SECONDS:
                    report_to_dashboard(dashboard_url, chef_name, None, "IDLE", None, 0)
                    last_heartbeat = now
                continue
            last_heartbeat = time.monotonic()

            for msg in messages:
                if not running:
                    break
                if msg.error():
                    print(f"⚠️ Record error: {msg.error()}")
                    continue

                order = Order.from_json(msg.value())
                order.chef_name = chef_name
                delivery_count = msg.delivery_count() or 1
                print(
                    f"\n✨ Received: {order.order_id} with {len(order.order_items)} items "
                    f"(offset={msg.offset()} | delivery count: {delivery_count})"
                )

                time.sleep(processing_delay_ms / 1000)

                decision = rng.random()
                if decision < accept_rate:
                    ack_type, action = AcknowledgeType.ACCEPT, "ACCEPTED"
                elif decision < accept_rate + release_rate:
                    ack_type, action = AcknowledgeType.RELEASE, "RELEASED"
                else:
                    ack_type, action = AcknowledgeType.REJECT, "REJECTED"
                order.status = action

                action_emoji = {"ACCEPTED": "✅", "RELEASED": "↩️", "REJECTED": "❌"}[action]
                print(f"{action_emoji} {action.lower()}: {order.order_id}")
                consumer.acknowledge(msg, ack_type)

                commit_results = consumer.commit_sync(timeout=10.0)
                commit_ok = True
                for topic_partition, exc in commit_results.items():
                    if exc is not None:
                        if "record state is invalid" not in str(exc):
                            print(f"⚠️ Commit warning: {exc}")
                        commit_ok = False

                if commit_ok:
                    if ack_type == AcknowledgeType.ACCEPT:
                        report_to_dashboard(dashboard_url, chef_name, order.order_id, action, order, delivery_count)
                    else:
                        report_to_dashboard(dashboard_url, chef_name, order.order_id, action, None, delivery_count)

                print()
                print_chef(chef_name)
    finally:
        report_to_dashboard(dashboard_url, chef_name, None, "SHUTDOWN", None, 0)
        consumer.close()


if __name__ == "__main__":
    main()
