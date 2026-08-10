#!/usr/bin/env python3
"""Dashboard: serves the web UI and REST API tying the whole demo together.

Runs the inventory consumer and (optionally) the chef auto-scaler as
background threads/processes, and exposes a small REST API that the static
dashboard frontend (public/) polls once a second.
"""

import json
import os
import random
import signal
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from flask import Flask, jsonify, request, send_from_directory

from qfk_demo import config
from qfk_demo.autoscaler import ChefAutoScaler
from qfk_demo.inventory_consumer import InventoryConsumer
from qfk_demo.models import INITIAL_INVENTORY, MENU_ITEMS, TOPIC, Order, OrderItem, random_order_id

STATE_FILE = Path("dashboard-state.json")
CHEF_STALE_THRESHOLD_SECONDS = 5
SEND_TIMEOUT_SECONDS = 5
MAX_CONSECUTIVE_FAILURES = 3
PUBLIC_DIR = Path(__file__).resolve().parent.parent / "public"

app = Flask(__name__, static_folder=None)


class DashboardState:
    def __init__(self, inventory_consumer: InventoryConsumer, autoscaler: Optional[ChefAutoScaler]):
        self.inventory_consumer = inventory_consumer
        self.autoscaler = autoscaler

        self.lock = threading.Lock()
        self.chef_statuses: Dict[str, dict] = {}
        self.inventory: Dict[str, int] = dict(INITIAL_INVENTORY)
        self.processed_orders: set = set()
        self.rejected_orders: set = set()

        self.completed_baseline = 0

        self.producer: Optional[Producer] = None
        self._rng = random.Random()

        self._load_state()
        inventory_consumer.add_listener(self._on_inventory_update)

    # -- inventory ---------------------------------------------------

    def _on_inventory_update(self, snapshot: Dict[str, int]) -> None:
        with self.lock:
            self.inventory.update(snapshot)

    # -- persistence ---------------------------------------------------

    def _load_state(self) -> None:
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
                self.processed_orders = set(data.get("processedOrders", []))
                self.rejected_orders = set(data.get("rejectedOrders", []))
                if data.get("inventory"):
                    self.inventory.update(data["inventory"])
                self.completed_baseline = len(self.processed_orders) + len(self.rejected_orders)
                print(
                    f"📂 Loaded state: {len(self.processed_orders)} processed, "
                    f"{len(self.rejected_orders)} rejected orders (baseline={self.completed_baseline})"
                )
            except (json.JSONDecodeError, OSError) as e:
                print(f"📂 Could not load state file, starting fresh: {e}")
        else:
            print("📂 No state file found, starting fresh")

    def save_state(self) -> None:
        with self.lock:
            data = {
                "processedOrders": list(self.processed_orders),
                "rejectedOrders": list(self.rejected_orders),
                "inventory": dict(self.inventory),
            }
        try:
            STATE_FILE.write_text(json.dumps(data))
        except OSError as e:
            print(f"⚠️ Could not save state file: {e}")

    # -- producer / order generation ---------------------------------

    def _init_producer(self) -> None:
        if self.producer is None:
            bootstrap_servers = config.get("bootstrap.servers", "localhost:9092")
            self.producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": f"dashboard-order-generator-{os.getpid()}",
                    "message.timeout.ms": 30000,
                }
            )
            print("📤 Kafka producer initialized for order generation")

    def _random_order_items(self):
        num_items = self._rng.randint(1, 3)
        names = self._rng.sample(MENU_ITEMS, num_items)
        return [OrderItem(item_name=name, quantity=self._rng.randint(1, 2)) for name in names]

    def generate_orders(self, count: int) -> dict:
        self._init_producer()

        success_count = 0
        fail_count = 0
        consecutive_failures = 0
        last_error = None

        for i in range(count):
            order_id = random_order_id(self._rng)
            items = self._random_order_items()
            order = Order(order_id=order_id, order_items=items)

            delivery_error = {}

            def on_delivery(err, msg, delivery_error=delivery_error):
                if err is not None:
                    delivery_error["err"] = err

            try:
                self.producer.produce(TOPIC, key=order_id, value=order.to_json(), callback=on_delivery)
                # flush() blocks (up to the timeout) until this message's delivery
                # callback has fired, mirroring the Java demo's producer.send().get().
                remaining = self.producer.flush(SEND_TIMEOUT_SECONDS)
                if remaining > 0:
                    raise KafkaException("send timed out")
                if "err" in delivery_error:
                    raise KafkaException(delivery_error["err"])

                success_count += 1
                consecutive_failures = 0
                print(f"✅ Generated: {order_id} with {len(items)} items [{i + 1}/{count}]")
            except Exception as e:
                fail_count += 1
                consecutive_failures += 1
                last_error = str(e)
                print(f"❌ Failed to send order {order_id}: {e} [{i + 1}/{count}]")

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                remaining = count - i - 1
                fail_count += remaining
                print(f"🛑 Bailing out after {MAX_CONSECUTIVE_FAILURES} consecutive failures ({remaining} orders skipped)")
                break

        result = {"requested": count, "generated": success_count, "failed": fail_count}
        if success_count == count:
            result["status"] = "success"
        elif success_count == 0:
            result["status"] = "error"
            if last_error:
                result["error"] = last_error
        else:
            result["status"] = "partial"
            if last_error:
                result["error"] = last_error
        return result

    # -- queue depth ---------------------------------------------------

    def local_queue_depth(self) -> int:
        # messages_seen comes from the inventory consumer, a standard consumer
        # group that sees every order on the topic regardless of which process
        # produced it (dashboard-generated or the standalone producer.py).
        # orders_produced only counted dashboard-generated orders, so queue
        # depth was always 0 (and auto-scaling never triggered) whenever
        # orders came from the standalone producer instead.
        total_produced = self.inventory_consumer.messages_seen()
        with self.lock:
            total_completed = len(self.processed_orders) + len(self.rejected_orders)
            completed_since_baseline = total_completed - self.completed_baseline
        return max(0, total_produced - completed_since_baseline)

    def reset(self) -> None:
        with self.lock:
            self.processed_orders.clear()
            self.rejected_orders.clear()
            self.completed_baseline = 0
            self.order_sequence = 0
            self.inventory = dict(INITIAL_INVENTORY)
            self.chef_statuses.clear()
        self.inventory_consumer.reset_offset_baseline()
        self.save_state()
        print("🔄 State reset via API (queue depth baseline reset)")

    def stop(self) -> None:
        print("💾 Saving state before shutdown...")
        self.save_state()
        if self.producer is not None:
            self.producer.flush(5)
            print("📤 Kafka producer closed")
        if self.autoscaler is not None:
            self.autoscaler.stop()


state: Optional[DashboardState] = None


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response


@app.route("/<path:filename>")
def static_files(filename):
    return send_from_directory(PUBLIC_DIR, filename)


@app.route("/")
def index():
    return send_from_directory(PUBLIC_DIR, "index.html")


@app.route("/api/inventory")
def get_inventory():
    with state.lock:
        return jsonify(state.inventory)


@app.route("/api/chefs")
def get_chefs():
    if state.autoscaler is not None:
        active_names = set(state.autoscaler.active_chef_names)
        with state.lock:
            for name in list(state.chef_statuses):
                if name not in active_names:
                    del state.chef_statuses[name]
            statuses = list(state.chef_statuses.values())
    else:
        now = time.time()
        with state.lock:
            for name in list(state.chef_statuses):
                if now - state.chef_statuses[name]["lastUpdateTime"] / 1000 > CHEF_STALE_THRESHOLD_SECONDS:
                    del state.chef_statuses[name]
            statuses = list(state.chef_statuses.values())
    return jsonify(statuses)


@app.route("/api/chefs/<chef_name>", methods=["POST"])
def post_chef_status(chef_name):
    try:
        data = request.get_json(force=True) or {}
        order_id = data.get("orderId")
        action = data.get("action")

        if action == "SHUTDOWN":
            with state.lock:
                state.chef_statuses.pop(chef_name, None)
            print(f"👋 Chef {chef_name} shut down, removed from dashboard")
            return jsonify({"status": "removed"})

        with state.lock:
            status = state.chef_statuses.setdefault(
                chef_name,
                {
                    "chefName": chef_name,
                    "lastOrderId": None,
                    "lastAction": None,
                    "lastUpdateTime": 0,
                    "totalOrdersProcessed": 0,
                    "lastOrderDeliveryCount": 0,
                },
            )
            status["lastAction"] = action
            status["lastUpdateTime"] = int(time.time() * 1000)

            if order_id is not None:
                status["lastOrderId"] = order_id
                status["totalOrdersProcessed"] += 1

            delivery_count = data.get("deliveryCount")
            if delivery_count is not None:
                status["lastOrderDeliveryCount"] = int(delivery_count)

            if order_id is not None:
                if action == "ACCEPTED" and order_id not in state.processed_orders:
                    for item in data.get("orderItems") or []:
                        item_name = item.get("itemName")
                        quantity = int(item.get("quantity", 1))
                        current = state.inventory.get(item_name, 50)
                        state.inventory[item_name] = max(0, current - quantity)
                    state.processed_orders.add(order_id)
                    print(f"📦 Order accepted: {order_id} (delivery count: {status['lastOrderDeliveryCount']})")
                elif action == "REJECTED" and order_id not in state.rejected_orders:
                    state.rejected_orders.add(order_id)
                    print(f"🗑️ Order rejected: {order_id}")

            result = dict(status)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/inventory/update", methods=["POST"])
def update_inventory():
    try:
        updates = request.get_json(force=True) or {}
        with state.lock:
            state.inventory.update(updates)
            snapshot = dict(state.inventory)
        return jsonify(snapshot)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/inventory/restock", methods=["POST"])
def restock_inventory():
    amount = 25
    body = request.get_json(silent=True) or {}
    if "amount" in body:
        amount = int(body["amount"])

    with state.lock:
        for item in state.inventory:
            state.inventory[item] += amount
        snapshot = dict(state.inventory)

    print(f"📦 Restocked inventory: +{amount} to each item")
    return jsonify({"status": "success", "amountAdded": amount, "inventory": snapshot})


@app.route("/api/orders/accepted-count")
def accepted_count():
    with state.lock:
        accepted = len(state.processed_orders)
        rejected = len(state.rejected_orders)
    return jsonify({"acceptedCount": accepted + rejected, "accepted": accepted, "rejected": rejected})


@app.route("/api/autoscale")
def autoscale_status():
    queue_depth = state.local_queue_depth()
    result = {"queueDepth": queue_depth}

    if state.autoscaler is not None:
        with state.lock:
            rejected = len(state.rejected_orders)
        result.update(
            {
                "activeChefs": state.autoscaler.active_chef_count,
                "targetChefs": state.autoscaler.target_chef_count,
                "activeChefNames": state.autoscaler.active_chef_names,
                "rejectedOrders": rejected,
                "enabled": True,
                "manualMode": state.autoscaler.manual_mode,
                "maxChefs": state.autoscaler.max_chefs,
                "minChefs": state.autoscaler.min_chefs,
            }
        )
    else:
        result["enabled"] = False
    return jsonify(result)


@app.route("/api/scaling/add-chef", methods=["POST"])
def add_chef():
    if state.autoscaler is None:
        return jsonify({"error": "Scaling not enabled"}), 400
    if not state.autoscaler.manual_mode:
        return jsonify({"error": "Manual scaling not enabled. Set manual.scale.mode=true in config.properties"}), 400
    if state.autoscaler.active_chef_count >= state.autoscaler.max_chefs:
        return jsonify({"error": "Maximum chef limit reached"}), 400
    state.autoscaler.spawn_chef()
    return jsonify({"status": "success", "activeChefs": state.autoscaler.active_chef_count})


@app.route("/api/scaling/remove-chef", methods=["POST"])
def remove_chef():
    if state.autoscaler is None:
        return jsonify({"error": "Scaling not enabled"}), 400
    if not state.autoscaler.manual_mode:
        return jsonify({"error": "Manual scaling not enabled. Set manual.scale.mode=true in config.properties"}), 400
    if state.autoscaler.active_chef_count <= state.autoscaler.min_chefs:
        return jsonify({"error": "Minimum chef limit reached"}), 400
    state.autoscaler.terminate_chef()
    return jsonify({"status": "success", "activeChefs": state.autoscaler.active_chef_count})


@app.route("/api/autoscale/queue-depth")
def queue_depth_endpoint():
    return jsonify({"queueDepth": state.local_queue_depth()})


@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/reset", methods=["POST"])
def reset():
    state.reset()
    return jsonify({"status": "reset"})


@app.route("/api/orders/generate", methods=["POST"])
def generate_orders_endpoint():
    count = 40
    body = request.get_json(silent=True) or {}
    if "count" in body:
        count = int(body["count"])

    print(f"🚀 Generating {count} orders via dashboard...")
    try:
        result = state.generate_orders(count)
        status_code = 502 if result.get("status") == "error" else 200
        return jsonify(result), status_code
    except Exception as e:
        print(f"❌ Unexpected error generating orders: {e}")
        return (
            jsonify(
                {"status": "error", "error": f"Internal error: {e}", "generated": 0, "failed": count, "requested": count}
            ),
            500,
        )


def _periodic_state_save(interval_seconds: int = 10) -> None:
    while True:
        time.sleep(interval_seconds)
        state.save_state()


def _handle_sigterm(signum, frame) -> None:
    # SIGINT already raises KeyboardInterrupt by default, which the try/finally
    # below relies on to stop the autoscaler's spawned chef subprocesses.
    # Without this, SIGTERM (e.g. `kill`, `pkill`, an IDE stop button) would
    # skip that cleanup entirely and leave chef processes orphaned.
    raise KeyboardInterrupt()


def main() -> None:
    global state

    signal.signal(signal.SIGTERM, _handle_sigterm)

    port = config.get_int("port", 8080)

    inventory_consumer = InventoryConsumer()

    autoscaler = None
    auto_scale_enabled = config.get("auto.scale.enabled", "true").lower() != "false"
    manual_mode = config.get("manual.scale.mode", "false").lower() == "true"
    if auto_scale_enabled:
        autoscaler = ChefAutoScaler(manual_mode=manual_mode)

    state = DashboardState(inventory_consumer, autoscaler)

    inventory_thread = threading.Thread(target=inventory_consumer.run, daemon=True)
    inventory_thread.start()

    if autoscaler is not None:
        autoscaler.start()

    save_thread = threading.Thread(target=_periodic_state_save, daemon=True)
    save_thread.start()

    print("╔═══════════════════════════════════════════╗")
    print("║         🌐 Dashboard Server Started       ║")
    print("╠═══════════════════════════════════════════╣")
    print(f"║  URL: http://localhost:{port:<18} ║")
    print("╚═══════════════════════════════════════════╝")
    print()

    try:
        app.run(host="0.0.0.0", port=port, threaded=True)
    finally:
        state.stop()
        inventory_consumer.stop()


if __name__ == "__main__":
    main()
