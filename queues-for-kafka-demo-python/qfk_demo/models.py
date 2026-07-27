"""Order/OrderItem model plus JSON (de)serialization shared by every component."""

from __future__ import annotations

import itertools
import json
import random
import string
import time
from dataclasses import dataclass, field
from typing import List, Optional

TOPIC = "orders-queue"

MENU_ITEMS = [
    "Pizza Margherita",
    "Pizza Pepperoni",
    "Pasta Carbonara",
    "Caesar Salad",
    "Burger",
    "Fish & Chips",
    "Steak",
    "Chicken Wings",
    "Soup",
    "Dessert",
]

INITIAL_INVENTORY = {
    "Pizza Margherita": 50,
    "Pizza Pepperoni": 50,
    "Pasta Carbonara": 40,
    "Caesar Salad": 30,
    "Burger": 40,
    "Fish & Chips": 35,
    "Steak": 30,
    "Chicken Wings": 45,
    "Soup": 40,
    "Dessert": 50,
}


@dataclass
class OrderItem:
    item_name: str
    quantity: int

    def to_dict(self) -> dict:
        return {"itemName": self.item_name, "quantity": self.quantity}

    @staticmethod
    def from_dict(data: dict) -> "OrderItem":
        return OrderItem(item_name=data["itemName"], quantity=int(data["quantity"]))


@dataclass
class Order:
    order_id: str
    order_items: List[OrderItem]
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    status: str = "PENDING"  # PENDING, ACCEPTED, RELEASED, REJECTED
    chef_name: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "orderId": self.order_id,
            "orderItems": [item.to_dict() for item in self.order_items],
            "timestamp": self.timestamp,
            "status": self.status,
            "chefName": self.chef_name,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")

    @staticmethod
    def from_dict(data: dict) -> "Order":
        return Order(
            order_id=data.get("orderId"),
            order_items=[OrderItem.from_dict(item) for item in (data.get("orderItems") or [])],
            timestamp=data.get("timestamp", 0),
            status=data.get("status", "PENDING"),
            chef_name=data.get("chefName"),
        )

    @staticmethod
    def from_json(data: bytes) -> "Order":
        return Order.from_dict(json.loads(data))


_order_sequence = itertools.count(1)
_order_id_prefix: Optional[str] = None


def random_order_id(rng: random.Random) -> str:
    # The random prefix is chosen once per process and stays constant for its
    # lifetime; only the sequence number advances per order.
    global _order_id_prefix
    if _order_id_prefix is None:
        _order_id_prefix = "".join(rng.choices(string.ascii_uppercase, k=3))
    sequence = next(_order_sequence)
    return f"ORD-{_order_id_prefix}-{sequence:04d}"


def random_order_items(rng: random.Random) -> List[OrderItem]:
    num_items = rng.randint(1, 3)
    names = rng.sample(MENU_ITEMS, num_items)
    return [OrderItem(item_name=name, quantity=rng.randint(1, 2)) for name in names]
