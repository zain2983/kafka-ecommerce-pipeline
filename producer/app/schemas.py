"""
Defines what a "valid" event looks like for each event type.

This is intentionally plain dicts/functions rather than a heavyweight
schema library (e.g. pydantic) - the goal in Phase 2 is just to produce
realistic, structured Python dicts. Kafka serialization (Phase 3) and
strict validation (Phase 4, on the consumer side) come later.
"""

import random

EVENT_TYPES = ["USER_SIGNUP", "PRODUCT_VIEW", "ADD_TO_CART", "CHECKOUT_STARTED", "PURCHASE"]


def user_id(n: int) -> str:
    return f"user_{n}"


def product_id(n: int) -> str:
    return f"prod_{n}"


def product_price(product_id_str: str) -> float:
    """
    Deterministic price per product so the same product always costs the
    same amount, like a real catalog would. Seeding a local Random
    instance with the product_id keeps this independent of whatever the
    global `random` state is doing elsewhere in the generator.
    """
    rng = random.Random(product_id_str)
    return round(rng.uniform(5.0, 500.0), 2)


def build_fields(event_type: str, user_id_str: str, product_id_str: str) -> dict:
    """Return the type-specific fields for a given event type."""
    if event_type == "USER_SIGNUP":
        return {"user_id": user_id_str}

    if event_type == "PRODUCT_VIEW":
        return {"user_id": user_id_str, "product_id": product_id_str}

    if event_type == "ADD_TO_CART":
        return {
            "user_id": user_id_str,
            "product_id": product_id_str,
            "quantity": random.randint(1, 5),
        }

    if event_type == "CHECKOUT_STARTED":
        return {"user_id": user_id_str}

    if event_type == "PURCHASE":
        quantity = random.randint(1, 5)
        return {
            "user_id": user_id_str,
            "product_id": product_id_str,
            "quantity": quantity,
            "unit_price": product_price(product_id_str),
        }

    raise ValueError(f"Unknown event_type: {event_type}")
