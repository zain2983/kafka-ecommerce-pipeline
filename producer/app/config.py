"""
Configuration for the event generator.

Everything is read from environment variables so the same code works
unchanged whether you run it directly on your laptop (Phase 2) or later
inside a Docker container (Phase 13) - only the env vars change.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass
class GeneratorConfig:
    # How many events to emit per second (on average).
    events_per_second: float = field(default_factory=lambda: _float("EVENTS_PER_SECOND", 2.0))

    # Size of the simulated user/product pools. A smaller pool means more
    # repeat activity per user/product, which is more realistic and also
    # gives dbt aggregations something interesting to group by later.
    num_users: int = field(default_factory=lambda: _int("NUM_USERS", 500))
    num_products: int = field(default_factory=lambda: _int("NUM_PRODUCTS", 200))

    # Probability (0.0-1.0) that a given generated event is deliberately
    # malformed. This lets us exercise the consumer's validation + DLQ
    # path in later phases without writing a separate test harness.
    invalid_event_probability: float = field(
        default_factory=lambda: _float("INVALID_EVENT_PROBABILITY", 0.02)
    )

    # Probability that a generated event is an exact duplicate (same
    # event_id) of the previously emitted event. This simulates the
    # at-least-once redelivery we'll need idempotent ingestion for.
    duplicate_event_probability: float = field(
        default_factory=lambda: _float("DUPLICATE_EVENT_PROBABILITY", 0.03)
    )

    # Relative weights for each event type. Do not need to sum to 1 -
    # they are normalized at use time.
    event_type_weights: dict = field(
        default_factory=lambda: {
            "PRODUCT_VIEW": 0.45,
            "ADD_TO_CART": 0.20,
            "CHECKOUT_STARTED": 0.10,
            "PURCHASE": 0.20,
            "USER_SIGNUP": 0.05,
        }
    )

    random_seed: Optional[int] = field(
        default_factory=lambda: (_int("RANDOM_SEED", 0) or None)
    )
