"""
Generates a continuous stream of synthetic e-commerce events.

This module knows nothing about Kafka - it just produces Python dicts.
Phase 3 will add a kafka_producer.py that takes these dicts and publishes
them to the ecommerce-events topic.
"""

import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from app.config import GeneratorConfig
from app.schemas import EVENT_TYPES, build_fields, product_id, user_id


def _new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class EventGenerator:
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self._rng = random.Random(config.random_seed)
        random.seed(config.random_seed)  # also seed the module-level RNG used in schemas.py

        types, weights = zip(*config.event_type_weights.items())
        self._event_types = list(types)
        self._weights = list(weights)

        self._last_event: Optional[dict] = None

    def _pick_event_type(self) -> str:
        return self._rng.choices(self._event_types, weights=self._weights, k=1)[0]

    def _build_valid_event(self) -> dict:
        event_type = self._pick_event_type()
        u = user_id(self._rng.randint(1, self.config.num_users))
        p = product_id(self._rng.randint(1, self.config.num_products))

        event = {
            "event_id": _new_event_id(),
            "event_type": event_type,
            "timestamp": _now_iso(),
            **build_fields(event_type, u, p),
        }
        return event

    def _corrupt(self, event: dict) -> dict:
        """
        Take an otherwise-valid event and break it in one of several ways
        that a normalization step (Phase 4) cannot silently fix - e.g. a
        missing required field, a non-numeric quantity, or a bad
        timestamp. These are the events that should end up in the DLQ
        (design.md section 12) once the consumer exists.
        """
        corrupted = dict(event)
        strategy = self._rng.choice(
            ["drop_field", "bad_quantity", "bad_price", "bad_timestamp", "unknown_type"]
        )

        if strategy == "drop_field" and len(corrupted) > 2:
            field_to_drop = self._rng.choice(
                [k for k in corrupted if k not in ("event_id", "event_type")]
            )
            del corrupted[field_to_drop]
        elif strategy == "bad_quantity" and "quantity" in corrupted:
            corrupted["quantity"] = "INVALID"
        elif strategy == "bad_price" and "unit_price" in corrupted:
            corrupted["unit_price"] = "N/A"
        elif strategy == "bad_timestamp":
            corrupted["timestamp"] = "not-a-timestamp"
        else:
            corrupted["event_type"] = "UNKNOWN_EVENT"

        return corrupted

    def next_event(self) -> dict:
        """Return the next event dict, applying duplicate/invalid injection."""
        if self._last_event is not None and self._rng.random() < self.config.duplicate_event_probability:
            return dict(self._last_event)

        event = self._build_valid_event()

        if self._rng.random() < self.config.invalid_event_probability:
            event = self._corrupt(event)

        self._last_event = event
        return event

    def run(self):
        """Yield events forever, paced at ~events_per_second."""
        delay = 1.0 / self.config.events_per_second if self.config.events_per_second > 0 else 0
        while True:
            yield self.next_event()
            if delay:
                time.sleep(delay)
