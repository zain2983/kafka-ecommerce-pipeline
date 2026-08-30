#!/usr/bin/env python3
"""
Standalone test for the event generator - no Kafka, no Docker required.

Generates N events directly from producer/app/event_generator.py and
prints:
  - each event as JSON (unless --quiet)
  - a summary: how many events of each type, how many look invalid
    (malformed on purpose), and how many are exact duplicates

This exercises exactly the same code the producer uses to build events -
it just never hands them to Kafka.

Usage:
    python tests/producer/test_event_generator.py --count 50
    python tests/producer/test_event_generator.py --count 2000 --quiet
    python tests/producer/test_event_generator.py --count 50 --seed 42   # reproducible
"""

import argparse
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "producer"))

from app.config import GeneratorConfig  # noqa: E402
from app.event_generator import EventGenerator  # noqa: E402

EXPECTED_FIELDS = {
    "USER_SIGNUP": {"event_id", "event_type", "timestamp", "user_id"},
    "PRODUCT_VIEW": {"event_id", "event_type", "timestamp", "user_id", "product_id"},
    "ADD_TO_CART": {"event_id", "event_type", "timestamp", "user_id", "product_id", "quantity"},
    "CHECKOUT_STARTED": {"event_id", "event_type", "timestamp", "user_id"},
    "PURCHASE": {
        "event_id",
        "event_type",
        "timestamp",
        "user_id",
        "product_id",
        "quantity",
        "unit_price",
    },
}


def parse_args():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--count", type=int, default=20, help="How many events to generate")
    p.add_argument(
        "--seed", type=int, default=None, help="Override RANDOM_SEED for reproducible output"
    )
    p.add_argument(
        "--quiet", action="store_true", help="Only print the summary, not every event"
    )
    return p.parse_args()


def looks_invalid(event: dict) -> bool:
    """Mirrors the corruption strategies in event_generator.py's _corrupt()."""
    event_type = event.get("event_type")
    if event_type not in EXPECTED_FIELDS:
        return True
    if set(event.keys()) != EXPECTED_FIELDS[event_type]:
        return True
    if "quantity" in event and not isinstance(event["quantity"], int):
        return True
    if "unit_price" in event and not isinstance(event["unit_price"], (int, float)):
        return True
    ts = event.get("timestamp", "")
    if not (len(ts) == 20 and ts.endswith("Z")):
        return True
    return False


def main():
    args = parse_args()
    if args.seed is not None:
        os.environ["RANDOM_SEED"] = str(args.seed)

    config = GeneratorConfig()
    generator = EventGenerator(config)

    print(
        f"Config: events/sec={config.events_per_second} users={config.num_users} "
        f"products={config.num_products} invalid_prob={config.invalid_event_probability} "
        f"duplicate_prob={config.duplicate_event_probability}\n"
    )

    type_counts = Counter()
    seen_ids = Counter()
    invalid_count = 0

    for _ in range(args.count):
        event = generator.next_event()
        if not args.quiet:
            print(json.dumps(event))

        type_counts[event.get("event_type", "?")] += 1
        seen_ids[event.get("event_id")] += 1
        if looks_invalid(event):
            invalid_count += 1

    duplicate_count = sum(c - 1 for c in seen_ids.values() if c > 1)

    print("\n--- Summary ---")
    print(f"Total events generated: {args.count}")
    for event_type, count in type_counts.most_common():
        print(f"  {event_type:<18} {count:>5}  ({count / args.count:.1%})")
    print(f"Invalid-looking events: {invalid_count:>5}  ({invalid_count / args.count:.1%})")
    print(f"Duplicate event_ids:    {duplicate_count:>5}  ({duplicate_count / args.count:.1%})")


if __name__ == "__main__":
    main()
