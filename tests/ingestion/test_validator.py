#!/usr/bin/env python3
"""
Standalone test for normalize_event()/validate_event() - no Kafka needed.

Feeds a handful of hand-written cases (clean, messy-but-fixable, and
genuinely broken) through the real ingestion/app/validator.py code and
prints what it decided, so you can confirm normalization and validation
behave the way design.md section 9 describes before ever touching Kafka.

Usage:
    python tests/ingestion/test_validator.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ingestion"))

from app.validator import normalize_event, validate_event  # noqa: E402

CASES = [
    (
        "clean PURCHASE event",
        {
            "event_id": "evt_1",
            "event_type": "PURCHASE",
            "timestamp": "2026-08-30T10:00:00Z",
            "user_id": "user_1",
            "product_id": "prod_1",
            "quantity": 2,
            "unit_price": 19.99,
        },
    ),
    (
        "messy but recoverable (lowercase type, stringified numbers)",
        {
            "event_id": "evt_2",
            "event_type": "purchase",
            "timestamp": "2026-08-30T10:00:00Z",
            "user_id": "user_2",
            "product_id": "prod_2",
            "quantity": "3",
            "unit_price": "49.99",
        },
    ),
    (
        "missing event_id",
        {
            "event_type": "PRODUCT_VIEW",
            "timestamp": "2026-08-30T10:00:00Z",
            "user_id": "user_3",
            "product_id": "prod_3",
        },
    ),
    (
        "non-numeric quantity",
        {
            "event_id": "evt_4",
            "event_type": "ADD_TO_CART",
            "timestamp": "2026-08-30T10:00:00Z",
            "user_id": "user_4",
            "product_id": "prod_4",
            "quantity": "INVALID",
        },
    ),
    (
        "bad timestamp",
        {
            "event_id": "evt_5",
            "event_type": "USER_SIGNUP",
            "timestamp": "not-a-timestamp",
            "user_id": "user_5",
        },
    ),
    (
        "unknown event_type",
        {
            "event_id": "evt_6",
            "event_type": "UNKNOWN_EVENT",
            "timestamp": "2026-08-30T10:00:00Z",
            "user_id": "user_6",
        },
    ),
]


def main():
    passed = 0
    for name, raw in CASES:
        normalized = normalize_event(raw)
        errors = validate_event(normalized)

        print(f"--- {name} ---")
        print(f"raw:        {json.dumps(raw)}")
        print(f"normalized: {json.dumps(normalized)}")
        if errors:
            print(f"result:     INVALID -> {errors}")
        else:
            print("result:     VALID")
            passed += 1
        print()

    print(f"{passed}/{len(CASES)} cases ended up VALID (expect 2: the clean and messy-but-fixable ones)")


if __name__ == "__main__":
    main()
