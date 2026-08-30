"""
Validation and normalization for incoming Kafka events.

design.md section 9 lists these as two separate responsibilities:

    Normalization - lightweight, best-effort fixes for messy-but-
    recoverable input, e.g. "purchase" -> "PURCHASE", "49.99" -> 49.99.

    Validation - checking the event contains sane, usable fields, e.g.
    event_id must exist, event_type must be a known type, timestamp must
    be parseable, quantity/price must be numeric.

We normalize first and validate second, on purpose: a lowercase
event_type or a stringified number isn't actually broken data, it's just
inconveniently formatted, and normalizing it before validating avoids
rejecting perfectly good events over formatting. Validation then only
flags events that are genuinely unusable even after normalization - the
ones design.md section 12 says should go to the DLQ once it exists
(Phase 9).
"""

from datetime import datetime

EVENT_TYPES = {"USER_SIGNUP", "PRODUCT_VIEW", "ADD_TO_CART", "CHECKOUT_STARTED", "PURCHASE"}

REQUIRED_FIELDS = {
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


def _try_int(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return value  # leave it as-is; validate_event() will reject it


def _try_float(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return value


def _is_valid_timestamp(value) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except ValueError:
        return False


def normalize_event(raw: dict) -> dict:
    event = dict(raw)

    if isinstance(event.get("event_type"), str):
        event["event_type"] = event["event_type"].strip().upper()

    if "quantity" in event:
        event["quantity"] = _try_int(event["quantity"])

    if "unit_price" in event:
        event["unit_price"] = _try_float(event["unit_price"])

    return event


def validate_event(event: dict) -> list:
    """Return a list of human-readable error strings; empty list = valid."""
    errors = []

    event_type = event.get("event_type")
    if event_type not in EVENT_TYPES:
        errors.append(f"unknown event_type: {event_type!r}")
        return errors  # required-field check below needs a known type

    missing = REQUIRED_FIELDS[event_type] - event.keys()
    if missing:
        errors.append(f"missing required fields: {sorted(missing)}")

    if "event_id" not in missing and not event.get("event_id"):
        errors.append("empty event_id")

    if "timestamp" not in missing and not _is_valid_timestamp(event.get("timestamp")):
        errors.append(f"invalid timestamp: {event.get('timestamp')!r}")

    if "quantity" in REQUIRED_FIELDS[event_type] and "quantity" in event:
        quantity = event["quantity"]
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
            errors.append(f"invalid quantity: {quantity!r}")

    if "unit_price" in REQUIRED_FIELDS[event_type] and "unit_price" in event:
        price = event["unit_price"]
        if not isinstance(price, (int, float)) or isinstance(price, bool) or price <= 0:
            errors.append(f"invalid unit_price: {price!r}")

    return errors
