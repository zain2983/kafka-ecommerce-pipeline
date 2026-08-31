#!/usr/bin/env python3
"""
Standalone test for DedupCache (ingestion/app/dedup_cache.py) - no
Kafka, no Postgres. Confirms:
  - a fresh event_id is not reported as seen
  - the same event_id IS reported as seen once add()-ed
  - LRU eviction: once max_size is exceeded, the least-recently-seen
    entry is forgotten first
  - TTL eviction: an entry older than ttl_seconds is forgotten even if
    the cache isn't full

Usage:
    python tests/ingestion/test_dedup_cache.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ingestion"))

from app.dedup_cache import DedupCache  # noqa: E402


def expect(condition: bool, description: str, failures: list):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {description}")
    if not condition:
        failures.append(description)


def main():
    failures = []

    # --- basic add/contains ---
    cache = DedupCache(max_size=10, ttl_seconds=60)
    expect(cache.contains("evt_1") is False, "unseen event_id is not reported as seen", failures)
    cache.add("evt_1")
    expect(cache.contains("evt_1") is True, "event_id IS reported as seen after add()", failures)
    expect(len(cache) == 1, "cache size is 1 after one add()", failures)

    # --- LRU eviction ---
    lru_cache = DedupCache(max_size=3, ttl_seconds=60)
    for i in range(3):
        lru_cache.add(f"evt_{i}")
    expect(len(lru_cache) == 3, "cache holds exactly max_size entries", failures)
    # Touch evt_0 so it becomes most-recently-used, then evt_1 should be
    # the next one evicted instead of evt_0.
    lru_cache.add("evt_0")
    lru_cache.add("evt_3")  # pushes the cache over max_size=3
    expect(
        lru_cache.contains("evt_1") is False,
        "least-recently-used entry (evt_1) was evicted once over max_size",
        failures,
    )
    expect(
        lru_cache.contains("evt_0") is True,
        "recently re-added entry (evt_0) survived eviction",
        failures,
    )
    expect(lru_cache.contains("evt_3") is True, "newest entry (evt_3) is present", failures)
    expect(len(lru_cache) == 3, "cache still holds exactly max_size entries after eviction", failures)

    # --- TTL eviction ---
    ttl_cache = DedupCache(max_size=100, ttl_seconds=0.2)
    ttl_cache.add("evt_ttl")
    expect(ttl_cache.contains("evt_ttl") is True, "entry is present immediately after add()", failures)
    time.sleep(0.3)
    expect(
        ttl_cache.contains("evt_ttl") is False,
        "entry is forgotten after ttl_seconds elapses, even though the cache isn't full",
        failures,
    )

    print()
    if failures:
        print(f"{len(failures)} test(s) FAILED")
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
