"""
Bounded, TTL-based cache of recently-seen event_ids.

design.md section 26 lists "Deduplication" and "Idempotency" as two
separate concepts, and this is what makes them separate mechanisms
rather than two names for the same thing:

    Idempotency (design.md section 11) - the durable guarantee. Postgres
    enforces event_id uniqueness (database.py's ON CONFLICT DO NOTHING),
    so no duplicate can EVER become a second row, no matter what. This
    is the correctness backstop and cannot be bypassed.

    Deduplication (this file) - a fast PATH, not a guarantee. If an
    event_id was handled recently enough to still be in this in-memory
    cache, main.py skips the Postgres round-trip entirely instead of
    sending a write it already knows will be a no-op. That's strictly
    an optimization: the cache is empty on every process restart, and
    it deliberately forgets entries once it's full (LRU eviction) or
    they've aged out (TTL), so it can absolutely miss a real duplicate -
    when it does, the DB constraint above still catches it. Nothing
    about correctness depends on this cache ever being right.
"""

import time
from collections import OrderedDict


class DedupCache:
    def __init__(self, max_size: int = 10_000, ttl_seconds: float = 300.0):
        self._max_size = max_size
        self._ttl_seconds = ttl_seconds
        # Insertion-ordered: add() always re-inserts at the end, so the
        # front of the dict is always the least-recently-seen entry -
        # that's what makes both eviction strategies below cheap.
        self._seen = OrderedDict()

    def contains(self, event_id: str) -> bool:
        self._evict_expired()
        return event_id in self._seen

    def add(self, event_id: str):
        self._seen[event_id] = time.monotonic()
        self._seen.move_to_end(event_id)
        while len(self._seen) > self._max_size:
            self._seen.popitem(last=False)

    def _evict_expired(self):
        cutoff = time.monotonic() - self._ttl_seconds
        while self._seen:
            oldest_id, seen_at = next(iter(self._seen.items()))
            if seen_at >= cutoff:
                break
            del self._seen[oldest_id]

    def __len__(self):
        return len(self._seen)
