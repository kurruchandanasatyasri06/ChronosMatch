"""Deterministic synthetic order-flow generator (same seed -> same orders)."""

import random

from .orderbook import BUY, SELL, LIMIT, MARKET, CANCEL


def generate_orders(n, seed=42, mid=10_000, spread=50):
    """Yield (order_id, side, otype, price, qty) tuples.

    ~85% limit orders around a random-walk mid price, ~5% market orders and
    ~10% cancels of previously issued limit orders. Cancels have no price of
    their own, so the id being cancelled is carried in the price field (this
    keeps every message the same fixed width).
    """
    rng = random.Random(seed)
    issued = []
    for oid in range(1, n + 1):
        r = rng.random()
        if r < 0.10 and issued:
            target = issued[rng.randrange(len(issued))]
            yield oid, BUY, CANCEL, target, 0
            continue
        if rng.random() < 0.02:
            mid += rng.choice((-1, 1))
        side = BUY if rng.random() < 0.5 else SELL
        qty = rng.randint(1, 100)
        if r < 0.15:
            yield oid, side, MARKET, 0, qty
        else:
            issued.append(oid)
            yield oid, side, LIMIT, mid + rng.randint(-spread, spread), qty
