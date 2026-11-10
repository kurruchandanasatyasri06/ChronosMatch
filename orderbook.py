"""Price-time priority limit order book.

Prices are integer ticks (avoid float rounding). Each side keeps:
  * a heap of price levels (best price on top), and
  * a dict price -> deque of resting orders in arrival (time) order.
Cancels are lazy: the resting entry's qty is zeroed and skipped when reached.
"""

import heapq
from collections import deque

BUY, SELL = 0, 1
LIMIT, MARKET, CANCEL, STOP = 0, 1, 2, 255


class OrderBook:
    def __init__(self):
        self.bid_heap = []      # stores -price  (max-heap via negation)
        self.ask_heap = []      # stores  price
        self.bid_levels = {}    # price -> deque([order_id, qty])
        self.ask_levels = {}
        self.index = {}         # order_id -> resting entry (for cancels)

    # ------------------------------------------------------------------ API
    def process(self, order_id, side, otype, price, qty):
        """Handle one order. Returns a list of (maker_id, price, qty) fills."""
        if otype == CANCEL:
            self.cancel(order_id)
            return []
        return self._match(order_id, side, price, qty, otype == MARKET)

    def cancel(self, order_id):
        entry = self.index.pop(order_id, None)
        if entry is None:
            return False
        entry[1] = 0
        return True

    def best_bid(self):
        return self._best(self.bid_heap, self.bid_levels, -1)

    def best_ask(self):
        return self._best(self.ask_heap, self.ask_levels, 1)

    def depth(self, side):
        """Total live resting quantity on a side (for tests/diagnostics)."""
        levels = self.bid_levels if side == BUY else self.ask_levels
        return sum(e[1] for lvl in levels.values() for e in lvl)

    # ------------------------------------------------------------- internals
    def _best(self, heap, levels, sign):
        while heap:
            price = heap[0] * sign
            lvl = levels[price]
            while lvl and lvl[0][1] == 0:
                lvl.popleft()
            if lvl:
                return price
            del levels[price]
            heapq.heappop(heap)
        return None

    def _match(self, order_id, side, price, qty, is_market):
        trades = []
        if side == BUY:
            opp_heap, opp_levels, sign = self.ask_heap, self.ask_levels, 1
        else:
            opp_heap, opp_levels, sign = self.bid_heap, self.bid_levels, -1

        while qty > 0:
            best = self._best(opp_heap, opp_levels, sign)
            if best is None:
                break
            if not is_market:
                if side == BUY and price < best:
                    break
                if side == SELL and price > best:
                    break
            lvl = opp_levels[best]
            while lvl and qty > 0:
                entry = lvl[0]
                if entry[1] == 0:
                    lvl.popleft()
                    continue
                fill = entry[1] if entry[1] < qty else qty
                entry[1] -= fill
                qty -= fill
                trades.append((entry[0], best, fill))
                if entry[1] == 0:
                    lvl.popleft()
                    self.index.pop(entry[0], None)

        if qty > 0 and not is_market:  # rest the remainder
            heap, levels, key = (
                (self.bid_heap, self.bid_levels, -price) if side == BUY
                else (self.ask_heap, self.ask_levels, price)
            )
            lvl = levels.get(price)
            if lvl is None:
                lvl = levels[price] = deque()
                heapq.heappush(heap, key)
            entry = [order_id, qty]
            lvl.append(entry)
            self.index[order_id] = entry
        return trades
