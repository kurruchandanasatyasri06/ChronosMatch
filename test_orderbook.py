import unittest

from chronosmatch.orderbook import OrderBook, BUY, SELL, LIMIT, MARKET, CANCEL


class OrderBookTests(unittest.TestCase):
    def test_non_crossing_orders_rest(self):
        b = OrderBook()
        self.assertEqual(b.process(1, BUY, LIMIT, 100, 10), [])
        self.assertEqual(b.process(2, SELL, LIMIT, 105, 10), [])
        self.assertEqual((b.best_bid(), b.best_ask()), (100, 105))

    def test_full_cross_trades_at_maker_price(self):
        b = OrderBook()
        b.process(1, SELL, LIMIT, 100, 10)
        fills = b.process(2, BUY, LIMIT, 105, 10)
        self.assertEqual(fills, [(1, 100, 10)])
        self.assertIsNone(b.best_ask())
        self.assertIsNone(b.best_bid())

    def test_partial_fill_rests_remainder(self):
        b = OrderBook()
        b.process(1, SELL, LIMIT, 100, 4)
        fills = b.process(2, BUY, LIMIT, 100, 10)
        self.assertEqual(fills, [(1, 100, 4)])
        self.assertEqual(b.best_bid(), 100)
        self.assertEqual(b.depth(BUY), 6)

    def test_price_priority(self):
        b = OrderBook()
        b.process(1, SELL, LIMIT, 102, 5)
        b.process(2, SELL, LIMIT, 101, 5)
        fills = b.process(3, BUY, LIMIT, 102, 5)
        self.assertEqual(fills, [(2, 101, 5)])

    def test_time_priority_within_level(self):
        b = OrderBook()
        b.process(1, SELL, LIMIT, 100, 5)
        b.process(2, SELL, LIMIT, 100, 5)
        fills = b.process(3, BUY, LIMIT, 100, 7)
        self.assertEqual(fills, [(1, 100, 5), (2, 100, 2)])

    def test_cancel(self):
        b = OrderBook()
        b.process(1, SELL, LIMIT, 100, 5)
        self.assertTrue(b.cancel(1))
        self.assertFalse(b.cancel(1))
        self.assertEqual(b.process(2, BUY, LIMIT, 100, 5), [])
        self.assertIsNone(b.best_ask())

    def test_cancel_via_process(self):
        b = OrderBook()
        b.process(1, BUY, LIMIT, 100, 5)
        b.process(1, BUY, CANCEL, 0, 0)
        self.assertIsNone(b.best_bid())

    def test_market_order_sweeps_and_never_rests(self):
        b = OrderBook()
        b.process(1, SELL, LIMIT, 100, 3)
        b.process(2, SELL, LIMIT, 101, 3)
        fills = b.process(3, BUY, MARKET, 0, 10)
        self.assertEqual(fills, [(1, 100, 3), (2, 101, 3)])
        self.assertIsNone(b.best_bid())
        self.assertIsNone(b.best_ask())

    def test_quantity_conserved(self):
        b = OrderBook()
        for i in range(1, 50):
            b.process(i, SELL, LIMIT, 100 + i % 5, 7)
        fills = b.process(999, BUY, MARKET, 0, 100)
        self.assertEqual(sum(f[2] for f in fills), 100)
        self.assertEqual(b.depth(SELL), 49 * 7 - 100)


if __name__ == "__main__":
    unittest.main()
