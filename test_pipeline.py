import os
import tempfile
import unittest

from chronosmatch.generator import generate_orders
from chronosmatch.messages import ORDER_FMT
from chronosmatch.orderbook import OrderBook, CANCEL
from chronosmatch.pipeline import run_pipeline
from chronosmatch.ringbuffer import SharedRingBuffer
from chronosmatch.store import open_db, insert_trades, summary


class RingBufferTests(unittest.TestCase):
    def setUp(self):
        self.ring = SharedRingBuffer(ORDER_FMT, capacity=8)

    def tearDown(self):
        self.ring.close()

    def test_push_pop_roundtrip(self):
        self.assertTrue(self.ring.try_push(1, 2, 3, 4, 0, 0, 0))
        self.assertEqual(self.ring.try_pop(), (1, 2, 3, 4, 0, 0, 0))
        self.assertIsNone(self.ring.try_pop())

    def test_full_then_drain(self):
        for i in range(8):
            self.assertTrue(self.ring.try_push(i, 0, 0, 0, 0, 0, 0))
        self.assertFalse(self.ring.try_push(99, 0, 0, 0, 0, 0, 0))
        self.assertEqual([r[0] for r in self.ring.pop_many(100)], list(range(8)))

    def test_wraparound_keeps_order(self):
        seen = []
        for i in range(100):
            self.assertTrue(self.ring.try_push(i, 0, 0, 0, 0, 0, 0))
            if i % 3 == 2:
                seen += [r[0] for r in self.ring.pop_many(10)]
        seen += [r[0] for r in self.ring.pop_many(10)]
        self.assertEqual(seen, list(range(100)))

    def test_rejects_non_power_of_two(self):
        with self.assertRaises(ValueError):
            SharedRingBuffer(ORDER_FMT, capacity=6)

    def test_second_handle_sees_same_memory(self):
        other = SharedRingBuffer(ORDER_FMT, 8, self.ring.name, create=False)
        self.ring.try_push(7, 0, 0, 0, 0, 0, 0)
        self.assertEqual(other.try_pop()[0], 7)
        other.close()


class PipelineTests(unittest.TestCase):
    def test_store_summary(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "t.db")
            con = open_db(path)
            insert_trades(con, [(2, 1, 100, 5, 0), (3, 1, 101, 2, 0)])
            con.close()
            self.assertEqual(summary(path), {"trades": 2, "volume": 7, "notional": 702})

    def test_pipeline_matches_single_process_reference(self):
        n, seed = 20_000, 123
        book, ref_trades, ref_vol = OrderBook(), 0, 0
        for oid, side, otype, price, qty in generate_orders(n, seed):
            fills = book.process(price if otype == CANCEL else oid, side, otype, price, qty)
            ref_trades += len(fills)
            ref_vol += sum(f[2] for f in fills)
        with tempfile.TemporaryDirectory() as d:
            stats = run_pipeline(n, seed, os.path.join(d, "t.db"))
        self.assertEqual(stats["orders"], n)
        self.assertEqual(stats["db"]["trades"], ref_trades)
        self.assertEqual(stats["db"]["volume"], ref_vol)


if __name__ == "__main__":
    unittest.main()
