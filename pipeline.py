"""Three-stage pipeline: producer -> [order ring] -> engine -> [trade ring] -> logger.

Each stage is a separate OS process (no GIL sharing); the stages exchange
fixed-width binary records through shared memory rings, never through pickling.
"""

import multiprocessing as mp
import os
import time

from .generator import generate_orders
from .messages import ORDER_FMT, TRADE_FMT
from .orderbook import OrderBook, STOP
from .ringbuffer import SharedRingBuffer
from .store import open_db, insert_trades, summary


def _percentiles(samples):
    if not samples:
        return {}
    s = sorted(samples)
    pick = lambda p: s[min(len(s) - 1, int(len(s) * p))] / 1000.0  # ns -> us
    return {"p50_us": pick(0.50), "p99_us": pick(0.99), "p999_us": pick(0.999), "max_us": s[-1] / 1000.0}


def engine_worker(order_name, order_cap, trade_name, trade_cap, result_q):
    orders = SharedRingBuffer(ORDER_FMT, order_cap, order_name, create=False)
    trades = SharedRingBuffer(TRADE_FMT, trade_cap, trade_name, create=False)
    book = OrderBook()
    latencies = []
    n_orders = n_trades = 0
    t_first = None
    running = True
    while running:
        batch = orders.pop_many(512)
        if not batch:
            time.sleep(0)
            continue
        if t_first is None:
            t_first = time.perf_counter_ns()
        for oid, ts, price, qty, side, otype, _pad in batch:
            if otype == STOP:
                running = False
                break
            # cancels carry the target order id in the price field
            fills = book.process(price if otype == 2 else oid, side, otype, price, qty)
            n_orders += 1
            now = time.perf_counter_ns()
            for maker, fprice, fqty in fills:
                while not trades.try_push(oid, maker, fprice, fqty, now):
                    time.sleep(0)
                n_trades += 1
            latencies.append(now - ts)
    t_end = time.perf_counter_ns()
    while not trades.try_push(0, 0, 0, 0, 0):  # sentinel: qty == 0
        time.sleep(0)
    elapsed = (t_end - t_first) / 1e9 if t_first else 0.0
    result_q.put({
        "orders": n_orders, "trades": n_trades, "elapsed_s": elapsed,
        "orders_per_s": n_orders / elapsed if elapsed else 0.0,
        **_percentiles(latencies),
    })
    orders.close()
    trades.close()


def logger_worker(trade_name, trade_cap, db_path):
    trades = SharedRingBuffer(TRADE_FMT, trade_cap, trade_name, create=False)
    con = open_db(db_path)
    done = False
    while not done:
        batch = trades.pop_many(2048)
        if not batch:
            time.sleep(0.001)
            continue
        rows = []
        for taker, maker, price, qty, ts in batch:
            if qty == 0:
                done = True
                break
            rows.append((taker, maker, price, qty, ts))
        if rows:
            insert_trades(con, rows)
    con.close()
    trades.close()


def run_pipeline(n_orders=200_000, seed=42, db_path="trades.db",
                 order_cap=1 << 16, trade_cap=1 << 16):
    if os.path.exists(db_path):
        os.remove(db_path)
    orders = SharedRingBuffer(ORDER_FMT, order_cap)
    trades = SharedRingBuffer(TRADE_FMT, trade_cap)
    ctx = mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
    result_q = ctx.Queue()
    eng = ctx.Process(target=engine_worker,
                      args=(orders.name, order_cap, trades.name, trade_cap, result_q))
    log = ctx.Process(target=logger_worker, args=(trades.name, trade_cap, db_path))
    eng.start()
    log.start()
    try:
        data = list(generate_orders(n_orders, seed))   # generate outside the timed path
        t0 = time.perf_counter()
        for oid, side, otype, price, qty in data:
            while not orders.try_push(oid, time.perf_counter_ns(), price, qty, side, otype, 0):
                time.sleep(0)
        while not orders.try_push(0, 0, 0, 0, 0, STOP, 0):
            time.sleep(0)
        stats = result_q.get()
        eng.join()
        log.join()
        stats["wall_s"] = time.perf_counter() - t0
        stats["db"] = summary(db_path)
        return stats
    finally:
        orders.close()
        trades.close()
