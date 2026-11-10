"""Benchmarks:
  1. Matching-only service time per order (in-process, no transport).
  2. Transport comparison: zero-copy shared-memory ring vs multiprocessing.Queue (pickle).
"""

import multiprocessing as mp
import time

from chronosmatch.generator import generate_orders
from chronosmatch.messages import ORDER_FMT
from chronosmatch.orderbook import OrderBook, CANCEL
from chronosmatch.ringbuffer import SharedRingBuffer

N = 200_000


def bench_matching():
    data = list(generate_orders(N, seed=7))
    book = OrderBook()
    lat = []
    t0 = time.perf_counter()
    for oid, side, otype, price, qty in data:
        a = time.perf_counter_ns()
        book.process(price if otype == CANCEL else oid, side, otype, price, qty)
        lat.append(time.perf_counter_ns() - a)
    dt = time.perf_counter() - t0
    lat.sort()
    p = lambda q: lat[int(len(lat) * q)] / 1000
    print(f"[matching] {N/dt:,.0f} orders/s  service time us: "
          f"p50={p(0.5):.1f} p99={p(0.99):.1f} p99.9={p(0.999):.1f}")


def _ring_consumer(name, cap, n, q):
    ring = SharedRingBuffer(ORDER_FMT, cap, name, create=False)
    got = 0
    while got < n:
        got += len(ring.pop_many(1024)) or (time.sleep(0) or 0)
    q.put(got)
    ring.close()


def _queue_consumer(inq, n, q):
    for _ in range(n):
        inq.get()
    q.put(n)


def bench_transport():
    ctx = mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
    done = ctx.Queue()

    ring = SharedRingBuffer(ORDER_FMT, 1 << 16)
    c = ctx.Process(target=_ring_consumer, args=(ring.name, 1 << 16, N, done))
    c.start()
    t0 = time.perf_counter()
    for i in range(N):
        while not ring.try_push(i, i, 10_000, 10, 0, 0, 0):
            time.sleep(0)
    done.get()
    dt_ring = time.perf_counter() - t0
    c.join()
    ring.close()

    inq = ctx.Queue(maxsize=1 << 16)
    c = ctx.Process(target=_queue_consumer, args=(inq, N, done))
    c.start()
    t0 = time.perf_counter()
    for i in range(N):
        inq.put((i, i, 10_000, 10, 0, 0, 0))
    done.get()
    dt_q = time.perf_counter() - t0
    c.join()

    print(f"[transport] shared-memory ring : {N/dt_ring:,.0f} msgs/s")
    print(f"[transport] multiprocessing.Queue: {N/dt_q:,.0f} msgs/s")
    print(f"[transport] speed-up: {dt_q/dt_ring:.1f}x")


if __name__ == "__main__":
    bench_matching()
    bench_transport()
