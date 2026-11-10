# ChronosMatch: Zero-Copy High-Frequency Trading Engine

A pure-Python limit-order matching engine whose stages communicate through
shared-memory ring buffers using fixed-width binary records, so no message is
ever pickled or copied between processes.

```
producer ──► [order ring] ──► matching engine ──► [trade ring] ──► SQLite logger
 (main)        shared memory     (process)          shared memory     (process)
```

## Project layout

| File | Purpose |
|---|---|
| `chronosmatch/messages.py` | Binary layouts (`struct` formats) for orders and trades |
| `chronosmatch/ringbuffer.py` | Lock-free SPSC ring buffer on `multiprocessing.shared_memory` |
| `chronosmatch/orderbook.py` | Price-time priority order book (limit, market, cancel) |
| `chronosmatch/generator.py` | Deterministic synthetic order flow |
| `chronosmatch/pipeline.py` | Producer / engine / logger processes and orchestration |
| `chronosmatch/store.py` | SQLite trade log (batch inserts, WAL) |
| `run_demo.py` | Runs the full pipeline and prints metrics |
| `benchmark.py` | Matching service time; ring vs `multiprocessing.Queue` |
| `tests/` | 16 unit and end-to-end tests |

## Step-by-step procedure

1. **Set up** Python 3.9+ (no third-party packages needed):
   `python -m venv .venv && source .venv/bin/activate`
2. **Define the messages** (`messages.py`). Fixed-width little-endian structs:
   order = 32 bytes, trade = 40 bytes. Prices are integer ticks, never floats.
3. **Build the ring buffer** (`ringbuffer.py`). One shared-memory segment holds a
   128-byte header (`head` and `tail` on separate cache lines) followed by
   `capacity` records. The producer uses `pack_into` and the consumer uses
   `unpack_from` directly on the shared buffer. That is the zero-copy path.
4. **Build the order book** (`orderbook.py`). A heap of price levels plus a
   `deque` per level gives price priority, then time priority. Cancels are lazy
   (quantity zeroed, skipped when reached). Trades execute at the resting
   (maker) price.
5. **Generate order flow** (`generator.py`). A seeded random walk produces limit,
   market and cancel orders. The same seed always gives the same orders.
6. **Wire the pipeline** (`pipeline.py`). The engine process pops batches from the
   order ring, matches, and pushes fills to the trade ring. The logger process
   drains the trade ring into SQLite. A `STOP` order and a `qty == 0` trade act
   as shutdown sentinels.
7. **Test**: `python -m unittest discover -s tests -t . -v`
   The key test runs the 3-process pipeline and a single-process reference on the
   same seed and asserts identical trade count and volume.
8. **Run**: `python run_demo.py 200000`
9. **Benchmark**: `python benchmark.py`
10. **Document and present**: use the results below for the report and PPT.

## Sample results (200,000 orders, one sandbox machine)

| Metric | Result |
|---|---|
| Matching-only throughput | ~698k orders/s (p50 0.8 µs, p99 4.3 µs) |
| Full pipeline throughput | ~239k orders/s, 148,983 trades logged |
| Shared-memory ring transport | ~1.07M msgs/s |
| `multiprocessing.Queue` transport | ~94k msgs/s (ring is ~11x faster) |

Your numbers will differ by machine.

**Reading the latency figure:** `run_demo.py` pre-generates all orders and pushes
them as fast as possible, so the ring stays full and the reported end-to-end
latency is dominated by queueing (hundreds of ms). Use the *matching service
time* from `benchmark.py` for per-order latency, and the pipeline number for
throughput.

## Design notes and limitations

- The ring is **single-producer / single-consumer only**. It is lock-free because
  each index has exactly one writer. Do not add a second producer or consumer.
- Python cannot guarantee true zero-copy end to end: `unpack_from` builds a small
  tuple per record. The transport itself (shared memory, no pickling, no
  serialization buffers) is zero-copy, which is where the 11x gain comes from.
- Pure Python is not microsecond-deterministic (GIL, GC, scheduling). For a
  production HFT system the same design would move to C/C++/Rust. Python keeps
  the architecture and logic easy to study.
- Ideas for extension: FastAPI endpoint to submit orders, order-book depth
  snapshots, Numba/Cython for the matching loop, `gc.disable()` in the engine.
