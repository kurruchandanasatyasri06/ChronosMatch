"""Run the full ChronosMatch pipeline and print a performance summary.

Usage:  python run_demo.py [n_orders] [seed]
"""

import sys

from chronosmatch.pipeline import run_pipeline

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 200_000
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 42
    s = run_pipeline(n, seed, "trades.db")
    print(f"orders processed : {s['orders']:,}")
    print(f"trades executed  : {s['trades']:,}")
    print(f"engine throughput: {s['orders_per_s']:,.0f} orders/s")
    print(f"end-to-end latency (us, includes ring queueing): "
          f"p50={s['p50_us']:.0f} p99={s['p99_us']:.0f} p99.9={s['p999_us']:.0f} max={s['max_us']:.0f}")
    print(f"trade log (SQLite): {s['db']}")
