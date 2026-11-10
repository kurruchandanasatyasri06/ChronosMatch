"""SQLite trade log (batch inserts, WAL mode)."""

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    taker_id  INTEGER NOT NULL,
    maker_id  INTEGER NOT NULL,
    price     INTEGER NOT NULL,
    qty       INTEGER NOT NULL,
    ts_ns     INTEGER NOT NULL
);
"""


def open_db(path):
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute(SCHEMA)
    return con


def insert_trades(con, rows):
    con.executemany(
        "INSERT INTO trades (taker_id, maker_id, price, qty, ts_ns) VALUES (?,?,?,?,?)",
        rows,
    )
    con.commit()


def summary(path):
    con = sqlite3.connect(path)
    n, qty, notional = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(qty),0), COALESCE(SUM(qty*price),0) FROM trades"
    ).fetchone()
    con.close()
    return {"trades": n, "volume": qty, "notional": notional}
