"""Fixed-width binary message layouts shared by every process in the pipeline.

Both layouts are little-endian, packed, and read/written in place with
``struct.pack_into`` / ``struct.unpack_from`` directly on shared memory.
"""

import struct

# order_id, ts_ns, price (ticks), qty, side, order_type, pad  -> 32 bytes
ORDER_FMT = "<QQqIBBH"
# taker_id, maker_id, price (ticks), qty, ts_ns                -> 40 bytes
TRADE_FMT = "<QQqQQ"

ORDER_SIZE = struct.calcsize(ORDER_FMT)
TRADE_SIZE = struct.calcsize(TRADE_FMT)
