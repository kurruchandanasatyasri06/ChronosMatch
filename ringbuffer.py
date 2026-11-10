"""Single-producer / single-consumer ring buffer living in shared memory.

Zero-copy design:
  * Records are fixed-width and packed straight into the shared segment with
    ``Struct.pack_into`` (no intermediate bytes object, no pickling).
  * The consumer decodes them in place with ``Struct.unpack_from``.
  * ``head`` (written only by the producer) and ``tail`` (written only by the
    consumer) live on separate 64-byte cache lines to avoid false sharing.

Caveat: this is lock-free SPSC. It relies on each index having exactly one
writer and on 8-byte aligned stores being atomic (true on x86-64 / ARM64).
Never use it with more than one producer or one consumer.
"""

import struct
from multiprocessing import shared_memory

_HDR = 128
_HEAD_OFF = 0
_TAIL_OFF = 64
_U64 = struct.Struct("<Q")


class SharedRingBuffer:
    def __init__(self, record_fmt, capacity=1 << 16, name=None, create=True):
        if capacity & (capacity - 1):
            raise ValueError("capacity must be a power of two")
        self.rec = struct.Struct(record_fmt)
        self.capacity = capacity
        self.mask = capacity - 1
        self._owner = create
        size = _HDR + capacity * self.rec.size
        if create:
            self.shm = shared_memory.SharedMemory(create=True, size=size)
            self.shm.buf[:_HDR] = bytes(_HDR)
        else:
            self.shm = shared_memory.SharedMemory(name=name)
        self.buf = self.shm.buf

    @property
    def name(self):
        return self.shm.name

    def __len__(self):
        return _U64.unpack_from(self.buf, _HEAD_OFF)[0] - _U64.unpack_from(self.buf, _TAIL_OFF)[0]

    def try_push(self, *fields):
        """Producer side. Returns False if the ring is full."""
        head = _U64.unpack_from(self.buf, _HEAD_OFF)[0]
        tail = _U64.unpack_from(self.buf, _TAIL_OFF)[0]
        if head - tail >= self.capacity:
            return False
        self.rec.pack_into(self.buf, _HDR + (head & self.mask) * self.rec.size, *fields)
        _U64.pack_into(self.buf, _HEAD_OFF, head + 1)  # publish after the write
        return True

    def try_pop(self):
        """Consumer side. Returns a tuple, or None if empty."""
        tail = _U64.unpack_from(self.buf, _TAIL_OFF)[0]
        head = _U64.unpack_from(self.buf, _HEAD_OFF)[0]
        if tail == head:
            return None
        rec = self.rec.unpack_from(self.buf, _HDR + (tail & self.mask) * self.rec.size)
        _U64.pack_into(self.buf, _TAIL_OFF, tail + 1)
        return rec

    def pop_many(self, max_n=256):
        """Consumer side batch read; commits the tail once per batch."""
        tail = _U64.unpack_from(self.buf, _TAIL_OFF)[0]
        head = _U64.unpack_from(self.buf, _HEAD_OFF)[0]
        n = min(head - tail, max_n)
        if n <= 0:
            return []
        unpack = self.rec.unpack_from
        size, mask = self.rec.size, self.mask
        out = [unpack(self.buf, _HDR + ((tail + i) & mask) * size) for i in range(n)]
        _U64.pack_into(self.buf, _TAIL_OFF, tail + n)
        return out

    def close(self):
        self.buf = None
        self.shm.close()
        if self._owner:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass
