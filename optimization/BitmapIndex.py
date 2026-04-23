import gzip
import base64
from typing import List


class BitmapIndex:
    """Lightweight bitmap index using Python int for bit operations.

    Bits are stored so that bit i (LSB=0) corresponds to row index i.
    """

    def __init__(self, bits: int, length: int):
        self.bits = int(bits)
        self.length = int(length)

    @staticmethod
    def from_values(values: List, target) -> "BitmapIndex":
        bits = 0
        for i, v in enumerate(values):
            if v == target:
                bits |= (1 << i)
        return BitmapIndex(bits, len(values))

    def get_positions(self) -> List[int]:
        # Efficiently iterate over set bits only (skip zeros)
        positions: List[int] = []
        bits = self.bits
        while bits:
            lsb = bits & -bits
            idx = lsb.bit_length() - 1
            positions.append(idx)
            bits &= bits - 1
        return positions

    def and_(self, other: "BitmapIndex") -> "BitmapIndex":
        if self.length != other.length:
            raise ValueError("Bitmap lengths differ")
        return BitmapIndex(self.bits & other.bits, self.length)

    def or_(self, other: "BitmapIndex") -> "BitmapIndex":
        if self.length != other.length:
            raise ValueError("Bitmap lengths differ")
        return BitmapIndex(self.bits | other.bits, self.length)

    def not_(self) -> "BitmapIndex":
        mask = (1 << self.length) - 1
        return BitmapIndex((~self.bits) & mask, self.length)

    def to_base64(self) -> str:
        # Serialize as 8-byte length prefix + big-endian bits bytes, then gzip+base64
        length_bytes = int(self.length).to_bytes(8, "big")
        if self.bits == 0:
            bits_bytes = b"\x00"
        else:
            byte_len = (self.bits.bit_length() + 7) // 8
            bits_bytes = self.bits.to_bytes(byte_len, "big")

        payload = length_bytes + bits_bytes
        compressed = gzip.compress(payload)
        return base64.b64encode(compressed).decode("ascii")

    @staticmethod
    def from_base64(s: str) -> "BitmapIndex":
        compressed = base64.b64decode(s)
        payload = gzip.decompress(compressed)
        if len(payload) < 9:
            raise ValueError("Invalid bitmap payload")
        length = int.from_bytes(payload[:8], "big")
        bits_bytes = payload[8:]
        if not bits_bytes:
            bits = 0
        else:
            bits = int.from_bytes(bits_bytes, "big")
        return BitmapIndex(bits, length)
