import bisect
from typing import List, Dict, Any


class ZoneMap:
    """Simple zonemap index: fixed-size blocks storing min/max and start/end indexes.

    Values are expected to be in the same order as the underlying column (row order).
    """

    def __init__(self, blocks: List[Dict[str, Any]], block_size: int):
        self.blocks = blocks
        self.block_size = int(block_size)

    @staticmethod
    def build(values: List, block_size: int = 512) -> "ZoneMap":
        blocks = []
        n = len(values)
        for i in range(0, n, block_size):
            chunk = values[i:i + block_size]
            if not chunk:
                continue
            try:
                bmin = min(chunk)
                bmax = max(chunk)
            except Exception:
                # For heterogeneous values, convert to string for comparisons
                s_chunk = [str(x) for x in chunk]
                bmin = min(s_chunk)
                bmax = max(s_chunk)

            blocks.append({
                "min": bmin,
                "max": bmax,
                "start": i,
                "end": i + len(chunk)
            })

        return ZoneMap(blocks, block_size)

    def to_dict(self) -> Dict:
        return {"blocks": self.blocks, "block_size": self.block_size}

    @staticmethod
    def from_dict(d: Dict) -> "ZoneMap":
        return ZoneMap(d.get("blocks", []), d.get("block_size", 512))

    def find_start(self, threshold, col_values: List) -> int:
        """Find the first index with value >= threshold using the zonemap to skip blocks.

        Returns an index between 0 and len(col_values).
        """
        # Find first block whose max >= threshold
        for blk in self.blocks:
            try:
                if blk["max"] >= threshold:
                    lo = blk["start"]
                    hi = blk["end"]
                    idx = bisect.bisect_left(col_values, threshold, lo, hi)
                    return idx
            except Exception:
                if str(blk["max"]) >= str(threshold):
                    lo = blk["start"]
                    hi = blk["end"]
                    idx = bisect.bisect_left([str(x) for x in col_values], str(threshold), lo, hi)
                    return idx

        return len(col_values)
