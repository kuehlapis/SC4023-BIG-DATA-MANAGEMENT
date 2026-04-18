import os
import json
from collections import defaultdict
from typing import List, Dict, Any, Optional

DEFAULT_BLOCK_SIZE = 4096
LEAF_CAPACITY = 256
INTERNAL_CAPACITY = 256


def build_index(db_path: str, col_name: str, block_size: int = DEFAULT_BLOCK_SIZE,
                low_card_threshold: int = 512) -> dict:
    col_file = os.path.join(db_path, f"{col_name}.col")
    if not os.path.exists(col_file):
        return {}

    leaf_blocks = []
    value_positions = defaultdict(list)
    row = 0
    cur_block_start = 0
    cur_min = None
    cur_max = None

    with open(col_file, "r", encoding="utf-8") as f:
        for line in f:
            v = line.rstrip("\n")

            if cur_min is None:
                cur_min = v
                cur_max = v
            else:
                if v != "":
                    try:
                        if float(v) < float(cur_min):
                            cur_min = v
                        if float(v) > float(cur_max):
                            cur_max = v
                    except Exception:
                        if str(v) < str(cur_min):
                            cur_min = v
                        if str(v) > str(cur_max):
                            cur_max = v

            value_positions[v].append(row)

            if (row + 1) % block_size == 0:
                leaf_blocks.append({
                    "start_row": cur_block_start,
                    "end_row": row,
                    "min": cur_min,
                    "max": cur_max,
                })
                cur_block_start = row + 1
                cur_min = None
                cur_max = None

            row += 1

    if cur_min is not None or row > cur_block_start:
        leaf_blocks.append({
            "start_row": cur_block_start,
            "end_row": row - 1,
            "min": cur_min,
            "max": cur_max,
        })

    unique = len(value_positions)
    value_map = None
    if unique <= low_card_threshold:
        value_map = {k: v for k, v in value_positions.items()}

    index = {
        "col": col_name,
        "row_count": row,
        "block_size": block_size,
        "leaf_blocks": leaf_blocks,
    }
    if value_map is not None:
        index["value_map"] = value_map

    idx_file = os.path.join(db_path, f"{col_name}.idx.json")
    try:
        with open(idx_file, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2)
    except Exception:
        pass

    return index


def load_index(db_path: str, col_name: str) -> dict:
    idx_file = os.path.join(db_path, f"{col_name}.idx.json")
    if not os.path.exists(idx_file):
        return {}
    try:
        with open(idx_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _try_numeric(values: List[str]) -> bool:
    for v in values:
        if v == "":
            continue
        try:
            float(v)
        except Exception:
            return False
    return True


def build_bpt(db_path: str, col_name: str, leaf_capacity: int = LEAF_CAPACITY,
              internal_capacity: int = INTERNAL_CAPACITY) -> Optional[str]:
    """Build a lightweight B+ tree stored as JSON node files. Returns root filename."""
    col_file = os.path.join(db_path, f"{col_name}.col")
    if not os.path.exists(col_file):
        return None

    # group rows by key
    key_rows: Dict[str, List[int]] = defaultdict(list)
    rows_keys = []
    with open(col_file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            k = line.rstrip("\n")
            key_rows[k].append(i)
            rows_keys.append(k)

    unique_keys = list(key_rows.keys())
    numeric = _try_numeric(unique_keys)
    if numeric:
        sort_key = lambda x: float(x) if x != "" else float('-inf')
    else:
        sort_key = lambda x: str(x)

    unique_keys.sort(key=sort_key)

    # create leaf nodes
    node_id = 0
    leaf_files = []
    i = 0
    while i < len(unique_keys):
        chunk = unique_keys[i:i+leaf_capacity]
        keys = chunk
        values = [key_rows[k] for k in keys]
        node = {
            "type": "leaf",
            "keys": keys,
            "values": values,
            "next": None,
            "numeric": numeric
        }
        fname = f"{col_name}.bpt.node.{node_id}.json"
        with open(os.path.join(db_path, fname), "w", encoding="utf-8") as nf:
            json.dump(node, nf, indent=2)
        leaf_files.append(fname)
        node_id += 1
        i += leaf_capacity

    # link leaves
    for idx in range(len(leaf_files)-1):
        p = os.path.join(db_path, leaf_files[idx])
        try:
            with open(p, "r", encoding="utf-8") as rf:
                node = json.load(rf)
            node["next"] = leaf_files[idx+1]
            with open(p, "w", encoding="utf-8") as wf:
                json.dump(node, wf, indent=2)
        except Exception:
            continue

    # build internal levels
    children = leaf_files
    while len(children) > 1:
        new_children = []
        j = 0
        while j < len(children):
            chunk = children[j:j+internal_capacity]
            # separator keys: first key of each child except first
            keys = []
            for ch in chunk[1:]:
                try:
                    with open(os.path.join(db_path, ch), "r", encoding="utf-8") as cf:
                        child_node = json.load(cf)
                        if child_node.get("keys"):
                            keys.append(child_node["keys"][0])
                        else:
                            keys.append("")
                except Exception:
                    keys.append("")

            inode = {
                "type": "internal",
                "keys": keys,
                "children": chunk,
                "numeric": numeric
            }
            fname = f"{col_name}.bpt.node.{node_id}.json"
            with open(os.path.join(db_path, fname), "w", encoding="utf-8") as nf:
                json.dump(inode, nf, indent=2)
            new_children.append(fname)
            node_id += 1
            j += internal_capacity
        children = new_children

    # final root
    root = children[0] if children else None
    # store root pointer file
    if root:
        root_meta = {"root": root, "numeric": numeric}
        root_file = os.path.join(db_path, f"{col_name}.bpt.root.json")
        try:
            with open(root_file, "w", encoding="utf-8") as rf:
                json.dump(root_meta, rf, indent=2)
            return f"{col_name}.bpt.root.json"
        except Exception:
            return root

    return None


def load_bpt_root(db_path: str, col_name: str) -> Optional[Dict[str, Any]]:
    root_file = os.path.join(db_path, f"{col_name}.bpt.root.json")
    if not os.path.exists(root_file):
        return None
    try:
        with open(root_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_node(db_path: str, node_fname: str) -> Optional[Dict[str, Any]]:
    p = os.path.join(db_path, node_fname)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def bpt_search_equal(db_path: str, root_fname: str, key: str) -> List[int]:
    rows: List[int] = []
    root_meta = load_node(db_path, root_fname)
    if not root_meta:
        return rows

    # if wrapper file, load actual root
    if not root_meta.get('type') and root_meta.get('root'):
        root_meta = load_node(db_path, root_meta.get('root'))
    node = root_meta
    numeric = node.get('numeric', False)

    # traverse to leaf
    while node and node.get('type') == 'internal':
        keys = node.get('keys', [])
        i = 0
        for sep in keys:
            try:
                if numeric:
                    if float(key) < float(sep):
                        break
                else:
                    if str(key) < str(sep):
                        break
            except Exception:
                if str(key) < str(sep):
                    break
            i += 1
        child = node.get('children', [])[i]
        node = load_node(db_path, child)

    # search leaf keys
    if not node or node.get('type') != 'leaf':
        return rows

    for k, v in zip(node.get('keys', []), node.get('values', [])):
        try:
            if numeric and float(k) == float(key):
                rows.extend(v)
                break
            if not numeric and str(k) == str(key):
                rows.extend(v)
                break
        except Exception:
            if str(k) == str(key):
                rows.extend(v)
                break

    return rows


def bpt_search_range(db_path: str, root_fname: str, low: Optional[str], high: Optional[str]) -> List[int]:
    rows: List[int] = []
    root_meta = load_node(db_path, root_fname)
    if not root_meta:
        return rows
    if not root_meta.get('type') and root_meta.get('root'):
        root_meta = load_node(db_path, root_meta.get('root'))
    node = root_meta
    numeric = node.get('numeric', False)

    # find starting leaf
    if low is None:
        while node and node.get('type') == 'internal':
            child = node.get('children', [])[0]
            node = load_node(db_path, child)
    else:
        while node and node.get('type') == 'internal':
            keys = node.get('keys', [])
            i = 0
            for sep in keys:
                try:
                    if numeric:
                        if float(low) < float(sep):
                            break
                    else:
                        if str(low) < str(sep):
                            break
                except Exception:
                    if str(low) < str(sep):
                        break
                i += 1
            child = node.get('children', [])[i]
            node = load_node(db_path, child)

    # iterate leaf nodes until keys exceed high
    while node and node.get('type') == 'leaf':
        for k, v in zip(node.get('keys', []), node.get('values', [])):
            try:
                ok_low = True if low is None else (float(k) >= float(low) if numeric else str(k) >= str(low))
                ok_high = True if high is None else (float(k) <= float(high) if numeric else str(k) <= str(high))
            except Exception:
                ok_low = True if low is None else (str(k) >= str(low))
                ok_high = True if high is None else (str(k) <= str(high))
            if ok_low and ok_high:
                rows.extend(v)
        nxt = node.get('next')
        if not nxt:
            break
        node = load_node(db_path, nxt)

    return rows
