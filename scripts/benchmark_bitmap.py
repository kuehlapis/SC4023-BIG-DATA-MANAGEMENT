import time
import json
import os
import sys

# Ensure project root is on sys.path so imports like `model.*` resolve
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from model.DatabaseModel import DatabaseModel
from model.TableModel import Table
from model.QueryModel import Query

DBS = ["bitmap", "sortmth"]


def measure(db_name):
    result = {"db": db_name}
    db = DatabaseModel(db_name)
    eng = db.get_engine()
    tbl = Table(eng, name=db_name)

    t0 = time.time()
    tbl.load()
    t1 = time.time()
    result["load_time_s"] = t1 - t0

    # pick a categorical column that is a bitmap candidate
    cat_col = "town"
    try:
        col_unit = tbl.get_unit(cat_col)
        col_vals = col_unit.scan()
        if not col_vals:
            result["town_eq_time_s"] = None
            result["town_match_count"] = 0
        else:
            sample_val = col_vals[len(col_vals) // 3]
            q = Query(tbl)
            t0 = time.time()
            q.where_eq(cat_col, sample_val)
            t1 = time.time()
            result["town_eq_time_s"] = t1 - t0
            result["town_match_count"] = len(q.select())
    except Exception as e:
        result["town_eq_error"] = str(e)

    # range query on month_num (sorted column)
    range_col = "month_num"
    try:
        col_unit = tbl.get_unit(range_col)
        col_vals = col_unit.scan()
        thr = None
        if col_vals:
            # pick median as threshold
            try:
                thr = int(col_vals[len(col_vals) // 2])
            except Exception:
                # fallback: use string threshold
                thr = col_vals[len(col_vals) // 2]
        q2 = Query(tbl)
        t0 = time.time()
        q2.where_gte(range_col, thr)
        t1 = time.time()
        result["month_gte_time_s"] = t1 - t0
        result["month_match_count"] = len(q2.select())
    except Exception as e:
        result["month_gte_error"] = str(e)

    return result


if __name__ == "__main__":
    out = []
    for d in DBS:
        print(f"Measuring {d}...")
        r = measure(d)
        print(json.dumps(r, indent=2))
        out.append(r)
    # summary
    print("\nSummary:")
    print(json.dumps(out, indent=2))
