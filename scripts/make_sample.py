"""Generate the deterministic local sample database and statistics snapshots.

Everything is driven by a fixed numpy seed, so the SQLite database, the
two snapshot versions and the deliberately broken snapshot are byte
reproducible. No external database service is involved.

Usage: python scripts/make_sample.py [output_dir]   (default: data/)
"""

import json
import os
import sqlite3
import sys

import numpy as np

SEED = 20261120
N_BUCKETS = 16
PAGE_SIZE = 4096

STATUSES = ["new", "paid", "shipped", "done", "lost"]
REGIONS = ["east", "west", "north", "south", "ne", "nw", "se", "sw"]


def _table_pages(rows, row_width):
    return max(1, int(np.ceil(rows * row_width / PAGE_SIZE)))


def _index_pages(rows, n_columns):
    return max(1, int(np.ceil(rows * 12 * n_columns / PAGE_SIZE)))


def _build_histogram(values):
    """Equi-depth histogram over non-null values; deterministic ordering."""
    v = np.sort(np.asarray(values, dtype=np.float64), kind="stable")
    n = len(v)
    if n == 0:
        return None
    edges = [int(round(i * n / N_BUCKETS)) for i in range(N_BUCKETS + 1)]
    boundaries = [float(v[0])]
    for e in edges[1:]:
        boundaries.append(float(v[min(max(e - 1, 0), n - 1)]))
    counts = [int(edges[i + 1] - edges[i]) for i in range(N_BUCKETS)]
    ndv_per_bucket = [
        int(np.unique(v[edges[i]:edges[i + 1]]).size) for i in range(N_BUCKETS)
    ]
    return {"boundaries": boundaries, "counts": counts,
            "ndv_per_bucket": ndv_per_bucket}


def _column_stats(values, numeric=True):
    arr = list(values)
    non_null = [x for x in arr if x is not None]
    null_frac = (len(arr) - len(non_null)) / len(arr) if arr else 0.0
    ndv = len(set(non_null))
    col = {"ndv": int(ndv), "null_frac": float(null_frac)}
    if numeric and non_null:
        col["histogram"] = _build_histogram(non_null)
    return col


def generate_data():
    """Deterministic, skewed, correlated sample data (fixed seed)."""
    rng = np.random.default_rng(SEED)

    # customers: city -> region functional dependency, skewed regions.
    n_customers = 2000
    region_idx = rng.choice(len(REGIONS), size=n_customers,
                            p=np.array([0.30, 0.22, 0.14, 0.12, 0.08, 0.06, 0.05, 0.03]))
    city_per_region = rng.integers(0, 5, size=n_customers)
    cities = ["c_%d_%d" % (r, c) for r, c in zip(region_idx, city_per_region)]
    ages = np.clip(rng.normal(45, 15, size=n_customers), 18, 90).round(0)
    ages = [None if i % 17 == 0 else float(a) for i, a in enumerate(ages)]
    customers = {
        "id": list(range(1, n_customers + 1)),
        "region": [REGIONS[r] for r in region_idx],
        "city": cities,
        "age": ages,
    }

    # orders: skewed status, lognormal-ish totals, day-of-year.
    n_orders = 20000
    status_idx = rng.choice(len(STATUSES), size=n_orders,
                            p=[0.42, 0.28, 0.17, 0.10, 0.03])
    totals = np.round(rng.lognormal(mean=4.0, sigma=0.9, size=n_orders), 2)
    created_day = rng.integers(1, 366, size=n_orders)
    orders = {
        "id": list(range(1, n_orders + 1)),
        "customer_id": rng.integers(1, n_customers + 1, size=n_orders).tolist(),
        "status": [STATUSES[s] for s in status_idx],
        "total": totals.tolist(),
        "created_day": created_day.tolist(),
    }

    # order_items: zipf-like products, qty correlated with price band.
    n_items = 60000
    product_ranks = rng.zipf(1.4, size=n_items)
    order_items = {
        "order_id": rng.integers(1, n_orders + 1, size=n_items).tolist(),
        "product_id": ((product_ranks - 1) % 500 + 1).tolist(),
        "qty": rng.integers(1, 6, size=n_items).tolist(),
        "price": np.round(rng.lognormal(mean=3.0, sigma=0.8, size=n_items), 2).tolist(),
    }
    return customers, orders, order_items, rng


def _extend_data(customers, orders, order_items, rng):
    """Second deterministic batch used to build snapshot v2."""
    n_new_customers = 500
    base = len(customers["id"])
    region_idx = rng.choice(len(REGIONS), size=n_new_customers,
                            p=np.array([0.30, 0.22, 0.14, 0.12, 0.08, 0.06, 0.05, 0.03]))
    city_per_region = rng.integers(0, 5, size=n_new_customers)
    customers["id"] += list(range(base + 1, base + 1 + n_new_customers))
    customers["region"] += [REGIONS[r] for r in region_idx]
    customers["city"] += ["c_%d_%d" % (r, c) for r, c in zip(region_idx, city_per_region)]
    customers["age"] += [None if i % 11 == 0 else float(a) for i, a in
                         enumerate(np.clip(rng.normal(44, 14, size=n_new_customers), 18, 90).round(0))]

    n_new_orders = 5000
    base_o = len(orders["id"])
    status_idx = rng.choice(len(STATUSES), size=n_new_orders,
                            p=[0.42, 0.28, 0.17, 0.10, 0.03])
    orders["id"] += list(range(base_o + 1, base_o + 1 + n_new_orders))
    orders["customer_id"] += rng.integers(1, len(customers["id"]) + 1, size=n_new_orders).tolist()
    orders["status"] += [STATUSES[s] for s in status_idx]
    orders["total"] += np.round(rng.lognormal(mean=4.1, sigma=0.9, size=n_new_orders), 2).tolist()
    orders["created_day"] += rng.integers(1, 366, size=n_new_orders).tolist()

    n_new_items = 15000
    product_ranks = rng.zipf(1.4, size=n_new_items)
    order_items["order_id"] += rng.integers(1, len(orders["id"]) + 1, size=n_new_items).tolist()
    order_items["product_id"] += ((product_ranks - 1) % 500 + 1).tolist()
    order_items["qty"] += rng.integers(1, 6, size=n_new_items).tolist()
    order_items["price"] += np.round(rng.lognormal(mean=3.0, sigma=0.8, size=n_new_items), 2).tolist()


def build_snapshot(version, generated_at, customers, orders, order_items):
    tables = {
        "customers": {
            "row_count": len(customers["id"]),
            "pages": _table_pages(len(customers["id"]), 64),
            "columns": {
                "id": _column_stats(customers["id"]),
                "region": _column_stats(customers["region"], numeric=False),
                "city": _column_stats(customers["city"], numeric=False),
                "age": _column_stats(customers["age"]),
            },
        },
        "orders": {
            "row_count": len(orders["id"]),
            "pages": _table_pages(len(orders["id"]), 80),
            "columns": {
                "id": _column_stats(orders["id"]),
                "customer_id": _column_stats(orders["customer_id"]),
                "status": _column_stats(orders["status"], numeric=False),
                "total": _column_stats(orders["total"]),
                "created_day": _column_stats(orders["created_day"]),
            },
        },
        "order_items": {
            "row_count": len(order_items["order_id"]),
            "pages": _table_pages(len(order_items["order_id"]), 64),
            "columns": {
                "order_id": _column_stats(order_items["order_id"]),
                "product_id": _column_stats(order_items["product_id"]),
                "qty": _column_stats(order_items["qty"]),
                "price": _column_stats(order_items["price"]),
            },
        },
    }
    indexes = [
        {"name": "idx_customers_city", "table": "customers", "columns": ["city"],
         "pages": _index_pages(len(customers["id"]), 1)},
        {"name": "idx_customers_region", "table": "customers", "columns": ["region"],
         "pages": _index_pages(len(customers["id"]), 1)},
        {"name": "idx_items_order", "table": "order_items", "columns": ["order_id"],
         "pages": _index_pages(len(order_items["order_id"]), 1)},
        {"name": "idx_items_product", "table": "order_items", "columns": ["product_id"],
         "pages": _index_pages(len(order_items["order_id"]), 1)},
        {"name": "idx_orders_cust_total", "table": "orders",
         "columns": ["customer_id", "total"],
         "pages": _index_pages(len(orders["id"]), 2)},
        {"name": "idx_orders_status", "table": "orders", "columns": ["status"],
         "pages": _index_pages(len(orders["id"]), 1)},
        {"name": "idx_orders_total", "table": "orders", "columns": ["total"],
         "pages": _index_pages(len(orders["id"]), 1)},
    ]
    correlations = [
        {"table": "customers", "columns": ["city", "region"],
         "kind": "functional_dependency", "value": 1.0},
        {"table": "orders", "columns": ["created_day", "status"],
         "kind": "coefficient", "value": 0.5},
        {"table": "order_items", "columns": ["price", "qty"],
         "kind": "coefficient", "value": 0.6},
    ]
    return {
        "format": "qopt-snapshot/1",
        "version": version,
        "generated_at": generated_at,
        "tables": tables,
        "indexes": indexes,
        "correlations": correlations,
    }


def write_sqlite(path, customers, orders, order_items):
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT, city TEXT, age REAL)")
    cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, total REAL, created_day INTEGER)")
    cur.execute("CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, qty INTEGER, price REAL)")
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?)",
                    zip(customers["id"], customers["region"], customers["city"], customers["age"]))
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?)",
                    zip(orders["id"], orders["customer_id"], orders["status"],
                        orders["total"], orders["created_day"]))
    cur.executemany("INSERT INTO order_items VALUES (?,?,?,?)",
                    zip(order_items["order_id"], order_items["product_id"],
                        order_items["qty"], order_items["price"]))
    con.commit()
    con.close()


def _dump(obj, path):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")


def main(out_dir="data"):
    os.makedirs(out_dir, exist_ok=True)
    customers, orders, order_items, rng = generate_data()

    snap_v1 = build_snapshot("v1", "2026-11-20T01:00:00Z",
                             customers, orders, order_items)
    _dump(snap_v1, os.path.join(out_dir, "snapshot_v1.json"))

    _extend_data(customers, orders, order_items, rng)
    write_sqlite(os.path.join(out_dir, "sample.db"),
                 customers, orders, order_items)
    snap_v2 = build_snapshot("v2", "2026-11-20T02:00:00Z",
                             customers, orders, order_items)
    _dump(snap_v2, os.path.join(out_dir, "snapshot_v2.json"))

    # Deliberately broken snapshot for the E_SNAPSHOT_INVALID demo:
    # histogram counts length mismatch + missing generated_at.
    broken = json.loads(json.dumps(snap_v1))
    del broken["generated_at"]
    broken["tables"]["orders"]["columns"]["total"]["histogram"]["counts"] = [1, 2, 3]
    _dump(broken, os.path.join(out_dir, "snapshot_broken.json"))

    print("sample.db rows: customers=%d orders=%d order_items=%d"
          % (len(customers["id"]), len(orders["id"]), len(order_items["order_id"])))
    print("wrote snapshot_v1.json, snapshot_v2.json, snapshot_broken.json to %s" % out_dir)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "data")
