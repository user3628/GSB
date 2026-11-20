"""End-to-end demo on the fixed deterministic sample library.

Starts the local HTTP API on the reserved loopback port 127.0.0.1:8787,
loads snapshot v1 and runs three typical queries, printing candidate
indexes, estimated cardinalities, cost breakdowns and the final choice
for manual verification. Then exercises snapshot versioning, the
cartesian ceiling policy and the business error codes.

Usage: .venv/bin/python scripts/demo.py
"""

import json
import os
import subprocess
import sys
import threading
import urllib.request
import urllib.error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from qopt.server import create_server  # noqa: E402
from qopt.numeric import summation_error_report  # noqa: E402
from qopt.stats import load_snapshot  # noqa: E402

HOST, PORT = "127.0.0.1", 8787
BASE = "http://%s:%d" % (HOST, PORT)
DATA = os.path.join(os.path.dirname(__file__), "..", "data")

Q1 = {
    "name": "Q1 single table: equality + range, composite index partial prefix",
    "query": {
        "tables": ["orders"],
        "predicates": [
            {"type": "eq", "table": "orders", "column": "customer_id", "value": 42},
            {"type": "range", "table": "orders", "column": "total",
             "low": 100, "high": 500},
            {"type": "eq", "table": "orders", "column": "status", "value": "paid"},
        ],
    },
}

Q2 = {
    "name": "Q2 two-table join with declared functional dependency city->region",
    "query": {
        "tables": ["customers", "orders"],
        "predicates": [
            {"type": "eq", "table": "customers", "column": "city", "value": "c_0_1"},
            {"type": "eq", "table": "customers", "column": "region", "value": "east"},
            {"type": "range", "table": "orders", "column": "created_day",
             "low": 100, "high": 200},
        ],
        "join_predicates": [
            {"type": "eq",
             "left": {"table": "orders", "column": "customer_id"},
             "right": {"table": "customers", "column": "id"}},
        ],
    },
}

Q3 = {
    "name": "Q3 three-table join with IN list and IS NOT NULL",
    "query": {
        "tables": ["customers", "orders", "order_items"],
        "predicates": [
            {"type": "in", "table": "orders", "column": "status",
             "values": ["paid", "shipped"]},
            {"type": "is_not_null", "table": "customers", "column": "age"},
            {"type": "range", "table": "order_items", "column": "price",
             "low": 10, "high": 80},
        ],
        "join_predicates": [
            {"type": "eq",
             "left": {"table": "orders", "column": "customer_id"},
             "right": {"table": "customers", "column": "id"}},
            {"type": "eq",
             "left": {"table": "order_items", "column": "order_id"},
             "right": {"table": "orders", "column": "id"}},
        ],
    },
}

Q4_CARTESIAN = {
    "name": "Q4 cartesian product (no join predicate): explicit ceiling policy",
    "query": {"tables": ["orders", "customers"]},
}


def http(method, path, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def show_plan(label, plan):
    print("  snapshot=%s fingerprint=%s" % (plan["snapshot_version"],
                                            plan["snapshot_fingerprint"][:16]))
    print("  join_order: %s" % " -> ".join(plan["join_order"]))
    for step in plan["join_steps"]:
        print("  join step +%-12s card=%.6g cartesian=%s cap_applied=%s"
              % (step["added_table"], step["intermediate_cardinality"],
                 step["cartesian"], step["cap_applied"]))
    print("  final_cardinality=%.6g" % plan["final_cardinality"])
    for trep in plan["tables"]:
        print("  table %-12s rows=%d local_sel=%.6g est_rows=%.6g"
              % (trep["table"], trep["row_count"], trep["local_selectivity"],
                 trep["estimated_rows"]))
        for note in trep["assumptions"]:
            print("    assumption: %s" % note)
        for cand in trep["candidates"]:
            if "cost" in cand:
                cost = cand["cost"]
                line = ("    [%s] %-24s total=%.6f (cpu=%.6f io=%.6f sel_part=%.6f)"
                        % (cand["status"], cand["access"], cost["total_cost"],
                           cost["cpu_cost"], cost["io_cost"],
                           cost["selectivity_sensitive_cost"]))
                if cand.get("matched_columns"):
                    line += " matched=%s" % ",".join(cand["matched_columns"])
            else:
                line = "    [%s] %-24s" % (cand["status"], cand["access"])
            print(line)
            if cand.get("reason"):
                print("        reason: %s" % cand["reason"])
    total = plan["total_cost"]
    print("  TOTAL cost=%.6f (cpu=%.6f io=%.6f) rounding<=%.1e"
          % (total["total_cost"], total["cpu_cost"], total["io_cost"],
             total["rounding"]["max_accumulated_rounding_error"]))


def main():
    if not os.path.exists(os.path.join(DATA, "snapshot_v1.json")):
        subprocess.run([sys.executable,
                        os.path.join(os.path.dirname(__file__), "make_sample.py")],
                       check=True)
    server, registry = create_server(host=HOST, port=PORT)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, body = http("GET", "/v1/health")
        assert status == 200 and body["ok"], body
        print("health: OK on %s" % BASE)

        status, body = http("POST", "/v1/snapshot/load",
                            {"path": os.path.join(DATA, "snapshot_v1.json")})
        assert status == 200, body
        print("loaded snapshot: %s generated_at=%s"
              % (body["snapshot"]["version"], body["snapshot"]["generated_at"]))

        for q in (Q1, Q2, Q3):
            print("\n=== %s" % q["name"])
            status, body = http("POST", "/v1/plan", {"query": q["query"]})
            assert status == 200, body
            show_plan(q["name"], body["plan"])

        print("\n=== determinism: Q1 planned twice must be byte-identical")
        req = urllib.request.Request(
            BASE + "/v1/plan", method="POST",
            data=json.dumps({"query": Q1["query"]}).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        raw1 = urllib.request.urlopen(req, timeout=10).read()
        raw2 = urllib.request.urlopen(req, timeout=10).read()
        assert raw1 == raw2, "plan output is not deterministic"
        print("  identical bytes: %d == %d -> OK" % (len(raw1), len(raw2)))

        print("\n=== Kahan compensated summation: quantified error bounds")
        snap = load_snapshot(os.path.join(DATA, "snapshot_v1.json"))
        hist = snap.table("orders").column("total").histogram
        report = summation_error_report(hist.counts)
        print("  orders.total histogram: %d buckets, sum|x|=%.6g"
              % (report["n_terms"], report["sum_abs"]))
        print("  kahan=%r naive=%r observed|diff|=%.3g"
              % (report["kahan_result"], report["naive_result"],
                 report["observed_abs_diff"]))
        print("  error bound: kahan<=%.3g  naive<=%.3g (u=2^-53)"
              % (report["kahan_error_bound"], report["naive_error_bound"]))
        skewed = [1e16] + [1.0] * 1000 + [-1e16]
        rep2 = summation_error_report(skewed)
        print("  adversarial skew [1e16, 1x1000, -1e16]: kahan=%r naive=%r"
              % (rep2["kahan_result"], rep2["naive_result"]))

        print("\n=== query pinned to snapshot_version=v1")
        status, body = http("POST", "/v1/plan",
                            {"query": Q1["query"], "snapshot_version": "v1"})
        assert status == 200 and body["plan"]["snapshot_version"] == "v1", body
        print("  served by snapshot %s -> OK" % body["plan"]["snapshot_version"])

        print("\n=== %s" % Q4_CARTESIAN["name"])
        status, body = http("POST", "/v1/plan", {"query": Q4_CARTESIAN["query"]})
        assert status == 200, body
        plan = body["plan"]
        print("  join_order: %s" % " -> ".join(plan["join_order"]))
        for note in plan["join_notes"]:
            print("  note: %s" % note)
        print("  final_cardinality=%.6g cap_applied=%s"
              % (plan["final_cardinality"], plan["cartesian_cap_policy"]["cap_applied"]))

        print("\n=== snapshot versioning: load v2 (atomic replace)")
        status, body = http("POST", "/v1/snapshot/load",
                            {"path": os.path.join(DATA, "snapshot_v2.json")})
        assert status == 200 and body["snapshot"]["version"] == "v2", body
        print("  active snapshot now %s" % body["snapshot"]["version"])
        status, body = http("POST", "/v1/plan",
                            {"query": Q1["query"], "snapshot_version": "v1"})
        assert status == 400 and body["error"]["code"] == "E_SNAPSHOT_VERSION_MISMATCH", body
        print("  stale version v1 rejected: %s" % body["error"]["code"])

        print("\n=== error codes")
        status, body = http("POST", "/v1/snapshot/load",
                            {"path": os.path.join(DATA, "snapshot_broken.json")})
        assert status == 400 and body["error"]["code"] == "E_SNAPSHOT_INVALID", body
        print("  broken snapshot -> %s (%s)"
              % (body["error"]["code"], body["error"]["message"]))
        status, body = http("GET", "/v1/snapshot")
        assert status == 200 and body["snapshot"]["version"] == "v2", body
        print("  previous snapshot v2 preserved after failed load (atomic) -> OK")
        status, body = http("POST", "/v1/plan",
                            {"query": {"tables": ["ghost_table"]}})
        assert status == 400 and body["error"]["code"] == "E_STATS_MISSING", body
        print("  unknown table -> %s" % body["error"]["code"])

        print("\nALL DEMO CHECKS PASSED")
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
