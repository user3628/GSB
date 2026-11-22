"""End-to-end offline demonstration against an isolated temporary database.

Runs the complete pipeline without HTTP (the same service methods the API
uses): topology snapshot, four SLI batches, topology increment + full
replacement, alert/suppression lifecycle, quantile index, RCA scoring,
fault injection rollback and a simulated forced-kill recovery.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rca_pipeline.config import get_settings
from rca_pipeline.errors import RebuildFailed, StaleBase
from rca_pipeline.service import Pipeline

DATA = ROOT / "data"


def heading(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> None:
    db_path = Path(tempfile.mkdtemp(prefix="rca_demo_")) / "demo.db"
    pipeline = Pipeline(get_settings(db_path))

    heading("1. topology snapshot v1")
    topo_v1 = json.loads((DATA / "topology_snapshot.json").read_text())
    print(pipeline.submit_topology(topo_v1)["topology_version"])

    heading("2. SLI batches (healthy -> degraded -> sustained -> recovered)")
    batches = json.loads((DATA / "sli_samples.json").read_text())["batches"]
    for batch in batches:
        result = pipeline.submit_samples(batch)
        print(f"{batch['commit_id']}: version={result['new_version']} "
              f"records={result['record_count']} duplicates={result['duplicate_count']}")

    heading("3. quantile index (payment, nearest-rank, version range)")
    quantiles = pipeline.query_quantiles(service="payment")
    for entry in quantiles["entries"]:
        print(entry["metric"], entry["quantiles"], "good_rate", entry["good_rate"],
              "range", entry["version_range"])

    heading("4. alert lifecycle states observed")
    for status in ("FIRING", "SUPPRESSED", "RESOLVED"):
        rows = pipeline.query_alerts(status=status, service="payment")["alerts"]
        print(status, len(rows))

    heading("5. suppression index")
    for entry in pipeline.query_suppression()["entries"][:4]:
        print(entry["service"], entry["level"], "count", entry["suppressed_count"])

    heading("6. RCA candidates for one payment alert")
    alert = pipeline.query_alerts(service="payment")["alerts"][0]
    rca = pipeline.query_rca(alert["id"])
    for candidate in rca["candidates"][:5]:
        print(candidate["rank"], candidate["candidate"], "hops", candidate["hops"],
              "score", candidate["score"])
    print("numeric contract:", rca["numeric_contract"]["rounding"])

    heading("7. topology increment (payment -> risk) cascades rebuild")
    topo_v2 = json.loads((DATA / "topology_increment.json").read_text())
    print("affected", pipeline.submit_topology(topo_v2)["affected_services"])

    heading("8. stale base rejected")
    try:
        stale = dict(topo_v2, commit_id="stale-x", base_version=1)
        pipeline.submit_topology(stale)
    except StaleBase as exc:
        print(exc.code, exc.context)

    heading("9. fault injection: fail_rebuild rolls back atomically")
    current = pipeline.diagnostic_report()["versions"]["sample_version"]
    pipeline.inject_fault("fail_rebuild")
    try:
        pipeline.submit_samples({
            "commit_id": "fault-batch",
            "base_version": current,
            "records": [{"ts_ms": 9_000_000_000 + i, "service": "gateway",
                         "metric": "request_latency_us", "is_good": True,
                         "latency_us": 1000} for i in range(5)],
        })
    except RebuildFailed as exc:
        print(exc.code, "-> version still",
              pipeline.diagnostic_report()["versions"]["sample_version"])

    heading("10. full snapshot replacement (drops cache, no content mixing)")
    topo_v3 = json.loads((DATA / "topology_snapshot_v3.json").read_text())
    pipeline.submit_topology(topo_v3)
    live_services = {r["service"] for r in pipeline.diagnostic_report()["service_refs"]}
    print("cache removed:", "cache" not in live_services, "| services:", sorted(live_services))

    heading("11. diagnostic report")
    report = pipeline.diagnostic_report()
    print("versions", report["versions"])
    print("counts", report["counts"])
    print("\nDemo database:", db_path)


if __name__ == "__main__":
    main()
