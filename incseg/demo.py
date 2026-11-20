"""End-to-end offline demonstration of the incseg engine."""

from __future__ import annotations

import json
import shutil
import threading
import urllib.request
from pathlib import Path

from .engine import Engine, QUANTIZE_OUTPUTS
from .generator import generate_workspace
from .util import sha256_file


def _banner(text):
    print("=" * 68)
    print(text)
    print("=" * 68)


def _output_digests(engine, tile_ids):
    digests = {}
    for tile_id in tile_ids:
        for name in QUANTIZE_OUTPUTS:
            path = engine.artifacts_dir(tile_id, "quantize") / name
            digests[f"{tile_id}/{name}"] = sha256_file(path)
    return digests


def run_demo(workspace, with_http=False):
    workspace = Path(workspace)
    tile_ids = [f"tile_{i:03d}" for i in range(4)]
    _banner("STEP 1: generate deterministic workspace (4 tiles, seed=42)")
    if workspace.exists():
        shutil.rmtree(workspace)
    generate_workspace(workspace, tiles=4, seed=42, size=96)
    print(f"workspace: {workspace}")
    print(f"tiles:     {', '.join(tile_ids)}")
    _banner("STEP 2: full build (4 stages x 4 tiles)")
    engine = Engine(workspace)
    report = engine.run()
    units = report["units_completed"]
    print(f"stage units executed: {units} (expected 16)")
    assert units == 16, units
    summary_path = engine.artifacts_dir("tile_000", "quantize") / "summary.json"
    summary = json.loads(summary_path.read_text())
    print("tile_000 summary (fixed, reproducible):")
    for entry in summary["classes"]:
        print(f"  class {entry['class_id']:>3} {entry['name']:<11} "
              f"pixels={entry['pixels']:>5} ratio={entry['ratio']}")
    print(f"  instances={summary['instances']['count']} "
          f"boundary_pixels={summary['boundary_pixels']}")
    baseline = _output_digests(engine, tile_ids)
    _banner("STEP 3: graceful stop between stage units + resume")
    ws_stop = workspace.with_name(workspace.name + "_stop")
    if ws_stop.exists():
        shutil.rmtree(ws_stop)
    generate_workspace(ws_stop, tiles=4, seed=42, size=96)
    engine_stop = Engine(ws_stop)
    partial = engine_stop.run(stop_after_units=5)
    print(f"stop requested after 5 units -> stopped={partial['stopped']}, "
          f"units_completed={partial['units_completed']}")
    seqs = [r.seq for r in engine_stop.store.load_records("tile_001")]
    print(f"tile_001 checkpoint seqs after stop: {seqs}")
    resumed = engine_stop.run()
    print(f"resume -> stopped={resumed['stopped']}, "
          f"units_completed={resumed['units_completed']} (expected 11)")
    resumed_digests = _output_digests(engine_stop, tile_ids)
    mismatch = [k for k in baseline if baseline[k] != resumed_digests[k]]
    assert not mismatch, mismatch
    for tile_id in tile_ids:
        seqs = [r.seq for r in engine_stop.store.load_records(tile_id)]
        assert seqs == [1, 2, 3, 4], (tile_id, seqs)
    all_seqs = [r.seq for tile_id in tile_ids
                for r in engine_stop.store.load_records(tile_id)]
    print("resume outputs are BYTE-IDENTICAL to the uninterrupted build")
    print(f"checkpoint seqs continuous, no gaps/dups: {sorted(all_seqs)}")
    verify_stop = engine_stop.verify()
    print(f"verify(stop/resumed workspace): {verify_stop['status']}")
    shutil.rmtree(ws_stop)
    _banner("STEP 4: single-tile config change -> incremental rebuild")
    from .config import TileConfig, WorkspaceConfig
    config = WorkspaceConfig.load(workspace / "config.json")
    data = config.tile("tile_002").to_dict()
    data["min_area"] = 80
    config.replace_tile(TileConfig.from_dict(data))
    config.save(workspace / "config.json")
    print("tile_002 config changed: min_area 24 -> 80")
    plan = engine.inspect_tile(engine.load_config().tile("tile_002"))
    print("invalidation plan for tile_002 (observable via status/API):")
    for stage, info in plan["stages"].items():
        print(f"  {stage:<12} status={info['status']:<7} reason={info['reason']}")
    rebuild = engine.run(tile_ids=["tile_002"])
    stages = rebuild["tiles"]["tile_002"]["stages"]
    reused = [s for s, i in stages.items() if i["status"] == "reused"]
    rebuilt = [s for s, i in stages.items() if i["status"] == "rebuilt"]
    print(f"reused stages:   {reused}")
    print(f"rebuilt stages:  {rebuilt}")
    assert reused == ["classify"] and rebuilt == ["instances", "postprocess", "quantize"]
    result = engine.verify()
    print(f"verify(incremental vs clean full build): {result['status']} "
          f"({len(result['checked_files'])} files byte-compared)")
    if with_http:
        _banner("STEP 5: loopback HTTP API")
        _demo_http(workspace)
    _banner("ALL CHECKS PASSED")
    return 0


def _demo_http(workspace):
    from . import server as server_mod

    handler = type("DemoHandler", (server_mod._Handler,), {})
    httpd = server_mod.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    httpd.engine = Engine(workspace)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    port = httpd.server_address[1]
    base = f"http://127.0.0.1:{port}"
    try:
        def get(path):
            with urllib.request.urlopen(base + path) as resp:
                return resp.status, resp.read()

        def post(path, payload=None):
            data = json.dumps(payload or {}).encode()
            req = urllib.request.Request(base + path, data=data,
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as resp:
                return resp.status, resp.read()

        status, _ = get("/health")
        print(f"GET  /health                 -> {status}")
        status, body = get("/tiles")
        print(f"GET  /tiles                  -> {status} {body.decode().strip()[:60]}...")
        status, _ = get("/tiles/tile_002")
        print(f"GET  /tiles/tile_002         -> {status} (stage fingerprints)")
        status, _ = get("/invalidations")
        print(f"GET  /invalidations          -> {status}")
        status, body = get("/tiles/tile_000/export?kind=summary")
        print(f"GET  /tiles/tile_000/export?kind=summary -> {status} ({len(body)} bytes)")
        status, _ = post("/stop")
        print(f"POST /stop                   -> {status}")
        status, _ = post("/resume")
        print(f"POST /resume                 -> {status}")
        status, _ = post("/verify")
        print(f"POST /verify                 -> {status}")
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)
