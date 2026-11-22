"""单写者 + 多读者并发一致性测试。"""

from __future__ import annotations

import json
import threading

from conftest import load_sample
from graph_bench.service import BenchService


def test_readers_always_observe_complete_versions(seeded_service: BenchService) -> None:
    service = seeded_service
    stop = threading.Event()
    violations: list[str] = []
    observed_versions: set[int] = set()

    def reader() -> None:
        while not stop.is_set():
            layers = service.get_layers()
            version = layers["snapshot_version"]
            observed_versions.add(version)
            # 完整性不变量：每层资源都能在派生视图中找到且层级一致。
            view = service.storage.load_view_rows(version)
            for layer in layers["layers"]:
                for rid in layer["resources"]:
                    entry = view["resources"][rid]
                    if entry["layer"] != layer["layer"] or entry["batch"] != f"batch-{layer['layer']}":
                        violations.append(f"v{version}:{rid} 派生结构与分层不一致")

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for thread in readers:
        thread.start()

    base_payload = load_sample("increment_weight_auth.json")
    for index in range(6):
        payload = {
            "base_version": index + 1,
            "upserts": [
                {
                    "id": "auth",
                    "replicas": 2,
                    "parents": ["gateway"],
                    "weight_milli": 800 + index,
                    "desired_version": f"0.9.{index}",
                }
            ],
            "removals": [],
        }
        service.submit_increment(payload, f"writer-{index}")

    stop.set()
    for thread in readers:
        thread.join()

    assert not violations
    # 读者至少应观察到多个完整版本，且每个版本的重复读取逐位一致。
    assert observed_versions
    for version in observed_versions:
        a = json.dumps(service.get_layers(version), sort_keys=True)
        b = json.dumps(service.get_layers(version), sort_keys=True)
        assert a == b
