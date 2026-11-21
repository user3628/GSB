import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fiw.engine import Engine  # noqa: E402
from test_endpoints.servers import (  # noqa: E402
    BlackholeServer,
    HealthyServer,
    free_port,
)


class Environment:
    """Real local test endpoints plus matching sample JSON files."""

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.healthy_1 = HealthyServer(0, "ep-web-1").start()
        self.healthy_2 = HealthyServer(0, "ep-web-2").start()
        self.healthy_3 = HealthyServer(0, "ep-api-1").start()
        self.blackhole = BlackholeServer(0).start()
        self.unlistened_port = free_port()
        self.servers = [self.healthy_1, self.healthy_2, self.healthy_3,
                        self.blackhole]
        self.endpoints_path = tmp_path / "endpoints.json"
        self.plan_path = tmp_path / "plan.json"
        self.write_catalog()
        self.write_plan()

    def stop(self):
        for srv in self.servers:
            srv.stop()

    def catalog_dict(self):
        return {"endpoints": [
            {"id": "ep-web-1", "node": "node-a", "weight": 0.9, "upstreams": [],
             "host": "127.0.0.1", "port": self.healthy_1.port,
             "profile": "healthy"},
            {"id": "ep-web-2", "node": "node-a", "weight": 0.8, "upstreams": [],
             "host": "127.0.0.1", "port": self.healthy_2.port,
             "profile": "healthy"},
            {"id": "ep-db-1", "node": "node-b", "weight": 0.95, "upstreams": [],
             "host": "127.0.0.1", "port": self.blackhole.port,
             "profile": "blackhole"},
            {"id": "ep-cache-1", "node": "node-b", "weight": 0.6, "upstreams": [],
             "host": "127.0.0.1", "port": self.unlistened_port,
             "profile": "unlistened"},
            {"id": "ep-api-1", "node": "node-c", "weight": 0.4,
             "upstreams": ["ep-web-1", "ep-db-1"],
             "host": "127.0.0.1", "port": self.healthy_3.port,
             "profile": "healthy"},
        ]}

    def plan_dict(self):
        return {
            "plan_id": "plan-test",
            "version": 1,
            "rounds": 2,
            "fault_switches": {"connection_refused": True,
                               "connection_timeout": True,
                               "corrupt_body": True},
            "scheduler": {"global_concurrency": 2, "queue_capacity": 4,
                          "round_time_budget_ms": 5000,
                          "connect_timeout_ms": 250,
                          "cost_estimates_ms": {"connection_refused": 20,
                                                "connection_timeout": 300,
                                                "corrupt_body": 30}},
            "items": [
                {"id": "it-1", "target": "ep-cache-1",
                 "fault_type": "connection_refused",
                 "duration_rounds": 2, "priority": 10},
                {"id": "it-2", "target": "ep-db-1",
                 "fault_type": "connection_timeout",
                 "duration_rounds": 2, "priority": 8},
                {"id": "it-4", "target": "ep-web-2",
                 "fault_type": "connection_refused",
                 "duration_rounds": 1, "priority": 7},
                {"id": "it-3", "target": "ep-web-1",
                 "fault_type": "corrupt_body",
                 "duration_rounds": 1, "priority": 5},
            ],
        }

    def write_catalog(self, data=None):
        self.endpoints_path.write_text(json.dumps(data or self.catalog_dict()))

    def write_plan(self, data=None):
        plan = data if data is not None else self.plan_dict()
        self.plan_path.write_text(json.dumps(plan))


@pytest.fixture()
def env(tmp_path):
    environment = Environment(tmp_path)
    try:
        yield environment
    finally:
        environment.stop()


@pytest.fixture()
def engine(tmp_path):
    eng = Engine(tmp_path / "fiw.db")
    try:
        yield eng
    finally:
        eng.close()


@pytest.fixture()
def loaded_engine(env, engine):
    engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    return engine
