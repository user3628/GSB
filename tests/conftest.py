import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from xfpipe.pipeline import Pipeline
from xfpipe.sample_gen import default_config, generate_sample_csv
from xfpipe.workbench import Workbench


@pytest.fixture()
def workdir(tmp_path):
    return str(tmp_path / "wb")


@pytest.fixture()
def sample_path(workdir):
    return generate_sample_csv(os.path.join(workdir, "data", "sample.csv"))


@pytest.fixture()
def config(workdir):
    cfg = default_config()
    path = os.path.join(workdir, "data", "config.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(cfg, fh)
    return cfg


@pytest.fixture()
def bench(workdir, sample_path, config):
    return Workbench(workdir)


@pytest.fixture()
def ran_pipeline(bench):
    report = bench.pipeline.run()
    assert report["status"] == "completed"
    return bench


def write_config(workdir: str, cfg: dict) -> None:
    with open(os.path.join(workdir, "data", "config.json"), "w") as fh:
        json.dump(cfg, fh, indent=2, sort_keys=True)


def read_config(workdir: str) -> dict:
    with open(os.path.join(workdir, "data", "config.json")) as fh:
        return json.load(fh)


def fresh_pipeline(workdir: str) -> Pipeline:
    return Pipeline(workdir, os.path.join(workdir, "data", "sample.csv"),
                    read_config(workdir))
