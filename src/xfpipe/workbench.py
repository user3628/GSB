"""Workbench: shared wiring between the pipeline, cache and query engine."""
from __future__ import annotations

import os

from .cache import QueryCache
from .config import load_config
from .pipeline import Pipeline
from .query import QueryEngine


class Workbench:
    def __init__(self, workdir: str) -> None:
        self.workdir = os.path.abspath(workdir)
        self.data_dir = os.path.join(self.workdir, "data")
        self.sample_path = os.path.join(self.data_dir, "sample.csv")
        self.config_path = os.path.join(self.data_dir, "config.json")
        self._reload()

    def _reload(self) -> None:
        self.config = load_config(self.config_path)
        self.pipeline = Pipeline(self.workdir, self.sample_path, self.config)
        self.cache = QueryCache(int(self.config["cache"]["capacity"]))
        self.query_engine = QueryEngine(
            self.pipeline, self.cache, self.config["query"]["latency_budget_ms"]
        )

    def update_config(self, new_config: dict) -> None:
        import json

        from .config import validate_config

        validate_config(new_config)
        with open(self.config_path, "w", encoding="utf-8") as fh:
            json.dump(new_config, fh, indent=2, sort_keys=True)
            fh.write("\n")
        self._reload()

    def import_sample(self, source_path: str) -> str:
        import shutil

        os.makedirs(self.data_dir, exist_ok=True)
        shutil.copyfile(source_path, self.sample_path)
        self._reload()
        return self.sample_path

    def cache_stats(self) -> dict:
        return self.cache.stats()

    def latency_traces(self) -> list[dict]:
        return list(self.query_engine.traces)
