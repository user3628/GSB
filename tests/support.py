"""Shared test helpers."""
from __future__ import annotations

import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from wckpt import Config, Engine, Page  # noqa: E402

DATA = os.path.join(ROOT, "data")


def load_sample(name):
    with open(os.path.join(DATA, name), "r", encoding="utf-8") as fh:
        return json.load(fh)


def test_config(**overrides):
    base = load_sample("checkpoint_config.json")
    base.setdefault("sqlite_synchronous", "NORMAL")
    base.update(overrides)
    return Config.from_dict(base)


def clean(path):
    shutil.rmtree(path, ignore_errors=True)


def new_engine(path, **cfg_overrides):
    clean(path)
    engine = Engine(path, test_config(**cfg_overrides))
    engine.open()
    return engine


def feed(engine, records, upto=None):
    for rec in records[:upto]:
        engine.apply_modification(
            page_no=rec["page_no"],
            after_digest=rec["after_digest"],
            txn_id=rec["txn_id"],
        )


def expected_pages_after(records, upto=None):
    """Independently compute the expected page state from the WAL sequence."""
    state = {}
    version = {}
    last_lsn = {}
    for idx, rec in enumerate(records[:upto], start=1):
        p = rec["page_no"]
        state[p] = rec["after_digest"]
        version[p] = version.get(p, 0) + 1
        last_lsn[p] = idx
    return {
        p: {"digest": state[p], "version": version[p], "last_lsn": last_lsn[p]}
        for p in state
    }
