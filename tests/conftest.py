import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from legal_cite.sample_generator import generate


@pytest.fixture(scope="session")
def sample_data(tmp_path_factory):
    out = tmp_path_factory.mktemp("sample")
    info = generate(out)
    return info


@pytest.fixture(scope="session")
def all_records(sample_data):
    from legal_cite.serde import read_jsonl
    records = []
    for shard in sample_data["shards"]:
        records.extend(read_jsonl(shard))
    return records


@pytest.fixture(scope="session")
def gold(sample_data):
    from legal_cite.serde import read_jsonl
    return read_jsonl(sample_data["gold"])
