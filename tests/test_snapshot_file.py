from __future__ import annotations

from hotroute import samples
from hotroute.cli import main
from hotroute.engine import Engine


def test_snapshot_file_cli_round_trip(tmp_path, db_path, capsys):
    payload = samples.initial_snapshot(snapshot_seq=1)
    path = samples.write_snapshot_file(tmp_path / "snapshot.json", payload)
    loaded = samples.read_snapshot_file(path)
    assert loaded["snapshot_seq"] == 1
    assert loaded["generated_at"] == payload["generated_at"]
    assert main(["snapshot-load", "--db", db_path, "--file", str(path)]) == 0
    output = capsys.readouterr().out
    assert '"snapshot_seq": 1' in output
    state = Engine(db_path, recover=False).get_state()
    assert state["snapshot_seq"] == 1

