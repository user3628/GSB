from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from hotroute import samples
from hotroute.engine import Engine
from hotroute.errors import BusinessError, E_STALE_BASE


def test_two_concurrent_increments_same_base_allow_one_winner(engine: Engine):
    first = engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))
    base = first["state_version"]

    def submit(version: str):
        try:
            return ("ok", engine.submit_increment(samples.race_increment(base, version, 2)))
        except BusinessError as exc:
            return ("error", exc)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, ["race-a", "race-b"]))
    winners = [result for kind, result in results if kind == "ok"]
    losers = [result for kind, result in results if kind == "error"]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0].code == E_STALE_BASE
    state = engine.get_state()
    assert state["snapshot_seq"] == 2
    assert state["active_models"][samples.MODEL_ID]["version"] in {"race-a", "race-b"}

