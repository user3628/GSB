"""Deliberately terminate a process in the middle of a write.

This module is only used by the recovery demo/tests.  It performs a real
``os._exit`` while an uncommitted SQLite transaction contains partial snapshot
and derived rows.  The already-committed PENDING intent survives, allowing the
normal recovery path to prove that only committed facts are used.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path

from .codec import compute_snapshot_checksum, sha256_text, utc_now_iso
from .coordinator import Coordinator
from .models import validate_components
from .state_machine import DISPATCHED, PENDING


def _parse_config_set(value: str) -> dict:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise SystemExit("--config-set must be a JSON object")
    return parsed


def crash_during_rebuild(
    db_path: str,
    *,
    base_version: int,
    config_set: dict,
    generated_at: str,
) -> None:
    coordinator = Coordinator(db_path)
    with coordinator.store.read_transaction() as connection:
        current = coordinator.store.current_version(connection)
        previous = coordinator.store.load_snapshot(connection, current)
    if previous is None:
        raise SystemExit("no committed snapshot to patch")
    if current != base_version:
        raise SystemExit(f"base_version {base_version} is not current {current}")
    configs = copy.deepcopy(previous["configs"])
    configs.update(config_set)
    version = current + 1
    components = validate_components(previous["components"], configs)
    checksum = compute_snapshot_checksum(version, generated_at, configs)
    intent = coordinator.store.create_intent(
        "SUBMIT_INCREMENT",
        version,
        {
            "simulated_crash": True,
            "base_version": current,
            "config_set_keys": sorted(config_set),
            "pid": os.getpid(),
        },
    )
    with coordinator.store.transaction() as connection:
        coordinator._publish_version(  # noqa: SLF001 - intentional crash probe
            connection,
            intent_id=intent,
            version=version,
            base_version=current,
            generated_at=generated_at,
            configs=configs,
            components=components,
            checksum=checksum,
            source="INCREMENT",
            previous=previous,
        )
        # The transaction has several hundred bytes of snapshot/derived rows
        # and an intent-complete update, but has not committed.  Hard exit
        # bypasses Python cleanup and forces SQLite rollback on process death.
        print(
            json.dumps(
                {
                    "crash_mode": "rebuild",
                    "pid": os.getpid(),
                    "intent_id": intent,
                    "target_version": version,
                    "committed": False,
                }
            ),
            flush=True,
        )
        os._exit(91)


def crash_during_push(db_path: str, *, base_version: int) -> None:
    coordinator = Coordinator(db_path)
    with coordinator.store.read_transaction() as connection:
        current = coordinator.store.current_version(connection)
        snapshot = coordinator.store.load_snapshot(connection, current)
        states = coordinator.store.load_component_states(connection, current)
        existing_run = coordinator.store.get_dispatch_run(connection, current)
    if snapshot is None:
        raise SystemExit("no committed snapshot")
    if current != base_version:
        raise SystemExit(f"base_version {base_version} is not current {current}")
    if existing_run is not None:
        raise SystemExit("dispatch run already exists for current version")
    component = next(
        (item for item in snapshot["components"] if states[item.component_id]["status"] == PENDING),
        None,
    )
    if component is None:
        raise SystemExit("no PENDING component to dispatch")
    run_id = "run:crash-" + sha256_text(f"{current}:{component.component_id}").split(":", 1)[1][:20]
    request_id = sha256_text(f"{run_id}:{component.component_id}")
    push_intent = coordinator.store.create_intent(
        "PUSH",
        current,
        {
            "simulated_crash": True,
            "run_id": run_id,
            "component_id": component.component_id,
            "request_id": request_id,
        },
    )
    claim_intent = coordinator.store.create_intent(
        "DISPATCH_CLAIM", current, {"simulated_crash": True, "run_id": run_id}
    )
    with coordinator.store.transaction() as connection:
        coordinator.store.insert_dispatch_run(
            connection,
            version=current,
            run_id=run_id,
            policy="CONTINUE",
            plan={
                "snapshot_version": current,
                "affected_components": [component.component_id],
                "batches": [[component.component_id]],
                "simulated_crash": True,
            },
        )
        coordinator.store.update_component_state(
            connection,
            current,
            component.component_id,
            status=DISPATCHED,
            last_applied_digest=states[component.component_id].get("last_applied_digest"),
            last_applied_version=states[component.component_id].get("last_applied_version"),
            request_id=request_id,
        )
        coordinator.store.append_event(
            connection,
            version=current,
            component_id=component.component_id,
            from_status=PENDING,
            to_status=DISPATCHED,
            reason="PUSH_STARTED_BEFORE_FORCED_TERMINATION",
        )
        coordinator.store.complete_intent(connection, claim_intent)
    print(
        json.dumps(
            {
                "crash_mode": "push",
                "pid": os.getpid(),
                "push_intent_id": push_intent,
                "run_id": run_id,
                "component_id": component.component_id,
                "persisted_state": DISPATCHED,
                "committed": True,
            }
        ),
        flush=True,
    )
    os._exit(92)


def main() -> None:
    parser = argparse.ArgumentParser(description="Force-terminate a coordinator write")
    parser.add_argument("--db", required=True)
    parser.add_argument("--mode", choices=["rebuild", "push"], default="rebuild")
    parser.add_argument("--base-version", type=int, required=True)
    parser.add_argument("--config-set", default="{}")
    parser.add_argument("--generated-at", default=None)
    args = parser.parse_args()
    db_path = str(Path(args.db))
    if args.mode == "rebuild":
        crash_during_rebuild(
            db_path,
            base_version=args.base_version,
            config_set=_parse_config_set(args.config_set),
            generated_at=args.generated_at or utc_now_iso(),
        )
    else:
        crash_during_push(db_path, base_version=args.base_version)


if __name__ == "__main__":
    main()

