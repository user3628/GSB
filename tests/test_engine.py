from __future__ import annotations

import json
import multiprocessing
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

import numpy as np

from motion_engine.checkpoints import CheckpointStore
from motion_engine.errors import EngineError
from motion_engine.inputs import load_series_data
from motion_engine.manifest import load_manifest
from motion_engine.render import render_frame, summarize_frame
from motion_engine.runner import StopController, report_bytes
from motion_engine.sample_gen import generate_samples
from motion_engine.util import sha256_file
from motion_engine.workspace import Workspace

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

EXPECTED_REPORT_FINGERPRINTS = {
    "alpha_translate": "e3403c6b0d4b342290b8f55cf57479f7b341aec35d997012be0b2dbf8bc2f9d0",
    "beta_deform": "0d3baa53260f13480fb417dd00139f51199fe54a396505bbaee09a883196bf07",
    "gamma_turn": "865aebc7590de1bdbba9d8f56c5209ad70171b69c41ffc1fb7f1f903b4084c23",
}


def _copy_demo(destination: Path) -> Path:
    shutil.copytree(DATA, destination)
    return destination / "manifest.json"


def _manifest_json(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return json.load(handle)


def _write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _series(manifest: dict[str, Any], series_id: str) -> dict[str, Any]:
    return next(item for item in manifest["series"] if item["series_id"] == series_id)


def _publish_worker(root: str, record: dict[str, Any], start: Any, queue: Any) -> None:
    start.wait()
    try:
        CheckpointStore(root).append(record)
        queue.put("ok")
    except EngineError as exc:
        queue.put(exc.code)
    except Exception as exc:  # pragma: no cover - diagnostic path
        queue.put(f"EXCEPTION:{exc}")


class DemoAndCoreTests(unittest.TestCase):
    def test_sample_generator_is_reproducible(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = generate_samples(root / "first", seed=20261121)
            second = generate_samples(root / "second", seed=20261121)
            self.assertEqual(first, second)
            for filename in (
                "manifest.json",
                "fingerprints.json",
                "alpha_translate.npy",
                "beta_deform.npy",
                "gamma_turn.npy",
            ):
                self.assertEqual(
                    (root / "first" / filename).read_bytes(),
                    (root / "second" / filename).read_bytes(),
                    msg=filename,
                )

    def test_demo_fingerprints_and_manifest_contract(self) -> None:
        fingerprints = json.loads((DATA / "fingerprints.json").read_text(encoding="utf-8"))
        self.assertEqual(fingerprints["seed"], 20261121)
        self.assertEqual(
            fingerprints["manifest_file_sha256"],
            sha256_file(DATA / "manifest.json"),
        )
        self.assertGreaterEqual(len(fingerprints["series"]), 3)
        manifest = load_manifest(DATA / "manifest.json")
        self.assertEqual(len(manifest["series"]), 3)
        for expected in fingerprints["series"]:
            frame_path = DATA / expected["frame_file"]
            self.assertEqual(expected["file_sha256"], sha256_file(frame_path))
            frames = np.load(frame_path, allow_pickle=False)
            self.assertEqual(list(frames.shape), expected["shape"])
            self.assertEqual(frames.dtype, np.uint8)
            self.assertGreaterEqual(int(frames.min()), 0)
            self.assertLessEqual(int(frames.max()), 255)
        for series in manifest["series"]:
            self.assertIsNotNone(series["dynamics"]["direction_event"])
            self.assertEqual(
                len(series["expected_state_ranges"]), series["prediction_steps"]
            )

    def test_complete_runs_have_fixed_report_digests(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Workspace(Path(temporary) / "workspace")
            workspace.import_manifest(DATA / "manifest.json")
            for series_id, expected in EXPECTED_REPORT_FINGERPRINTS.items():
                report = workspace.run(series_id)
                self.assertTrue(report["complete"])
                self.assertTrue(report["run"]["complete"])
                self.assertEqual(report["report_fingerprint"], expected)
                self.assertGreater(report["aggregate"]["mean_mae"], 0.0)
                self.assertEqual(report["aggregate"]["clamp_count"], 0)
                self.assertEqual(
                    report["checkpoint"]["sequences"],
                    list(range(1, report["prediction_steps"] + 1)),
                )
                self.assertAlmostEqual(
                    report["aggregate"]["sum_mae"],
                    sum(report["error_curve"]),
                    places=15,
                )

    def test_state_clamping_is_recorded_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = _copy_demo(root / "data")
            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["expected_state_ranges"][0]["x"] = [-1000.0, 30.0]
            _write_manifest(manifest_path, manifest)

            runs: list[dict[str, Any]] = []
            for name in ("one", "two"):
                workspace = Workspace(root / name)
                workspace.import_manifest(manifest_path)
                runs.append(workspace.run("alpha_translate"))
            first, second = runs
            self.assertEqual(report_bytes(first), report_bytes(second))
            self.assertEqual(first["report_fingerprint"], second["report_fingerprint"])
            self.assertEqual(first["steps"][0]["state"]["x"], 30.0)
            self.assertTrue(first["clamp_events"])
            event = first["clamp_events"][0]
            self.assertEqual(event["step"], 1)
            self.assertEqual(event["component"], "x")
            self.assertEqual(event["clamped"], 30.0)
            self.assertGreater(event["requested"], event["clamped"])

    def test_renderer_is_byte_deterministic_and_uses_bilinear_warp(self) -> None:
        manifest = load_manifest(DATA / "manifest.json")
        series = manifest["series"][0]
        data = load_series_data(series)
        state = {
            "step": 3,
            "x": data.initial_state["x"] + 2.4,
            "y": data.initial_state["y"] - 1.1,
            "vx": 1.0,
            "vy": 0.2,
            "deformation": 0.03,
        }
        first = render_frame(data.frames[0], data.initial_state, state, data.render)
        second = render_frame(data.frames[0], data.initial_state, state, data.render)
        self.assertEqual(first.tobytes(order="C"), second.tobytes(order="C"))
        self.assertEqual(first.dtype, np.uint8)
        self.assertEqual(summarize_frame(first), summarize_frame(second))
        self.assertEqual(first.shape, data.frames.shape[1:])
        self.assertFalse(np.array_equal(first, data.frames[0]))


class StopRecoveryTests(unittest.TestCase):
    def test_stop_then_resume_is_byte_identical_to_complete_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            complete = Workspace(root / "complete")
            resumed = Workspace(root / "resumed")
            complete.import_manifest(DATA / "manifest.json")
            resumed.import_manifest(DATA / "manifest.json")

            full = complete.run("beta_deform")
            controller = StopController()

            def stop_after_five(record: dict[str, Any]) -> None:
                if record["step_index"] == 5:
                    controller.request()

            partial = resumed.run(
                "beta_deform", stop_controller=controller, on_step=stop_after_five
            )
            self.assertFalse(partial["complete"])
            self.assertTrue(partial["run"]["stopped"])
            self.assertEqual(partial["checkpoint"]["sequences"], [1, 2, 3, 4, 5])

            final = resumed.run("beta_deform")
            self.assertTrue(final["complete"])
            self.assertEqual(full["report_fingerprint"], final["report_fingerprint"])
            self.assertEqual(report_bytes(full), report_bytes(final))
            self.assertEqual(
                complete.report("beta_deform", canonical=True),
                resumed.report("beta_deform", canonical=True),
            )
            first_dir = complete.root / "checkpoints" / "beta_deform"
            second_dir = resumed.root / "checkpoints" / "beta_deform"
            first_files = sorted(path.name for path in first_dir.glob("*.json"))
            second_files = sorted(path.name for path in second_dir.glob("*.json"))
            self.assertEqual(first_files, second_files)
            for filename in first_files:
                self.assertEqual(
                    (first_dir / filename).read_bytes(),
                    (second_dir / filename).read_bytes(),
                    msg=filename,
                )
            self.assertEqual(
                sorted(int(value) for value in final["checkpoint"]["sequences"]),
                final["checkpoint"]["sequences"],
            )
            self.assertEqual(
                len(set(final["checkpoint"]["sequences"])),
                len(final["checkpoint"]["sequences"]),
            )

            repeated = resumed.run("beta_deform")
            self.assertEqual(repeated["run"]["performed_steps"], 0)
            self.assertEqual(repeated["report_fingerprint"], full["report_fingerprint"])

    def test_corrupt_incomplete_and_duplicate_records_are_ignored_and_repaired(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            reference = Workspace(root / "reference")
            damaged = Workspace(root / "damaged")
            reference.import_manifest(DATA / "manifest.json")
            damaged.import_manifest(DATA / "manifest.json")
            full = reference.run("alpha_translate")
            damaged.run("alpha_translate", max_steps=6)
            directory = damaged.root / "checkpoints" / "alpha_translate"
            (directory / "step-00000007.json").write_bytes(b'{"broken":')
            shutil.copyfile(
                directory / "step-00000004.json",
                directory / "duplicate-copy.json",
            )
            (directory / ".step-00000005.json.tmp-half").write_bytes(b"half record")

            inspected = damaged.inspect_series("alpha_translate")
            self.assertTrue(inspected["invalidated"])
            self.assertEqual(inspected["rebuild_from_step"], 4)
            self.assertIn(4, inspected["checkpoint"]["duplicate_sequences"])
            self.assertTrue(inspected["checkpoint"]["ignored_files"])

            repaired = damaged.run("alpha_translate")
            self.assertEqual(repaired["report_fingerprint"], full["report_fingerprint"])
            self.assertEqual(report_bytes(repaired), report_bytes(full))
            status = damaged.inspect_series("alpha_translate")
            self.assertFalse(status["invalidated"])
            self.assertEqual(status["checkpoint"]["duplicate_sequences"], [])
            self.assertEqual(status["checkpoint"]["ignored_files"], [])
            self.assertTrue((directory / ".step-00000005.json.tmp-half").exists())


class InvalidationTests(unittest.TestCase):
    def test_late_frame_change_rebuilds_only_step_and_downstream(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = _copy_demo(root / "data")
            workspace = Workspace(root / "workspace")
            workspace.import_manifest(manifest_path)
            workspace.run("alpha_translate")
            checkpoint_dir = workspace.root / "checkpoints" / "alpha_translate"
            before = {
                index: (checkpoint_dir / f"step-{index:08d}.json").read_bytes()
                for index in range(1, 17)
            }

            frame_path = manifest_path.parent / "alpha_translate.npy"
            frames = np.load(frame_path, allow_pickle=False)
            frames[5, 0, 0] = (int(frames[5, 0, 0]) + 17) % 256
            np.save(frame_path, frames, allow_pickle=False)

            status = workspace.inspect_series("alpha_translate")
            self.assertTrue(status["invalidated"])
            self.assertEqual(status["rebuild_from_step"], 5)
            self.assertEqual(status["invalidation_reasons"][0]["stage"], "truth_frame")
            self.assertNotEqual(
                status["version_fingerprints"]["current"],
                status["version_fingerprints"]["checkpoint"],
            )
            rebuilt = workspace.run("alpha_translate")
            for index in range(1, 5):
                self.assertEqual(
                    before[index],
                    (checkpoint_dir / f"step-{index:08d}.json").read_bytes(),
                    msg=f"step {index} should be reused",
                )
            for index in range(5, 17):
                self.assertNotEqual(
                    before[index],
                    (checkpoint_dir / f"step-{index:08d}.json").read_bytes(),
                    msg=f"step {index} should be rebuilt",
                )
            fresh = Workspace(root / "fresh")
            fresh.import_manifest(manifest_path)
            fresh_report = fresh.run("alpha_translate")
            self.assertEqual(report_bytes(rebuilt), report_bytes(fresh_report))

    def test_render_change_invalidates_only_changed_series(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = _copy_demo(root / "data")
            workspace = Workspace(root / "workspace")
            workspace.import_manifest(manifest_path)
            workspace.run("alpha_translate")
            workspace.run("beta_deform")
            beta_dir = workspace.root / "checkpoints" / "beta_deform"
            beta_before = {
                path.name: path.read_bytes() for path in beta_dir.glob("*.json")
            }

            manifest = _manifest_json(manifest_path)
            _series(manifest, "alpha_translate")["render"]["translation_gain"] = 1.1
            _write_manifest(manifest_path, manifest)
            workspace.import_manifest(manifest_path)

            alpha_status = workspace.inspect_series("alpha_translate")
            beta_status = workspace.inspect_series("beta_deform")
            self.assertEqual(alpha_status["rebuild_from_step"], 1)
            self.assertEqual(alpha_status["invalidation_reasons"][0]["stage"], "render")
            self.assertFalse(beta_status["invalidated"])
            self.assertEqual(beta_status["rebuild_from_step"], None)

            workspace.run("alpha_translate")
            beta_after = {
                path.name: path.read_bytes() for path in beta_dir.glob("*.json")
            }
            self.assertEqual(beta_before, beta_after)

    def test_initial_dynamics_timeline_and_prediction_count_invalidation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = _copy_demo(root / "data")
            workspace = Workspace(root / "workspace")
            workspace.import_manifest(manifest_path)
            workspace.run("alpha_translate")

            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["initial_state"]["x"] = float(alpha["initial_state"]["x"]) + 1.0
            _write_manifest(manifest_path, manifest)
            workspace.import_manifest(manifest_path)
            status = workspace.inspect_series("alpha_translate")
            self.assertEqual(status["rebuild_from_step"], 1)
            self.assertIn(
                "initial_state",
                {reason["stage"] for reason in status["invalidation_reasons"]},
            )
            workspace.run("alpha_translate")

            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["dynamics"]["acceleration_decay"] = 0.97
            _write_manifest(manifest_path, manifest)
            workspace.import_manifest(manifest_path)
            status = workspace.inspect_series("alpha_translate")
            self.assertEqual(status["rebuild_from_step"], 1)
            self.assertIn(
                "dynamics",
                {reason["stage"] for reason in status["invalidation_reasons"]},
            )
            workspace.run("alpha_translate")

            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["timeline"]["stride_frames"] = 2
            alpha["prediction_steps"] = 8
            alpha["expected_state_ranges"] = alpha["expected_state_ranges"][:8]
            _write_manifest(manifest_path, manifest)
            workspace.import_manifest(manifest_path)
            status = workspace.inspect_series("alpha_translate")
            self.assertEqual(status["rebuild_from_step"], 1)
            self.assertIn(
                "timeline",
                {reason["stage"] for reason in status["invalidation_reasons"]},
            )
            report = workspace.run("alpha_translate")
            self.assertTrue(report["complete"])
            self.assertEqual(report["prediction_steps"], 8)

    def test_prediction_count_increase_reuses_prefix_and_decrease_truncates_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = _copy_demo(root / "data")
            workspace = Workspace(root / "workspace")
            workspace.import_manifest(manifest_path)
            initial = workspace.run("alpha_translate")
            checkpoint_dir = workspace.root / "checkpoints" / "alpha_translate"
            before = {
                path.name: path.read_bytes() for path in checkpoint_dir.glob("*.json")
            }

            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["prediction_steps"] = 17
            alpha["expected_state_ranges"].append(
                dict(alpha["expected_state_ranges"][-1])
            )
            _write_manifest(manifest_path, manifest)
            workspace.import_manifest(manifest_path)
            status = workspace.inspect_series("alpha_translate")
            self.assertFalse(status["invalidated"])
            increased = workspace.run("alpha_translate")
            self.assertEqual(increased["prediction_steps"], 17)
            self.assertEqual(increased["run"]["started_at_step"], 17)
            for filename, content in before.items():
                self.assertEqual(
                    content, (checkpoint_dir / filename).read_bytes(), msg=filename
                )
            self.assertEqual(initial["checkpoint"]["sequences"], list(range(1, 17)))

            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["prediction_steps"] = 13
            alpha["expected_state_ranges"] = alpha["expected_state_ranges"][:13]
            _write_manifest(manifest_path, manifest)
            workspace.import_manifest(manifest_path)
            status = workspace.inspect_series("alpha_translate")
            self.assertTrue(status["invalidated"])
            self.assertEqual(status["rebuild_from_step"], 14)
            decreased = workspace.run("alpha_translate")
            self.assertEqual(decreased["prediction_steps"], 13)
            self.assertEqual(decreased["checkpoint"]["sequences"], list(range(1, 14)))
            self.assertEqual(
                sorted(path.name for path in checkpoint_dir.glob("*.json")),
                [f"step-{index:08d}.json" for index in range(1, 14)],
            )


class ErrorAndConcurrencyTests(unittest.TestCase):
    def test_timeline_overflow_and_frame_count_mismatch_have_stable_codes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = _copy_demo(root / "data")
            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["prediction_steps"] = 19
            alpha["expected_state_ranges"].extend(
                dict(alpha["expected_state_ranges"][-1]) for _ in range(3)
            )
            _write_manifest(manifest_path, manifest)
            with self.assertRaises(EngineError) as raised:
                Workspace(root / "overflow").import_manifest(manifest_path)
            self.assertEqual(raised.exception.code, "E_TIMELINE_OVERFLOW")
            self.assertEqual(raised.exception.context["series_id"], "alpha_translate")
            self.assertEqual(raised.exception.context["overflow_frames"], 1)

            manifest = _manifest_json(manifest_path)
            alpha = _series(manifest, "alpha_translate")
            alpha["prediction_steps"] = 16
            alpha["expected_state_ranges"] = alpha["expected_state_ranges"][:16]
            alpha["frame_count"] = 20
            _write_manifest(manifest_path, manifest)
            with self.assertRaises(EngineError) as raised:
                Workspace(root / "mismatch").import_manifest(manifest_path)
            self.assertEqual(raised.exception.code, "E_INVALID_STATE")
            self.assertEqual(raised.exception.context["series_id"], "alpha_translate")
            self.assertEqual(raised.exception.context["position"], "frame_count")

    def test_illegal_resume_step_has_invalid_state_code(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            workspace = Workspace(Path(temporary) / "workspace")
            workspace.import_manifest(DATA / "manifest.json")
            workspace.run("gamma_turn", max_steps=3)
            with self.assertRaises(EngineError) as raised:
                workspace.run("gamma_turn", from_step=99)
            self.assertEqual(raised.exception.code, "E_INVALID_STATE")
            self.assertEqual(raised.exception.context["series_id"], "gamma_turn")
            self.assertEqual(raised.exception.context["position"], "checkpoint_step:99")

            with self.assertRaises(EngineError) as raised:
                workspace.run("gamma_turn", from_step=1)
            self.assertEqual(raised.exception.code, "E_INVALID_STATE")
            self.assertEqual(raised.exception.context["latest_valid_step"], 3)

            step_two = workspace.root / "checkpoints" / "gamma_turn" / "step-00000002.json"
            step_two.unlink()
            with self.assertRaises(EngineError) as raised:
                workspace.run("gamma_turn", from_step=3)
            self.assertEqual(raised.exception.code, "E_INVALID_STATE")
            self.assertEqual(raised.exception.context["position"], "checkpoint_step:3")

    def test_stale_duplicate_and_cross_series_publish_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepared = Workspace(root / "prepared")
            prepared.import_manifest(DATA / "manifest.json")
            prepared.run("gamma_turn", max_steps=2)
            step_one_path = prepared.root / "checkpoints" / "gamma_turn" / "step-00000001.json"
            step_one = json.loads(step_one_path.read_text(encoding="utf-8"))

            target = root / "target"
            store = CheckpointStore(target)
            store.append(step_one)
            with self.assertRaises(EngineError) as raised:
                store.append(step_one)
            self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")
            with self.assertRaises(EngineError) as raised:
                store.append(step_one, expected_series_id="beta_deform")
            self.assertEqual(raised.exception.code, "E_STALE_CHECKPOINT")

    def test_file_lock_allows_exactly_one_concurrent_publisher(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            prepared = Workspace(root / "prepared")
            prepared.import_manifest(DATA / "manifest.json")
            prepared.run("gamma_turn", max_steps=1)
            record_path = prepared.root / "checkpoints" / "gamma_turn" / "step-00000001.json"
            record = json.loads(record_path.read_text(encoding="utf-8"))
            target = root / "race"

            context = multiprocessing.get_context("fork")
            start = context.Event()
            queue = context.Queue()
            processes = [
                context.Process(
                    target=_publish_worker, args=(str(target), record, start, queue)
                )
                for _ in range(2)
            ]
            for process in processes:
                process.start()
            start.set()
            for process in processes:
                process.join(timeout=10)
                self.assertEqual(process.exitcode, 0)
            results = sorted(queue.get(timeout=2) for _ in range(2))
            self.assertEqual(results, ["E_STALE_CHECKPOINT", "ok"])
            scan = CheckpointStore(target).scan("gamma_turn")
            self.assertEqual(scan.last_step, 1)
            self.assertEqual(len(scan.prefix), 1)


if __name__ == "__main__":
    unittest.main()
