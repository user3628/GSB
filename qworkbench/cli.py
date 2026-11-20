from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import platform
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence

from . import ALGORITHM_VERSION, __version__
from .config import WorkbenchConfig
from .errors import BusinessError
from .fingerprints import STAGES
from .generator import generate_demo
from .locking import lock_backend, probe_lock, probe_lock_contention
from .pipeline import Pipeline
from .samples import (
    SampleDataset,
    discover_data_dir,
    load_imported_spec,
    read_jsonl,
    write_imported_dataset,
)
from .store import CheckpointStore
from .util import atomic_write_bytes


def _json_dump(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _resolve_dataset(args: argparse.Namespace) -> SampleDataset:
    workdir = Path(args.workdir)
    if args.data_dir:
        data_dir = Path(args.data_dir)
        files, discovered_annotation = discover_data_dir(data_dir)
        annotation = Path(args.annotation_file) if args.annotation_file else discovered_annotation
        return SampleDataset.load(files, annotation)
    imported = load_imported_spec(workdir)
    if imported is not None:
        files, annotation = imported
        if args.annotation_file:
            annotation = Path(args.annotation_file)
        return SampleDataset.load(files, annotation)
    default_data = Path(args.default_data_dir)
    files, discovered_annotation = discover_data_dir(default_data)
    annotation = Path(args.annotation_file) if args.annotation_file else discovered_annotation
    return SampleDataset.load(files, annotation)


def _config(args: argparse.Namespace) -> WorkbenchConfig:
    return WorkbenchConfig.load(Path(args.config))


def _pipeline(args: argparse.Namespace, **kwargs: Any) -> Pipeline:
    return Pipeline(
        _config(args),
        _resolve_dataset(args),
        Path(args.workdir),
        **kwargs,
    )


def cmd_generate_demo(args: argparse.Namespace) -> dict[str, Any]:
    meta = generate_demo(Path(args.data_dir), seed=args.seed, shards=args.shards)
    return {"ok": True, "generated": meta, "data_dir": str(Path(args.data_dir).resolve())}


def cmd_run(args: argparse.Namespace) -> dict[str, Any]:
    pipeline = _pipeline(
        args,
        stop_after_units=args.stop_after_units,
        stop_after_stage=args.stop_after_stage,
    )
    return pipeline.run(start_stage=args.from_stage)


def cmd_resume(args: argparse.Namespace) -> dict[str, Any]:
    return _pipeline(args).resume()


def cmd_stop(args: argparse.Namespace) -> dict[str, Any]:
    store = CheckpointStore(Path(args.workdir))
    return {"ok": True, "stop": store.request_stop(args.reason)}


def cmd_status(args: argparse.Namespace) -> dict[str, Any]:
    return _pipeline(args).status()


def cmd_diagnostics(args: argparse.Namespace) -> dict[str, Any]:
    return {"ok": True, "diagnostics": _pipeline(args).diagnostics()}


def cmd_allocation(args: argparse.Namespace) -> dict[str, Any]:
    return {"ok": True, "allocation": _pipeline(args).allocation()}


def cmd_consistency(args: argparse.Namespace) -> dict[str, Any]:
    return {"ok": True, "consistency": _pipeline(args).consistency()}


def cmd_manifest(args: argparse.Namespace) -> dict[str, Any]:
    return {"ok": True, "manifest": _pipeline(args).final_manifest()}


def cmd_export(args: argparse.Namespace) -> dict[str, Any]:
    pipeline = _pipeline(args)
    source = pipeline.export_path()
    target = Path(args.output)
    data = source.read_bytes()
    if str(target) == "-":
        sys.stdout.buffer.write(data)
        return {"ok": True, "exported_to": "stdout", "bytes": len(data)}
    atomic_write_bytes(target, data)
    return {
        "ok": True,
        "exported_to": str(target.resolve()),
        "bytes": len(data),
        "sha256": pipeline.final_manifest()["mixture_sha256"],
    }


def cmd_import_jsonl(args: argparse.Namespace) -> dict[str, Any]:
    samples: list[dict[str, Any]] = []
    for raw_path in args.samples:
        samples.extend(read_jsonl(Path(raw_path)))
    annotations: list[dict[str, Any]] = []
    if args.annotations:
        annotations = read_jsonl(Path(args.annotations))
    result = write_imported_dataset(Path(args.workdir), samples, annotations or None)
    return {"ok": True, "import": result}


def _package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "not-installed"


def _linux_fs_type(path: Path) -> str | None:
    if os.name == "nt":  # pragma: no cover - Windows branch
        return None
    mounts = Path("/proc/mounts")
    if not mounts.exists():
        return None
    resolved = path.resolve()
    best_mount = None
    best_type = None
    for line in mounts.read_text(encoding="utf-8", errors="replace").splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        mount_point = fields[1].replace("\\040", " ")
        try:
            resolved.relative_to(mount_point)
        except ValueError:
            continue
        if best_mount is None or len(mount_point) > len(best_mount):
            best_mount = mount_point
            best_type = fields[2]
    return best_type


def cmd_doctor(args: argparse.Namespace) -> dict[str, Any]:
    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    probe = probe_lock(workdir / "locks" / "doctor-probe.lock")
    contention_probe = probe_lock_contention(workdir / "locks" / "doctor-contention.lock")
    windows_probe = None
    windows_probe_attempts: list[dict[str, Any]] = []
    for candidate in (Path("/mnt/c/Users/Public"), Path("/mnt/c/Windows/Temp"), Path("/mnt/c")):
        if not candidate.exists() or not os.access(candidate, os.W_OK):
            continue
        probe_path: Path | None = None
        try:
            fd, tmp_name = tempfile.mkstemp(prefix=".iqw-lock-probe-", dir=candidate)
            os.close(fd)
            probe_path = Path(tmp_name)
            windows_probe = probe_lock(probe_path)
            contention_path = candidate / ".iqw-lock-contention-probe"
            windows_contention = probe_lock_contention(contention_path)
            contention_path.unlink(missing_ok=True)
            windows_probe["filesystem"] = _linux_fs_type(probe_path)
            windows_probe["contention_probe"] = windows_contention
            windows_probe["note"] = (
                "WSL2 DrvFs 9p mount. The POSIX branch uses fcntl.flock; the Windows "
                "msvcrt branch remains compiled only when the host Python runs on Windows."
            )
            windows_probe_attempts.append({"path": str(candidate), "ok": True})
            break
        except OSError as exc:
            windows_probe_attempts.append({"path": str(candidate), "ok": False, "error": str(exc)})
        finally:
            if probe_path is not None:
                probe_path.unlink(missing_ok=True)
    report = {
        "ok": True,
        "workbench_version": __version__,
        "algorithm_version": ALGORITHM_VERSION,
        "python": sys.version.split()[0],
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "os_name": os.name,
        "packages": {
            name: _package_version(name)
            for name in ("numpy", "fastapi", "uvicorn", "pydantic", "pytest", "httpx")
        },
        "workspace": {
            "path": str(workdir),
            "filesystem": _linux_fs_type(workdir),
        },
        "lock_backend": lock_backend(),
        "lock_probe": probe,
        "lock_contention_probe": contention_probe,
        "windows_filesystem_lock_probe": windows_probe,
        "windows_filesystem_probe_attempts": windows_probe_attempts,
        "note": (
            "The current session is WSL2/Linux. Windows-native msvcrt.locking cannot execute "
            "in this Python process; the Windows branch is provided in qworkbench.locking and "
            "the writable DrvFs probe verifies advisory locking on the mounted C: filesystem."
        ),
    }
    output = Path(args.output) if args.output else workdir / "environment.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report["environment_file"] = str(output)
    return report


def cmd_serve(args: argparse.Namespace) -> dict[str, Any]:
    import uvicorn

    from .api import Settings, create_app

    app = create_app(
        Settings(
            config_path=Path(args.config),
            data_dir=Path(args.data_dir) if args.data_dir else None,
            annotation_file=Path(args.annotation_file) if args.annotation_file else None,
            workdir=Path(args.workdir),
            default_data_dir=Path(args.default_data_dir),
        )
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    return {"ok": True, "served": True}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="qwb",
        description="Deterministic instruction-data quality and diagnostics workbench",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_workbench_args(sub: argparse.ArgumentParser, *, dataset: bool = True) -> None:
        sub.add_argument("--config", default="configs/demo.json")
        sub.add_argument("--workdir", default=".workbench")
        if dataset:
            sub.add_argument("--data-dir", default=None)
            sub.add_argument("--annotation-file", default=None)
            sub.add_argument("--default-data-dir", default="data/demo")

    gen = subparsers.add_parser("generate-demo", help="generate deterministic demo JSONL shards")
    gen.add_argument("--data-dir", default="data/demo")
    gen.add_argument("--seed", type=int, default=20261120)
    gen.add_argument("--shards", type=int, default=4)
    gen.set_defaults(func=cmd_generate_demo)

    run = subparsers.add_parser("run", help="run or incrementally resume the pipeline")
    add_workbench_args(run)
    run.add_argument("--from-stage", choices=STAGES, default="normalize")
    run.add_argument("--stop-after-stage", choices=STAGES, default=None)
    run.add_argument("--stop-after-units", type=int, default=None)
    run.set_defaults(func=cmd_run)

    resume = subparsers.add_parser("resume", help="clear a stop request and resume")
    add_workbench_args(resume)
    resume.set_defaults(func=cmd_resume)

    stop = subparsers.add_parser("stop", help="write a graceful stop request")
    add_workbench_args(stop, dataset=False)
    stop.add_argument("--reason", default="cli_operator_request")
    stop.set_defaults(func=cmd_stop)

    for name, func, help_text in (
        ("status", cmd_status, "show stage status and fingerprints"),
        ("diagnostics", cmd_diagnostics, "show numeric error-propagation diagnostics"),
        ("allocation", cmd_allocation, "show stratified allocation details"),
        ("consistency", cmd_consistency, "show annotation consistency and review flags"),
        ("manifest", cmd_manifest, "show the final mixture manifest"),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        add_workbench_args(sub)
        sub.set_defaults(func=func)

    export = subparsers.add_parser("export", help="export the final mixture JSONL")
    add_workbench_args(export)
    export.add_argument("--output", default="mixture.jsonl")
    export.set_defaults(func=cmd_export)

    import_cmd = subparsers.add_parser("import-jsonl", help="import local JSONL shards into the workdir")
    import_cmd.add_argument("--workdir", default=".workbench")
    import_cmd.add_argument("--samples", nargs="+", required=True)
    import_cmd.add_argument("--annotations", default=None)
    import_cmd.set_defaults(func=cmd_import_jsonl)

    doctor = subparsers.add_parser("doctor", help="record environment versions and test file locks")
    doctor.add_argument("--workdir", default=".workbench")
    doctor.add_argument("--output", default=None)
    doctor.set_defaults(func=cmd_doctor)

    serve = subparsers.add_parser("serve", help="serve the local HTTP diagnostic workbench")
    add_workbench_args(serve)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8017)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=cmd_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = args.func(args)
    except BusinessError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    except (ValueError, OSError) as exc:
        payload = {
            "ok": False,
            "code": "E_INVALID_INPUT",
            "message": str(exc),
            "details": {},
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2
    _json_dump(result)
    return 0
