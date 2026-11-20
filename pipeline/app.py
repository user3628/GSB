"""Interactive CLI for the data pipeline.

Usage:
    python -m pipeline.app --workspace WS <command> [args]
    python -m pipeline.app --workspace WS          # interactive REPL

Commands:
    import <file.jsonl> [name]   import a JSONL shard into data/shards/
    submit [json-line]           append sample(s) to shard_user.jsonl
    run [--target STAGE]         run incomplete stages up to target
    resume                       resume from the latest consistent checkpoint
    run-stage STAGE              run exactly one stage (upstream required)
    status [--json]              show stage states and version fingerprints
    stats                        show mix statistics
    stop                         request a graceful stop
    clearstop                    clear a previous stop request
    reset                        wipe state/ and artifacts/ (data kept)
    help                         show this help
    quit                         exit the REPL
"""

import argparse
import json
import os
import shutil
import sys

from .errors import E_INVALID_STAGE, PipelineError
from .fsutil import read_json_file, short
from .runner import EXIT_STOPPED, Runner, StopRequested, _install_signal_handlers
from .stages import REQUIRED_FIELDS, STAGES, artifacts_dir, shards_dir

HELP = __doc__


def _runner(args):
    return Runner(args.workspace)


def cmd_import(args):
    src = args.file
    if not os.path.exists(src):
        raise PipelineError("E_INPUT", "file not found: %s" % src)
    name = args.name or os.path.basename(src)
    if not name.endswith(".jsonl"):
        name += ".jsonl"
    dest_dir = shards_dir(args.workspace)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, name)
    shutil.copyfile(src, dest)
    print("[import] %s -> %s" % (src, dest))
    print("[import] shards version changed; affected stages will be "
          "marked invalidated on next status/run")


def _append_samples(workspace, lines):
    dest_dir = shards_dir(workspace)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "shard_user.jsonl")
    added = 0
    with open(dest, "a", encoding="utf-8") as fh:
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PipelineError(
                    "E_INPUT", "invalid JSON sample: %s" % exc)
            missing = [f for f in REQUIRED_FIELDS if f not in obj]
            if missing:
                raise PipelineError(
                    "E_INPUT", "sample missing fields: %s" % ",".join(missing))
            fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            added += 1
    return dest, added


def cmd_submit(args):
    if args.json:
        lines = [args.json]
    else:
        print("Enter JSON samples, one per line; empty line to finish:")
        lines = []
        while True:
            try:
                line = input("sample> ")
            except EOFError:
                break
            if not line.strip():
                break
            lines.append(line)
    dest, added = _append_samples(args.workspace, lines)
    print("[submit] appended %d sample(s) to %s" % (added, dest))
    print("[submit] shards version changed; run 'status' to see "
          "invalidated stages")


def cmd_run(args):
    runner = _runner(args)
    runner.run(target=args.target)
    print("[done] pipeline up to '%s' completed" % args.target)


def cmd_resume(args):
    runner = _runner(args)
    if os.path.exists(runner.stop_file):
        print("[resume] a stop request is still pending; "
              "run 'clearstop' first")
        return
    print("[resume] resuming from latest consistent checkpoint ...")
    runner.run(target="mix")
    print("[done] resume finished")


def cmd_run_stage(args):
    runner = _runner(args)
    runner.run_single_stage(args.stage)
    print("[done] stage '%s' completed" % args.stage)


def _status_payload(args):
    # Keep stdout machine-clean for --json: refresh logs go to stderr.
    runner = Runner(args.workspace,
                    log=lambda *a: print(*a, file=sys.stderr))
    statuses = runner.refresh()
    return runner, [s.as_dict() for s in statuses]


def cmd_status(args):
    runner, statuses = _status_payload(args)
    if args.json:
        print(json.dumps(statuses, indent=2, ensure_ascii=False))
        return
    print("%-10s %-12s %-4s %-14s %-14s %s"
          % ("STAGE", "STATE", "SEQ", "ARTIFACT", "UPSTREAM", "REASON"))
    for s in statuses:
        versions = s["input_versions"] or {}
        print("%-10s %-12s %-4s %-14s %-14s %s" % (
            s["stage"], s["state"],
            s["seq"] if s["seq"] is not None else "-",
            short(s["artifact_digest"]),
            short(versions.get("upstream")),
            s["reason"] or ""))
    print("\ninput versions (current):")
    if statuses:
        for key, value in sorted((statuses[0]["input_versions"] or {}).items()):
            if key != "upstream":
                print("  %-14s %s" % (key, value))


def cmd_stats(args):
    stats_path = os.path.join(artifacts_dir(args.workspace),
                              "mix.stats.json")
    if not os.path.exists(stats_path):
        raise PipelineError(
            E_INVALID_STAGE,
            "mix artifact is not available: stage 'mix' has not completed; "
            "run the pipeline first")
    stats = read_json_file(stats_path)
    print("mix statistics (seed=%s)" % stats.get("seed"))
    print("%-14s %6s %9s %6s %10s %10s" % (
        "LAYER", "WEIGHT", "AVAILABLE", "ALLOC", "SEL_TOKENS", "AVAIL_TOK"))
    for layer in sorted(stats["layers"]):
        row = stats["layers"][layer]
        print("%-14s %6.1f %9d %6d %10d %10d" % (
            layer, row["weight"], row["available"], row["allocated"],
            row["selected_tokens"], row["available_tokens"]))
    print("total: selected=%d requested=%d tokens=%d" % (
        stats["total_selected"], stats["total_requested"],
        stats["total_selected_tokens"]))


def cmd_stop(args):
    runner = _runner(args)
    runner.request_stop()
    print("[stop] stop requested; a running pipeline will finish its "
          "current unit and exit")


def cmd_clearstop(args):
    runner = _runner(args)
    runner.clear_stop()
    print("[stop] stop request cleared")


def cmd_reset(args):
    state_dir = os.path.join(args.workspace, "state")
    if os.path.isdir(state_dir):
        shutil.rmtree(state_dir)
    # Remove only pipeline-produced artifacts; leave foreign files alone.
    known = ("normalize.jsonl", "dedup.jsonl", "dedup.report.json",
             "mix.jsonl", "mix.stats.json")
    out_dir = artifacts_dir(args.workspace)
    removed = 0
    if os.path.isdir(out_dir):
        for name in os.listdir(out_dir):
            if name in known or ".tmp." in name:
                os.remove(os.path.join(out_dir, name))
                removed += 1
    print("[reset] state/ removed and %d artifact file(s) cleaned; "
          "data/ kept" % removed)


COMMANDS = {
    "import": cmd_import,
    "submit": cmd_submit,
    "run": cmd_run,
    "resume": cmd_resume,
    "run-stage": cmd_run_stage,
    "status": cmd_status,
    "stats": cmd_stats,
    "stop": cmd_stop,
    "clearstop": cmd_clearstop,
    "reset": cmd_reset,
}


def _namespace(workspace, command, rest):
    ns = argparse.Namespace(workspace=workspace, command=command,
                            target="mix", stage=None, file=None,
                            name=None, json=False)
    positional = []
    it = iter(rest)
    for token in it:
        if token == "--target":
            ns.target = next(it)
        elif token == "--json":
            ns.json = True
        elif token == "--name":
            ns.name = next(it)
        else:
            positional.append(token)
    if command == "run-stage" and positional:
        ns.stage = positional.pop(0)
    elif command == "import" and positional:
        ns.file = positional.pop(0)
        if positional:
            ns.name = positional.pop(0)
    elif command == "submit" and positional:
        ns.json = positional.pop(0)
    return ns


def dispatch(workspace, command, rest):
    if command in ("help", "-h", "--help"):
        print(HELP)
        return 0
    if command not in COMMANDS:
        raise PipelineError(
            E_INVALID_STAGE,
            "unknown command '%s'; try 'help'" % command)
    ns = _namespace(workspace, command, rest)
    if command == "run-stage" and not ns.stage:
        raise PipelineError(E_INVALID_STAGE,
                            "run-stage requires a stage name")
    if command == "import" and not ns.file:
        raise PipelineError("E_INPUT", "import requires a file path")
    COMMANDS[command](ns)
    return 0


def repl(workspace):
    print("LLM data pipeline console. Type 'help' for commands, "
          "'quit' to exit.")
    while True:
        try:
            line = input("pipeline> ").strip()
        except EOFError:
            print()
            break
        if not line:
            continue
        parts = line.split(None, 1)
        command = parts[0]
        if command == "submit" and len(parts) > 1:
            rest = [parts[1]]  # keep the JSON sample as one argument
        else:
            rest = parts[1].split() if len(parts) > 1 else []
        if command in ("quit", "exit"):
            break
        try:
            dispatch(workspace, command, rest)
        except PipelineError as exc:
            print("ERROR %s: %s" % (exc.code, exc.message))
        except StopRequested as exc:
            print("STOPPED: %s" % exc)


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    workspace = "."
    if "--workspace" in argv:
        idx = argv.index("--workspace")
        workspace = argv[idx + 1]
        del argv[idx:idx + 2]
    _install_signal_handlers()
    if not argv:
        repl(workspace)
        return 0
    command, rest = argv[0], argv[1:]
    try:
        return dispatch(workspace, command, rest)
    except PipelineError as exc:
        print("ERROR %s: %s" % (exc.code, exc.message))
        return 2
    except StopRequested as exc:
        print("STOPPED: %s" % exc)
        return EXIT_STOPPED


if __name__ == "__main__":
    sys.exit(main())
