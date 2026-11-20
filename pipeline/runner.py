"""Pipeline runner: validity planning, stop/resume, atomic stage publish."""

import os

from .errors import E_INVALID_STAGE, PipelineError
from .fsutil import atomic_write_bytes, short
from .stages import (RELEVANT_INPUTS, STAGE_FUNCS, STAGES,
                     artifacts_dir, compute_versions)
from .store import CheckpointStore

STOP_FILENAME = "STOP"
STOP_ENV_VAR = "PIPELINE_STOP_AFTER"
EXIT_STOPPED = 75

_signal_stop_requested = False


def _install_signal_handlers():
    """SIGINT/SIGTERM request a graceful stop (finish current unit)."""
    import signal

    def handler(signum, frame):
        global _signal_stop_requested
        _signal_stop_requested = True

    for name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass


class StopRequested(Exception):
    def __init__(self, where):
        super().__init__("stop requested %s" % where)
        self.where = where


class StageStatus:
    def __init__(self, stage, state, seq=None, input_versions=None,
                 artifact_digest=None, reason=None):
        self.stage = stage
        self.state = state  # pending | completed | invalidated
        self.seq = seq
        self.input_versions = input_versions or {}
        self.artifact_digest = artifact_digest
        self.reason = reason

    def as_dict(self):
        return {
            "stage": self.stage,
            "state": self.state,
            "seq": self.seq,
            "input_versions": self.input_versions,
            "artifact_digest": self.artifact_digest,
            "reason": self.reason,
        }


class Runner:
    def __init__(self, workspace, log=print):
        self.workspace = os.path.abspath(workspace)
        self.store = CheckpointStore(self.workspace)
        self.log = log
        os.makedirs(artifacts_dir(self.workspace), exist_ok=True)

    @property
    def stop_file(self):
        return os.path.join(self.workspace, "state", STOP_FILENAME)

    def request_stop(self):
        os.makedirs(os.path.dirname(self.stop_file), exist_ok=True)
        with open(self.stop_file, "w", encoding="utf-8") as fh:
            fh.write("stop requested\n")

    def clear_stop(self):
        if os.path.exists(self.stop_file):
            os.remove(self.stop_file)

    def stop_requested(self):
        return _signal_stop_requested or os.path.exists(self.stop_file)

    def _check_stop(self, where):
        if self.stop_requested():
            raise StopRequested(where)

    def plan(self):
        """Recompute per-stage validity from current input versions."""
        versions = compute_versions(self.workspace)
        statuses = []
        upstream_valid = True
        upstream_digest = None
        upstream_name = None
        for stage in STAGES:
            record = self.store.latest_checkpoint(stage)
            if record is None:
                statuses.append(StageStatus(stage, "pending",
                                            input_versions=versions))
                upstream_valid = False
                upstream_digest = None
                upstream_name = stage
                continue
            if not upstream_valid:
                statuses.append(StageStatus(
                    stage, "invalidated", seq=record["seq"],
                    input_versions=record["input_versions"],
                    artifact_digest=record["artifact_digest"],
                    reason="upstream stage '%s' is not valid" % upstream_name))
                continue
            reasons = []
            for key in RELEVANT_INPUTS[stage]:
                current = upstream_digest if key == "upstream" \
                    else versions.get(key)
                recorded = record["input_versions"].get(key)
                if recorded != current:
                    reasons.append("%s version changed (%s -> %s)" % (
                        key, short(recorded), short(current)))
            if reasons:
                statuses.append(StageStatus(
                    stage, "invalidated", seq=record["seq"],
                    input_versions=record["input_versions"],
                    artifact_digest=record["artifact_digest"],
                    reason="; ".join(reasons)))
                upstream_valid = False
                upstream_digest = None
                upstream_name = stage
            else:
                statuses.append(StageStatus(
                    stage, "completed", seq=record["seq"],
                    input_versions=record["input_versions"],
                    artifact_digest=record["artifact_digest"]))
                upstream_digest = record["artifact_digest"]
                upstream_name = stage
        return statuses

    def refresh(self):
        """Persist freshly detected invalidations so they are observable."""
        statuses = self.plan()
        stored = self.store.stage_states()
        for status in statuses:
            if status.state == "invalidated":
                current = stored.get(status.stage, {})
                if not (current.get("state") == "invalidated" and
                        current.get("detail", {}).get("reason")
                        == status.reason):
                    self.store.mark_invalidated(status.stage, status.reason)
                    self.log("[invalidate] stage '%s' marked invalidated: %s"
                             % (status.stage, status.reason))
        return statuses

    def _execute_stage(self, stage):
        self._check_stop("before stage '%s'" % stage)
        self.store.set_stage_state(stage, "running")
        self.log("[run] stage '%s' computing ..." % stage)
        result = STAGE_FUNCS[stage](self.workspace)
        self._check_stop("during stage '%s'" % stage)
        out_dir = artifacts_dir(self.workspace)
        primary_path = os.path.join(out_dir, result.primary_name)
        atomic_write_bytes(primary_path, result.primary_bytes)
        for name, data in result.extra_files.items():
            atomic_write_bytes(os.path.join(out_dir, name), data)
        versions = compute_versions(self.workspace)
        upstream_digest = None
        idx = STAGES.index(stage)
        if idx > 0:
            upstream = self.store.latest_checkpoint(STAGES[idx - 1])
            upstream_digest = upstream["artifact_digest"] if upstream else None
        input_versions = dict(versions)
        input_versions["upstream"] = upstream_digest
        expected = self.store.current_seq()
        seq = self.store.publish(
            stage, input_versions, result.digest,
            os.path.relpath(primary_path, self.workspace), expected)
        self.log("[publish] stage '%s' seq=%d artifact=%s (%s)"
                 % (stage, seq, result.primary_name,
                    short(result.digest)))
        return seq, result

    def run(self, target="mix", stop_after=None):
        """Run every incomplete stage up to `target` (inclusive)."""
        if target not in STAGES:
            raise PipelineError(
                E_INVALID_STAGE,
                "unknown stage '%s'; valid stages: %s"
                % (target, ", ".join(STAGES)))
        statuses = self.refresh()
        target_idx = STAGES.index(target)
        todo = [i for i, s in enumerate(statuses)
                if s.state != "completed" and i <= target_idx]
        if not todo:
            self.log("[ok] all stages up to '%s' are completed; "
                     "nothing to do" % target)
            return statuses
        for i in range(0, target_idx + 1):
            stage = STAGES[i]
            if statuses[i].state == "completed":
                self.log("[reuse] stage '%s' already completed (seq=%d, "
                         "artifact=%s); skipping recompute"
                         % (stage, statuses[i].seq,
                            short(statuses[i].artifact_digest)))
                continue
            self._execute_stage(stage)
            injected = stop_after or os.environ.get(STOP_ENV_VAR)
            if injected == stage:
                raise StopRequested("after stage '%s' (injected)" % stage)
        return self.refresh()

    def run_single_stage(self, stage):
        """Run exactly one stage; upstream must be completed and valid."""
        if stage not in STAGES:
            raise PipelineError(
                E_INVALID_STAGE,
                "unknown stage '%s'; valid stages: %s"
                % (stage, ", ".join(STAGES)))
        statuses = self.refresh()
        idx = STAGES.index(stage)
        if idx > 0 and statuses[idx - 1].state != "completed":
            raise PipelineError(
                E_INVALID_STAGE,
                "cannot run stage '%s': upstream stage '%s' artifact is "
                "missing or invalid (state=%s); run the pipeline in order"
                % (stage, STAGES[idx - 1], statuses[idx - 1].state))
        if statuses[idx].state == "completed":
            self.log("[ok] stage '%s' already completed (seq=%d); "
                     "nothing to do" % (stage, statuses[idx].seq))
            return statuses
        self._execute_stage(stage)
        return self.refresh()
