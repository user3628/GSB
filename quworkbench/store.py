"""Stage 3: versioned checkpoint persistence with atomic publish and locks.

Layout per workspace:
  checkpoints/<shard_id>/<stage>/records.jsonl   append-only record journal
  checkpoints/<shard_id>/<stage>/artifact-NNNNNN.json
  locks/<shard_id>.lock                          real flock-protected lock

A record line holds: seq (monotonic, contiguous per shard+stage), stage,
shard_id, shard content fingerprint, config version fingerprint, input
digests, artifact digest and artifact file name. Artifacts are written to a
temp file and atomically renamed; journal lines are appended with a single
write + fsync, so an interrupted write never leaves a half-written record
behind. Recovery ignores corrupt tails, incomplete records and duplicate
sequence numbers, then resumes from the most recent consistent checkpoint.
"""

import fcntl
import json
import os
from contextlib import contextmanager
from pathlib import Path

from .canon import canonical, digest_bytes
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, WorkbenchError

STAGES = ("normalize", "classify", "slots")


class Checkpoint:
    def __init__(self, seq, record, artifact_path):
        self.seq = seq
        self.record = record
        self.artifact_path = artifact_path


class CheckpointStore:
    def __init__(self, workspace):
        self.ws = Path(workspace)
        self.ckpt_dir = self.ws / "checkpoints"
        self.lock_dir = self.ws / "locks"
        self.ckpt_dir.mkdir(parents=True, exist_ok=True)
        self.lock_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ lock
    @contextmanager
    def lock(self, shard_id):
        """Exclusive cross-process file lock for one shard."""
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_dir / f"{shard_id}.lock", os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    # ----------------------------------------------------------------- paths
    def stage_dir(self, shard_id, stage):
        return self.ckpt_dir / shard_id / stage

    def journal_path(self, shard_id, stage):
        return self.stage_dir(shard_id, stage) / "records.jsonl"

    def artifact_path(self, shard_id, stage, seq):
        return self.stage_dir(shard_id, stage) / f"artifact-{seq:06d}.json"

    # ----------------------------------------------------------- journal read
    def _read_journal(self, shard_id, stage):
        """Return (valid_records, corrupt_offset). Parsing stops at the first
        corrupt or incomplete line; duplicate seqs keep the first record."""
        path = self.journal_path(shard_id, stage)
        records = []
        corrupt_offset = None
        if not path.exists():
            return records, corrupt_offset
        seen = set()
        with open(path, "rb") as fh:
            offset = 0
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    offset += len(line)
                    continue
                try:
                    rec = json.loads(stripped.decode("utf-8"))
                    if not isinstance(rec, dict) or "seq" not in rec:
                        raise ValueError("incomplete record")
                except (ValueError, UnicodeDecodeError):
                    corrupt_offset = offset
                    break
                if rec["seq"] not in seen:
                    seen.add(rec["seq"])
                    records.append(rec)
                offset += len(line)
        return records, corrupt_offset

    def _consistent_seq(self, shard_id, stage, records):
        """Highest seq k such that records 1..k are contiguous and artifact-k
        exists with a matching digest."""
        by_seq = {r["seq"]: r for r in records}
        prefix = 0
        while prefix + 1 in by_seq:
            prefix += 1
        k = prefix
        while k >= 1:
            rec = by_seq[k]
            artifact = self.stage_dir(shard_id, stage) / rec.get("artifact_file", "")
            if artifact.exists() and digest_bytes(artifact.read_bytes()) == rec.get("artifact_digest"):
                return k
            k -= 1
        return 0

    def consistent(self, shard_id, stage):
        """Most recent consistent checkpoint view (read-only)."""
        records, _ = self._read_journal(shard_id, stage)
        if not records:
            return None
        k = self._consistent_seq(shard_id, stage, records)
        if k == 0:
            return None
        by_seq = {r["seq"]: r for r in records}
        rec = by_seq[k]
        return Checkpoint(k, rec, self.stage_dir(shard_id, stage) / rec["artifact_file"])

    def read_artifact(self, shard_id, stage, checkpoint=None):
        cp = checkpoint or self.consistent(shard_id, stage)
        if cp is None:
            return None
        return json.loads(cp.artifact_path.read_bytes().decode("utf-8"))

    # --------------------------------------------------------------- publish
    def publish(self, shard_id, stage, record, artifact, seq=None,
                expected_shard_fingerprint=None):
        """Atomically publish one stage artifact under the shard lock.

        Only contiguous sequence advancement is accepted; stale, duplicate or
        cross-shard publishes raise E_STALE_CHECKPOINT, a missing upstream
        stage raises E_INVALID_STAGE, and a rejected publish leaves the
        workspace untouched.
        """
        if stage not in STAGES:
            raise WorkbenchError(E_INVALID_STAGE, "unknown_stage", details={"stage": stage})
        with self.lock(shard_id):
            current = self.consistent(shard_id, stage)
            expected_seq = (current.seq if current else 0) + 1
            if seq is None:
                seq = expected_seq
            if seq != expected_seq:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "sequence_not_contiguous",
                    stage=stage,
                    details={"expected_seq": expected_seq, "got_seq": seq, "shard_id": shard_id},
                )
            if record.get("shard_id") != shard_id:
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "cross_shard_publish",
                    stage=stage,
                    details={"record_shard": record.get("shard_id"), "target_shard": shard_id},
                )
            if (
                expected_shard_fingerprint is not None
                and record.get("shard_fingerprint") != expected_shard_fingerprint
            ):
                raise WorkbenchError(
                    E_STALE_CHECKPOINT,
                    "shard_fingerprint_mismatch",
                    stage=stage,
                    details={
                        "expected": expected_shard_fingerprint,
                        "got": record.get("shard_fingerprint"),
                    },
                )
            if stage != STAGES[0]:
                upstream = self.consistent(shard_id, STAGES[STAGES.index(stage) - 1])
                if upstream is None:
                    raise WorkbenchError(
                        E_INVALID_STAGE,
                        "missing_upstream_stage_artifact",
                        stage=stage,
                        details={"upstream_stage": STAGES[STAGES.index(stage) - 1]},
                    )
            sdir = self.stage_dir(shard_id, stage)
            sdir.mkdir(parents=True, exist_ok=True)
            artifact_bytes = canonical(artifact)
            artifact_name = f"artifact-{seq:06d}.json"
            tmp = sdir / f".{artifact_name}.tmp"
            with open(tmp, "wb") as fh:
                fh.write(artifact_bytes)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, sdir / artifact_name)
            full_record = dict(
                record,
                seq=seq,
                stage=stage,
                artifact_digest=digest_bytes(artifact_bytes),
                artifact_file=artifact_name,
            )
            line = canonical(full_record) + b"\n"
            journal = self.journal_path(shard_id, stage)
            with open(journal, "ab") as fh:
                fh.write(line)
                fh.flush()
                os.fsync(fh.fileno())
            return full_record

    # --------------------------------------------------------------- recover
    def recover(self):
        """Repair every shard/stage journal: drop corrupt tails, duplicate or
        non-contiguous records and orphaned artifacts. Returns a report."""
        report = {"truncated_journals": [], "dropped_records": [], "removed_files": []}
        if not self.ckpt_dir.exists():
            return report
        for shard_dir in sorted(p for p in self.ckpt_dir.iterdir() if p.is_dir()):
            for stage_dir in sorted(p for p in shard_dir.iterdir() if p.is_dir()):
                shard_id, stage = shard_dir.name, stage_dir.name
                for tmp in stage_dir.glob("*.tmp"):
                    tmp.unlink()
                    report["removed_files"].append(str(tmp))
                records, corrupt_offset = self._read_journal(shard_id, stage)
                journal = self.journal_path(shard_id, stage)
                if corrupt_offset is not None:
                    with open(journal, "r+b") as fh:
                        fh.truncate(corrupt_offset)
                    report["truncated_journals"].append(str(journal))
                    records, _ = self._read_journal(shard_id, stage)
                k = self._consistent_seq(shard_id, stage, records)
                by_seq = {r["seq"]: r for r in records}
                keep = [by_seq[i] for i in range(1, k + 1)]
                dropped = [r for r in records if r["seq"] > k]
                if dropped or corrupt_offset is not None:
                    for r in dropped:
                        report["dropped_records"].append(
                            {"shard_id": shard_id, "stage": stage, "seq": r["seq"]}
                        )
                    tmp = journal.with_suffix(".jsonl.tmp")
                    with open(tmp, "wb") as fh:
                        for rec in keep:
                            fh.write(canonical(rec) + b"\n")
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.replace(tmp, journal)
                for artifact in stage_dir.glob("artifact-*.json"):
                    if artifact.name not in {r["artifact_file"] for r in keep}:
                        artifact.unlink()
                        report["removed_files"].append(str(artifact))
        return report
