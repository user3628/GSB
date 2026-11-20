"""Filesystem & hashing helpers: canonical JSON, digests, atomic writes."""

import hashlib
import json
import os


def canonical_json_bytes(obj):
    """Deterministic JSON encoding used for version fingerprints."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode("utf-8")


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def digest_bytes(data):
    return "sha256:" + sha256_hex(data)


def digest_text(text):
    return digest_bytes(text.encode("utf-8"))


def short(digest, width=12):
    if not digest:
        return "-"
    return digest.replace("sha256:", "")[:width]


def atomic_write_bytes(path, data):
    """Write bytes to path atomically (tmp file + fsync + os.replace).

    A crash at any point leaves either the old file or the new file,
    never a half-written one. Leftover tmp files are harmless.
    """
    path = str(path)
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    if os.name != "nt":
        try:
            dirfd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
            try:
                os.fsync(dirfd)
            finally:
                os.close(dirfd)
        except OSError:
            pass


def read_json_file(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def dump_json_lines(records):
    """Deterministic JSONL encoding: sorted keys, UTF-8, trailing newline."""
    out = []
    for rec in records:
        out.append(json.dumps(rec, sort_keys=True, ensure_ascii=False,
                              separators=(",", ":")))
    return ("\n".join(out) + "\n").encode("utf-8") if out else b""
