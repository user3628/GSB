"""In-memory representations of pages, WAL records and checkpoints."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .util import crc32_hex, sha256_hex


@dataclass
class Page:
    page_no: int
    digest: str
    version: int
    last_lsn: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_no": self.page_no,
            "digest": self.digest,
            "version": self.version,
            "last_lsn": self.last_lsn,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Page":
        return cls(
            page_no=int(data["page_no"]),
            digest=data["digest"],
            version=int(data["version"]),
            last_lsn=int(data["last_lsn"]),
        )


@dataclass
class WalRecord:
    lsn: int
    page_no: int
    before_digest: Optional[str]
    after_digest: str
    txn_id: str
    ts: int

    def checksum_payload(self) -> Dict[str, Any]:
        return {
            "lsn": self.lsn,
            "page_no": self.page_no,
            "before_digest": self.before_digest,
            "after_digest": self.after_digest,
            "txn_id": self.txn_id,
            "ts": self.ts,
        }

    def checksum(self) -> str:
        return crc32_hex(self.checksum_payload())

    def to_dict(self) -> Dict[str, Any]:
        payload = self.checksum_payload()
        payload["crc"] = self.checksum()
        return payload

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WalRecord":
        rec = cls(
            lsn=int(data["lsn"]),
            page_no=int(data["page_no"]),
            before_digest=data.get("before_digest"),
            after_digest=data["after_digest"],
            txn_id=data["txn_id"],
            ts=int(data["ts"]),
        )
        stored = data.get("crc")
        if stored is None or stored != rec.checksum():
            raise ValueError(f"record checksum mismatch at lsn={rec.lsn}")
        return rec


@dataclass
class Checkpoint:
    seq: int
    start_lsn: int
    pages: Dict[int, Page]
    checksum: Optional[str] = None

    def _digest_payload(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "start_lsn": self.start_lsn,
            "pages": {
                str(page_no): {
                    "digest": page.digest,
                    "version": page.version,
                    "last_lsn": page.last_lsn,
                }
                for page_no, page in sorted(self.pages.items())
            },
        }

    def compute_checksum(self) -> str:
        return sha256_hex(self._digest_payload())

    def seal(self) -> "Checkpoint":
        self.checksum = self.compute_checksum()
        return self

    def verify(self) -> bool:
        return self.checksum is not None and self.checksum == self.compute_checksum()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "start_lsn": self.start_lsn,
            "checksum": self.checksum,
            "pages": {str(k): v.to_dict() for k, v in sorted(self.pages.items())},
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Checkpoint":
        pages = {
            int(k): Page.from_dict(v)
            for k, v in data.get("pages", {}).items()
        }
        cp = cls(
            seq=int(data["seq"]),
            start_lsn=int(data["start_lsn"]),
            pages=pages,
            checksum=data.get("checksum"),
        )
        return cp
