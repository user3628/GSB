"""Immutable semantic DNS records and resolution results."""

from __future__ import annotations

import base64
import ipaddress
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from dnslib import A, AAAA, CNAME, MX, NS, SOA, TXT, DNSLabel, QTYPE, RR


SUPPORTED_QTYPES = frozenset(
    {QTYPE.A, QTYPE.AAAA, QTYPE.CNAME, QTYPE.MX, QTYPE.TXT, QTYPE.NS, QTYPE.SOA}
)
TYPE_NAMES = {
    QTYPE.A: "A",
    QTYPE.AAAA: "AAAA",
    QTYPE.CNAME: "CNAME",
    QTYPE.MX: "MX",
    QTYPE.TXT: "TXT",
    QTYPE.NS: "NS",
    QTYPE.SOA: "SOA",
}


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(data: str) -> bytes:
    return base64.b64decode(data.encode("ascii"), validate=True)


def labels_to_json(labels: Sequence[bytes]) -> list[str]:
    return [_b64(label) for label in labels]


def labels_from_json(items: Sequence[str]) -> tuple[bytes, ...]:
    return tuple(_unb64(item) for item in items)


@dataclass(frozen=True)
class Record:
    """A compression-free, JSON-serializable semantic resource record."""

    owner: tuple[bytes, ...]
    rtype: int
    rclass: int
    ttl: int
    data: Mapping[str, Any]

    @property
    def type_name(self) -> str:
        return TYPE_NAMES.get(self.rtype, str(self.rtype))

    def with_ttl(self, ttl: int) -> "Record":
        return Record(self.owner, self.rtype, self.rclass, max(0, int(ttl)), self.data)

    @classmethod
    def from_rr(cls, rr: RR) -> "Record":
        rtype = int(rr.rtype)
        if rtype == QTYPE.A:
            data = {"address": ".".join(str(part) for part in rr.rdata.data)}
        elif rtype == QTYPE.AAAA:
            data = {"address": ipaddress.IPv6Address(bytes(rr.rdata.data)).compressed}
        elif rtype in (QTYPE.CNAME, QTYPE.NS):
            data = {"target": labels_to_json(rr.rdata.label.label)}
        elif rtype == QTYPE.MX:
            data = {
                "preference": int(rr.rdata.preference),
                "exchange": labels_to_json(rr.rdata.label.label),
            }
        elif rtype == QTYPE.TXT:
            data = {"strings": [_b64(item) for item in rr.rdata.data]}
        elif rtype == QTYPE.SOA:
            data = {
                "mname": labels_to_json(rr.rdata.mname.label),
                "rname": labels_to_json(rr.rdata.rname.label),
                "times": [int(item) for item in rr.rdata.times],
            }
        else:
            raise ValueError(f"unsupported record type {rtype}")
        return cls(tuple(rr.rname.label), rtype, int(rr.rclass), max(0, int(rr.ttl)), data)

    def to_rr(self, ttl: int | None = None) -> RR:
        name = DNSLabel(list(self.owner))
        value = int(self.ttl if ttl is None else max(0, int(ttl)))
        if self.rtype == QTYPE.A:
            rdata = A(self.data["address"])
        elif self.rtype == QTYPE.AAAA:
            rdata = AAAA(self.data["address"])
        elif self.rtype == QTYPE.CNAME:
            rdata = CNAME(DNSLabel(list(labels_from_json(self.data["target"]))))
        elif self.rtype == QTYPE.NS:
            rdata = NS(DNSLabel(list(labels_from_json(self.data["target"]))))
        elif self.rtype == QTYPE.MX:
            rdata = MX(
                DNSLabel(list(labels_from_json(self.data["exchange"]))),
                int(self.data["preference"]),
            )
        elif self.rtype == QTYPE.TXT:
            rdata = TXT([_unb64(item) for item in self.data["strings"]])
        elif self.rtype == QTYPE.SOA:
            rdata = SOA(
                DNSLabel(list(labels_from_json(self.data["mname"]))),
                DNSLabel(list(labels_from_json(self.data["rname"]))),
                tuple(int(item) for item in self.data["times"]),
            )
        else:
            raise ValueError(f"unsupported record type {self.rtype}")
        return RR(name, self.rtype, self.rclass, value, rdata)

    def to_dict(self) -> dict[str, Any]:
        return {
            "owner": labels_to_json(self.owner),
            "rtype": self.rtype,
            "rclass": self.rclass,
            "ttl": self.ttl,
            "data": dict(self.data),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "Record":
        return cls(
            labels_from_json(value["owner"]),
            int(value["rtype"]),
            int(value["rclass"]),
            int(value["ttl"]),
            dict(value["data"]),
        )


@dataclass(frozen=True)
class Resolution:
    kind: str
    rcode: int
    answers: tuple[Record, ...] = ()
    authority: tuple[Record, ...] = ()
    negative_ttl: int | None = None
    negative_ttl_source: str | None = None
    business_code: str | None = None
    error_detail: str | None = None
    cached: bool = False
    upstream_queries: int = 0

    @property
    def is_error(self) -> bool:
        return self.business_code is not None

    def remaining_copy(self, elapsed: float) -> "Resolution":
        """Return records with independently decremented TTLs."""

        answers = tuple(record.with_ttl(record.ttl - int(elapsed)) for record in self.answers)
        authority = tuple(record.with_ttl(record.ttl - int(elapsed)) for record in self.authority)
        negative_ttl = None
        if self.negative_ttl is not None:
            negative_ttl = max(0, self.negative_ttl - int(elapsed))
            authority = tuple(
                record.with_ttl(negative_ttl)
                if record.rtype == QTYPE.SOA
                else record.with_ttl(record.ttl - int(elapsed))
                for record in authority
            )
        return Resolution(
            kind=self.kind,
            rcode=self.rcode,
            answers=answers,
            authority=authority,
            negative_ttl=negative_ttl,
            negative_ttl_source=self.negative_ttl_source,
            business_code=self.business_code,
            error_detail=self.error_detail,
            cached=True,
            upstream_queries=0,
        )
