"""Deterministic business error codes used above the DNS protocol layer."""

from __future__ import annotations


E_MALFORMED_MESSAGE = "E_MALFORMED_MESSAGE"
E_CNAME_LOOP = "E_CNAME_LOOP"
E_CHAIN_TOO_LONG = "E_CHAIN_TOO_LONG"
E_UPSTREAM_TIMEOUT = "E_UPSTREAM_TIMEOUT"
E_UPSTREAM_REFUSED = "E_UPSTREAM_REFUSED"
E_UPSTREAM_SERVFAIL = "E_UPSTREAM_SERVFAIL"
E_UPSTREAM_MALFORMED_MESSAGE = "E_UPSTREAM_MALFORMED_MESSAGE"
E_UPSTREAM_TRUNCATED = "E_UPSTREAM_TRUNCATED"
E_UPSTREAM_RCODE = "E_UPSTREAM_RCODE"
E_UPSTREAM_QUESTION_MISMATCH = "E_UPSTREAM_QUESTION_MISMATCH"
E_NEGATIVE_WITHOUT_SOA = "E_NEGATIVE_WITHOUT_SOA"
E_CACHE_WRITE = "E_CACHE_WRITE"
E_INFLIGHT_TIMEOUT = "E_INFLIGHT_TIMEOUT"
E_BAD_EDNS_VERSION = "E_BAD_EDNS_VERSION"
E_UNSUPPORTED_OPCODE = "E_UNSUPPORTED_OPCODE"
E_UNSUPPORTED_QTYPE = "E_UNSUPPORTED_QTYPE"
E_UNSUPPORTED_QCLASS = "E_UNSUPPORTED_QCLASS"


class BusinessError(Exception):
    """An error carrying a stable code exposed by logs, admin API and EDNS option 65001."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


class DnsFormatError(BusinessError):
    def __init__(self, detail: str) -> None:
        super().__init__(E_MALFORMED_MESSAGE, detail)


def rcode_for_error(code: str) -> int:
    """Map business errors to DNS header response codes."""

    if code == E_MALFORMED_MESSAGE:
        return 1  # FORMERR
    if code in (E_UNSUPPORTED_OPCODE, E_UNSUPPORTED_QTYPE, E_UNSUPPORTED_QCLASS):
        return 4  # NOTIMP
    return 2  # SERVFAIL
