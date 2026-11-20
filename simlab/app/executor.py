"""Real, interruptible injection execution against local HTTP test endpoints.

* node_down      -> connect to an endpoint that refuses / resets the
                    connection (ConnectionRefusedError) => replica unavailable.
* link_timeout   -> connect to a hanging endpoint with a hard socket timeout;
                    on timeout the underlying connection is closed and the
                    injection returns E_INJECTION_TIMEOUT.
* replica_jitter -> probe a delay endpoint, measure real latency with NumPy
                    statistics; latency beyond the dependency budget degrades
                    the replica.

Every probe closes its socket in a ``finally`` so a hung injection releases
resources and never leaks file descriptors.
"""
from __future__ import annotations

import http.client
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import numpy as np

from .errors import E_INJECTION_TIMEOUT

JITTER_PROBES = 3


@dataclass
class InjectionResult:
    item_key: str
    service: str
    fault_type: str
    ok: bool
    effect: str                      # replica_unavailable | link_timeout | latency_spike | none
    error_code: str | None = None
    latency_ms: float = 0.0
    samples: list[float] = field(default_factory=list)
    detail: str = ""


def _probe_once(url: str, timeout_s: float) -> float:
    """One real HTTP probe.  Returns latency ms.  Raises on refusal/timeout."""
    parsed = urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout_s)
    start = time.monotonic()
    try:
        conn.request("GET", path, headers={"Connection": "close"})
        resp = conn.getresponse()
        resp.read()
        return (time.monotonic() - start) * 1000.0
    finally:
        conn.close()  # always release the underlying connection


def execute_injection(item: dict) -> InjectionResult:
    """Execute one injection item against its loopback endpoint."""
    fault = item["fault_type"]
    timeout_s = max(item.get("timeout_ms", 800), 50) / 1000.0
    result = InjectionResult(item_key=item["item_key"], service=item["service"],
                             fault_type=fault, ok=True, effect="none")
    try:
        if fault == "node_down":
            _probe_once(item["endpoint"], timeout_s)
            result.effect = "none"
            result.detail = "endpoint reachable; node_down not confirmed"
        elif fault == "link_timeout":
            _probe_once(item["endpoint"], timeout_s)
            result.effect = "none"
            result.detail = "endpoint responded within budget"
        elif fault == "replica_jitter":
            samples = [_probe_once(item["endpoint"], timeout_s)
                       for _ in range(JITTER_PROBES)]
            arr = np.asarray(samples, dtype=float)
            result.samples = [round(float(x), 2) for x in arr]
            result.latency_ms = round(float(arr.mean()), 2)
            p95 = float(np.percentile(arr, 95))
            budget = item.get("timeout_ms", 800)
            if p95 > budget * 0.8:
                result.effect = "latency_spike"
                result.detail = f"p95={p95:.1f}ms exceeds 80% of budget {budget}ms"
            else:
                result.detail = f"latency within budget (p95={p95:.1f}ms)"
    except ConnectionRefusedError as exc:
        result.ok = True  # the fault was successfully observed
        result.effect = "replica_unavailable"
        result.detail = f"connection refused: {exc.__class__.__name__}"
    except (TimeoutError, http.client.HTTPException, OSError) as exc:
        is_timeout = isinstance(exc, (TimeoutError,)) or "timed out" in str(exc)
        if is_timeout:
            result.ok = False
            result.effect = "link_timeout"
            result.error_code = E_INJECTION_TIMEOUT
            result.detail = "probe timed out; underlying connection closed"
        elif isinstance(exc, ConnectionResetError):
            result.effect = "replica_unavailable"
            result.detail = "connection reset by peer"
        else:
            result.ok = False
            result.effect = "link_timeout"
            result.error_code = E_INJECTION_TIMEOUT
            result.detail = f"probe failed: {exc.__class__.__name__}: {exc}"
    return result
