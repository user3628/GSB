from __future__ import annotations

from diaglab.formats import decode_bundle, encode_binary, encode_jsonl
from diaglab.models import ProbeRequest
from diaglab.normalize import normalize_bundle
from diaglab.probe import probe_tcp
from diaglab.store import make_batch_fingerprint
from diaglab.test_service import ProbeTestService


def request(port: int, *, timeout_ms: int = 750, path: str = "probe-test") -> ProbeRequest:
    return ProbeRequest(
        host="127.0.0.1",
        port=port,
        path_node=path,
        probe_id="probe-test-001",
        timeout_ms=timeout_ms,
        payload_bytes=32,
        protocol="tcp",
    )


def test_real_success_echo_probe_closes_socket():
    with ProbeTestService(mode="echo") as service:
        observation = probe_tcp(request(service.probe_port))
        assert observation.outcome == "success"
        assert observation.code is None
        assert observation.socket_closed is True
        assert observation.record.success is True
        assert observation.record.rtt_us > 0
        assert observation.record.payload_bytes == 32
        metrics = service.metrics()
        assert metrics["accepted_connections"] == 1
        assert metrics["completed_probes"] == 1
        assert metrics["bytes_in"] == 32
        assert metrics["bytes_out"] == 32


def test_real_refused_probe_is_classified():
    with ProbeTestService(mode="refuse") as service:
        observation = probe_tcp(request(service.probe_port))
        assert observation.outcome == "refused"
        assert observation.code == "E_CONNECTION_REFUSED"
        assert observation.record.failure_type == "refused"
        assert observation.socket_closed is True
        assert service.metrics()["mode"] == "refuse"


def test_real_timeout_probe_releases_socket():
    with ProbeTestService(mode="hang") as service:
        observation = probe_tcp(request(service.probe_port, timeout_ms=120))
        assert observation.outcome == "timeout"
        assert observation.code == "E_PROBE_TIMEOUT"
        assert observation.record.failure_type == "timeout"
        assert observation.socket_closed is True
        assert service.metrics()["accepted_connections"] == 1


def test_probe_record_export_import_preserves_core_fields():
    with ProbeTestService(mode="echo") as service:
        observation = probe_tcp(request(service.probe_port, path="roundtrip-probe"))
    original = [observation.record]
    original_core = [record.core_dict() for record in original]
    for encoded in (encode_binary(original), encode_jsonl(original)):
        restored = normalize_bundle(decode_bundle(encoded)).records
        assert [record.core_dict() for record in restored] == original_core


def test_probe_written_to_store_can_export_and_reimport_without_field_drift(tmp_path):
    from diaglab.store import WorkbenchStore

    store = WorkbenchStore(tmp_path / "probe-store.sqlite3")
    store.initialize()
    with ProbeTestService(mode="echo") as service:
        probe_request = request(service.probe_port, path="persisted-probe")
        fingerprint = make_batch_fingerprint("probe", {"probe_id": probe_request.probe_id})
        result = store.probe_batch(
            batch_id="persist-probe-1",
            fingerprint=fingerprint,
            request=probe_request,
        )
    assert result["probe"]["outcome"] == "success"
    _, exported_records = store.export_records(path_node="persisted-probe")
    original_core = [record.core_dict() for record in exported_records]
    for encoded in (encode_binary(exported_records), encode_jsonl(exported_records)):
        restored = normalize_bundle(decode_bundle(encoded)).records
        assert [record.core_dict() for record in restored] == original_core

