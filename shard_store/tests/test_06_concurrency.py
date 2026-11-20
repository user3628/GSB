"""Requirement 6: serialised writes, concurrent readers, unique versions."""
import threading

from tests.conftest import create_session, upload_part


def test_concurrent_same_part_only_one_wins_others_idempotent(client):
    sess = create_session(client, min_part_size=1)
    results = []
    barrier = threading.Barrier(5)

    def worker():
        barrier.wait()
        r = upload_part(client, sess["session_id"], 1, b"same-content-bytes")
        results.append((r.status_code, r.json()))

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(status == 200 for status, _ in results)
    statuses = sorted(body["data"]["status"] for _, body in results)
    assert statuses.count("stored") == 1
    assert statuses.count("idempotent") == 4

    listing = client.get(f"/api/v1/sessions/{sess['session_id']}/parts").json()["data"]
    assert len(listing["parts"]) == 1
    assert listing["part_count"] == 1
    assert listing["total_size"] == len(b"same-content-bytes")


def test_concurrent_merges_same_key_get_distinct_versions(client, manifest):
    from tests.conftest import sample_part
    obj = manifest["objects"][1]
    sessions = []
    for _ in range(3):
        sess = create_session(
            client, object_key="k/parallel", min_part_size=1,
            expected_total_size=obj["expected_total_size"], conflict_policy="allow",
        )
        for part_number in range(1, obj["part_count"] + 1):
            upload_part(client, sess["session_id"], part_number,
                        sample_part(manifest, 1, part_number))
        sessions.append(sess["session_id"])

    merge_results = []
    barrier = threading.Barrier(3)

    def merge_worker(session_id):
        barrier.wait()
        r = client.post(f"/api/v1/sessions/{session_id}/merge", json={"strategy": "fast"})
        merge_results.append(r.json())

    threads = [threading.Thread(target=merge_worker, args=(sid,)) for sid in sessions]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    versions = sorted(body["data"]["version"] for body in merge_results)
    assert versions == [1, 2, 3]
    digests = {body["data"]["object_sha256"] for body in merge_results}
    assert digests == {obj["expected_object_sha256"]}

    current = client.get("/api/v1/objects/k/parallel").json()["data"]
    assert current["version"] == 3
    history = client.get("/api/v1/objects/versions/k/parallel").json()["data"]
    assert [v["version"] for v in history["versions"]] == [1, 2, 3]


def test_readers_never_observe_partial_counter_state(client):
    # Counters and part rows commit together; repeated reads stay self-consistent.
    sess = create_session(client, min_part_size=1)
    observed = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            data = client.get(f"/api/v1/sessions/{sess['session_id']}/parts").json()["data"]
            assert data["part_count"] == len(data["parts"])
            observed.append(data["part_count"])

    thread = threading.Thread(target=reader)
    thread.start()
    for i in range(1, 21):
        upload_part(client, sess["session_id"], i, f"payload-{i}".encode())
    stop.set()
    thread.join()
    final = client.get(f"/api/v1/sessions/{sess['session_id']}/parts").json()["data"]
    assert final["part_count"] == 20
    assert observed[-1] == 20
