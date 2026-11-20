from __future__ import annotations

import sqlite3
import threading
import time

from dnslib import QTYPE

from dnsmw.cache import CacheStore
from dnsmw.codec import QueryKey
from dnsmw.model import Record, Resolution


def _record(name: bytes, ttl: int) -> Record:
    return Record((name, b"example", b"test"), QTYPE.A, 1, ttl, {"address": "192.0.2.1"})


def _key(name: str) -> QueryKey:
    return QueryKey(name, QTYPE.A, 1)


def test_positive_cache_decrements_each_ttl_and_expires(tmp_path):
    cache = CacheStore(tmp_path / "cache.sqlite3")
    key = _key("chain.example.test.")
    session = cache.start_session(key)
    assert cache.intents()[0]["state"] == "pending"
    resolution = Resolution(
        kind="positive",
        rcode=0,
        answers=(_record(b"chain", 2), _record(b"target", 10)),
    )
    cache.commit_resolution(session, key, resolution)
    assert cache.intents()[0]["state"] == "committed"
    first = cache.get(key)
    assert first is not None
    assert [item.ttl for item in first.answers] == [2, 10]
    time.sleep(1.05)
    second = cache.get(key)
    assert second is not None
    assert [item.ttl for item in second.answers] == [1, 9]
    time.sleep(1.05)
    assert cache.get(key) is None
    assert cache.stats()["entries"] == 0
    cache.close()


def test_recovery_cleans_pending_intent_and_orphan_cache_but_keeps_committed(tmp_path):
    path = tmp_path / "cache.sqlite3"
    cache = CacheStore(path)
    committed_key = _key("committed.example.test.")
    session = cache.start_session(committed_key)
    cache.commit_resolution(
        session,
        committed_key,
        Resolution(kind="positive", rcode=0, answers=(_record(b"committed", 120),)),
    )
    cache.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys=ON")
    now = time.time()
    running = connection.execute(
        """
        INSERT INTO resolution_sessions
            (cache_key,qname,qtype,qclass,started_at,updated_at,status,chain_steps,last_upstream_name)
        VALUES ('crashed.example.test.','crashed.example.test.',1,1,?,?, 'running',2,
                'step2.example.test.')
        """,
        (now, now),
    )
    crashed_session = int(running.lastrowid)
    intent = connection.execute(
        """
        INSERT INTO cache_commit_intents(session_id,cache_key,state,created_at)
        VALUES (?, 'crashed.example.test.', 'pending', ?)
        """,
        (crashed_session, now),
    )
    intent_id = int(intent.lastrowid)
    connection.execute(
        """
        INSERT INTO cache_entries
            (cache_key,session_id,intent_id,qname,qtype,qclass,kind,rcode,written_at,
             expires_at,remaining_ttl,source_types,negative_ttl_source,payload)
        VALUES ('crashed.example.test.',?,?,'crashed.example.test.',1,1,'positive',0,?,
                ?,100,'A',NULL,'{}')
        """,
        (crashed_session, intent_id, now, now + 100),
    )
    connection.commit()
    connection.close()

    recovered = CacheStore(path)
    report = recovered.latest_recovery_report()
    assert report["source"] == "sqlite"
    assert [item["cache_key"] for item in report["interrupted_sessions"]] == [
        "crashed.example.test."
    ]
    assert [item["intent_id"] for item in report["abandoned_intents"]] == [intent_id]
    assert [item["cache_key"] for item in report["removed_uncommitted_cache_entries"]] == [
        "crashed.example.test."
    ]
    assert recovered.get(committed_key) is not None
    assert recovered.get(_key("crashed.example.test.")) is None
    sessions = {row["cache_key"]: row for row in recovered.sessions()}
    assert sessions["crashed.example.test."]["status"] == "interrupted"
    assert sessions["committed.example.test."]["status"] == "committed"
    recovered.close()


def test_readers_never_observe_partial_chain(tmp_path):
    cache = CacheStore(tmp_path / "cache.sqlite3")
    key = _key("atomic.example.test.")
    session = cache.start_session(key)
    cache.commit_resolution(
        session,
        key,
        Resolution(
            kind="positive",
            rcode=0,
            answers=(_record(b"atomic", 100), _record(b"target", 100)),
        ),
    )
    observed: list[list[int]] = []
    lock = threading.Lock()

    def reader() -> None:
        for _ in range(100):
            hit = cache.get(key)
            with lock:
                observed.append([record.ttl for record in hit.answers])

    threads = [threading.Thread(target=reader) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert observed
    assert all(ttls in ([100, 100], [99, 99]) for ttls in observed)
    cache.close()
