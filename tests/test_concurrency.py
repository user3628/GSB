import threading

from logindex.engine import LogIndexEngine


def test_concurrent_duplicate_batch_applies_once(db_path, sample_lines):
    engine = LogIndexEngine(db_path)
    lines = sample_lines("batch1_full.txt")
    results = []
    errors = []

    def submit():
        try:
            results.append(engine.submit_batch("DUP", lines))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    assert len(results) == 5
    assert sum(1 for r in results if r.reused) == 4
    assert len({r.version for r in results}) == 1
    assert len(engine.query_records()) == 12


def test_writers_serialize_and_readers_concur(db_path, sample_lines):
    engine = LogIndexEngine(db_path)
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    order = []

    def writer(batch_id, lines, base):
        order.append(("start", batch_id))
        engine.submit_batch(batch_id, lines, base_version=base)
        order.append(("end", batch_id))

    t2 = threading.Thread(
        target=writer, args=("B2", sample_lines("batch2_append.txt"), 1))
    t3 = threading.Thread(
        target=writer, args=("B3", sample_lines("batch3_mixed.txt"), 2))
    t2.start()
    t3.start()
    # 读操作在写期间可以并发执行且不抛错
    snapshot = engine.query_records()
    t2.join()
    t3.join()
    assert isinstance(snapshot, list)
    assert engine.current_version() == 3
