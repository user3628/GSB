from prefalign.web.app import create_app


def _client(workspace):
    app = create_app(workspace)
    app.testing = True
    return app.test_client()


def test_full_http_workflow(workspace):
    client = _client(workspace)
    assert client.get("/health").get_json()["status"] == "ok"
    assert client.post("/shards/import", json={"demo": True}).status_code == 200
    run = client.post("/pipeline/run", json={"reason": "http"}).get_json()
    assert run["rebuilt"] == ["normalize", "quality", "features", "sampling"]

    status = client.get("/pipeline/status").get_json()
    assert all(info["present"] for info in status["checkpoints"].values())

    quality = client.get("/pipeline/artifacts/quality").get_json()
    assert quality["stage"] == "quality"

    stop = client.post("/stop").get_json()
    assert stop["stop_requested"] is True
    client.post("/resume")

    mixture = client.get("/mixture").get_json()
    assert "mixture_summary" in mixture
    exported = client.post("/mixture/export").get_json()
    assert "exported" in exported

    verify = client.post("/verify", json={}).get_json()
    assert verify["status"] == "matched"
    assert verify["stages_covered"] == ["normalize", "quality", "features", "sampling"]


def test_http_edit_invalidates(workspace):
    client = _client(workspace)
    client.post("/shards/import", json={"demo": True})
    client.post("/pipeline/run", json={})
    norm = client.get("/pipeline/artifacts/normalize").get_json()
    pair_id = norm["records"][0]["pair_id"]
    base = next(r for r in norm["records"] if r["pair_id"] == pair_id)
    payload = {
        "pair_id": pair_id, "prompt": base["norm_prompt"],
        "chosen": base["norm_chosen"] + " edited",
        "rejected": base["norm_rejected"],
        "source": base["source"], "lang": base["lang"],
    }
    outcome = client.post("/samples/edit", json=payload).get_json()
    assert outcome["run"]["rebuilt"][0] == "normalize"
    verify = client.post("/verify", json={}).get_json()
    assert verify["status"] == "matched"
