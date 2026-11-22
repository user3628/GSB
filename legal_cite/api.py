"""Local FastAPI service bound to 127.0.0.1 only."""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import config_fingerprint, default_config
from .errors import LegalCiteError
from .evidence import import_manifest, verify_manifest
from .pipeline import StopController, run_pipeline

app = FastAPI(title="legal-cite", version="2.0.0")


class ResolveRequest(BaseModel):
    records: list
    contract_id: str | None = None


class ManifestImportRequest(BaseModel):
    manifest: dict


@app.get("/health")
def health():
    return {"status": "ok", "bind": "127.0.0.1"}


@app.post("/resolve")
def resolve(req: ResolveRequest):
    cfg = default_config()
    records = req.records
    if req.contract_id:
        records = [r for r in records if r.get("contract_id") == req.contract_id]
    try:
        state = run_pipeline(records, cfg=cfg)
    except LegalCiteError as exc:
        raise HTTPException(status_code=422, detail=exc.to_dict())
    return {
        "pipeline_fingerprint": state["pipeline_fingerprint"],
        "decisions": state["manifest"]["decisions"],
        "folded_duplicates": state["folded_duplicates"],
        "diagnostics": state["diagnostics"],
    }


@app.post("/evidence/verify")
def evidence_verify(req: ManifestImportRequest):
    cfg = default_config()
    try:
        imported = import_manifest(req.manifest, cfg,
                                   config_fingerprint(cfg))
        verify_manifest(imported)
    except LegalCiteError as exc:
        raise HTTPException(status_code=422, detail=exc.to_dict())
    return {"manifest_version": imported["manifest_version"],
            "chain_count": len(imported["evidence_chains"]),
            "migrated": imported.get("migrated_from_version")}


@app.get("/contracts/{contract_id}/citations/{citation_id}/evidence")
def citation_evidence(contract_id: str, citation_id: str):
    return {"note": "use POST /evidence/verify with a manifest for lookup",
            "contract_id": contract_id, "citation_id": citation_id}
