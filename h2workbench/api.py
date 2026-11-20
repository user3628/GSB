from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .engine import Workbench
from .errors import BusinessError


class SampleSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    sample_id: str
    connection_id: str | None = None
    frames: list[dict[str, Any]]
    batch_size: int = 128
    config: dict[str, Any] | None = None


class ScheduleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    scheduler_mode: str | None = None
    mode: str | None = None
    round_budget: int | None = None
    max_rounds: int | None = None
    candidate_threshold: int | None = None
    candidate_limit: int | None = None
    fail_on_block: bool = True


class WindowUpdateRequest(BaseModel):
    stream_id: int = Field(ge=0)
    increment: int = Field(gt=0)
    sample_id: str | None = None


SAMPLE_EXAMPLE = {
    "sample_id": "quick-normal",
    "connection_id": "quick-conn",
    "batch_size": 2,
    "config": {
        "connection_window": 65535,
        "stream_window": 65535,
        "round_budget": 4096,
        "candidate_threshold": 8,
        "candidate_limit": 8,
    },
    "frames": [
        {
            "connection_id": "quick-conn",
            "stream_id": 1,
            "frame_type": "HEADERS",
            "flags": ["END_HEADERS"],
            "payload_bytes": 0,
            "window_increment": 0,
            "priority": 0,
            "weight": 4,
        },
        {
            "connection_id": "quick-conn",
            "stream_id": 2,
            "frame_type": "HEADERS",
            "flags": ["END_HEADERS"],
            "payload_bytes": 0,
            "window_increment": 0,
            "priority": 1,
            "weight": 1,
        },
        {
            "connection_id": "quick-conn",
            "stream_id": 1,
            "frame_type": "DATA",
            "flags": [],
            "payload_bytes": 5000,
            "window_increment": 0,
        },
        {
            "connection_id": "quick-conn",
            "stream_id": 2,
            "frame_type": "DATA",
            "flags": ["END_STREAM"],
            "payload_bytes": 2000,
            "window_increment": 0,
        },
    ],
}


def create_app(db_path: str | Path | None = None) -> FastAPI:
    resolved_db = Path(
        db_path or os.environ.get("H2WB_DB_PATH", "var/h2workbench.db")
    ).resolve()
    app = FastAPI(
        title="HTTP/2 Multiplexing Diagnostics Workbench",
        version="1.0.0",
        description=(
            "Deterministic loopback-only HTTP/2 frame-sequence diagnostics with "
            "connection/stream flow-control windows and weighted multiplexing traces."
        ),
    )
    app.state.db_path = str(resolved_db)
    app.state.workbench = Workbench(resolved_db)

    @app.exception_handler(BusinessError)
    async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status or 400, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        err = BusinessError(
            code="E_FRAME_INVALID",
            message="request validation failed",
            context={"errors": jsonable_encoder(exc.errors())},
        )
        return JSONResponse(status_code=400, content=err.to_dict())

    def workbench() -> Workbench:
        return app.state.workbench

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "loopback_only": True,
            "db_path": app.state.db_path,
        }

    @app.get("/api/demo/sample")
    def demo_sample() -> dict[str, Any]:
        return SAMPLE_EXAMPLE

    @app.post("/api/samples")
    def submit_sample(body: SampleSubmitRequest) -> dict[str, Any]:
        return workbench().submit_sample(body.model_dump())

    @app.get("/api/samples/{sample_id}")
    def get_sample(sample_id: str) -> dict[str, Any]:
        return workbench().get_sample(sample_id)

    @app.post("/api/samples/{sample_id}/schedule")
    def schedule_sample(sample_id: str, body: ScheduleRequest | None = None) -> dict[str, Any]:
        payload = body.model_dump(exclude_none=True) if body else {}
        fail_on_block = bool(payload.pop("fail_on_block", True))
        return workbench().schedule_sample(
            sample_id, payload, fail_on_block=fail_on_block
        )

    @app.get("/api/samples/{sample_id}/trace")
    def sample_trace(
        sample_id: str,
        stream_id: int | None = Query(default=None),
    ) -> dict[str, Any]:
        return workbench().get_sample_trace(sample_id, stream_id=stream_id)

    @app.get("/api/connections")
    def list_connections() -> dict[str, Any]:
        return workbench().list_connections()

    @app.get("/api/connections/{connection_id}")
    def get_connection(connection_id: str) -> dict[str, Any]:
        return workbench().get_connection(connection_id)

    @app.get("/api/connections/{connection_id}/streams")
    def get_streams(connection_id: str) -> dict[str, Any]:
        snapshot = workbench().get_connection(connection_id)
        return {
            "connection_id": connection_id,
            "connection_window": snapshot["connection_window"],
            "streams": snapshot["streams"],
        }

    @app.get("/api/connections/{connection_id}/streams/{stream_id}")
    def get_stream(connection_id: str, stream_id: int) -> dict[str, Any]:
        return workbench().get_stream(connection_id, stream_id)

    @app.post("/api/connections/{connection_id}/schedule")
    def schedule_connection(
        connection_id: str, body: ScheduleRequest | None = None
    ) -> dict[str, Any]:
        payload = body.model_dump(exclude_none=True) if body else {}
        fail_on_block = bool(payload.pop("fail_on_block", True))
        return workbench().schedule_connection(
            connection_id, payload, fail_on_block=fail_on_block
        )

    @app.get("/api/connections/{connection_id}/trace")
    def connection_trace(
        connection_id: str,
        sample_id: str | None = Query(default=None),
        stream_id: int | None = Query(default=None),
        limit: int = Query(default=25, ge=1, le=500),
    ) -> dict[str, Any]:
        return workbench().get_connection_trace(
            connection_id, sample_id=sample_id, stream_id=stream_id, limit=limit
        )

    @app.get("/api/connections/{connection_id}/transitions")
    def connection_transitions(
        connection_id: str, sample_id: str | None = Query(default=None)
    ) -> dict[str, Any]:
        return workbench().get_connection_transitions(
            connection_id, sample_id=sample_id
        )

    @app.get("/api/connections/{connection_id}/violations")
    def connection_violations(connection_id: str) -> dict[str, Any]:
        return workbench().get_connection_violations(connection_id)

    @app.post("/api/connections/{connection_id}/window-update")
    def window_update(connection_id: str, body: WindowUpdateRequest) -> dict[str, Any]:
        sample_id = body.sample_id or f"window-update-{uuid.uuid4().hex[:16]}"
        return workbench().submit_sample(
            {
                "sample_id": sample_id,
                "connection_id": connection_id,
                "frames": [
                    {
                        "connection_id": connection_id,
                        "stream_id": body.stream_id,
                        "frame_type": "WINDOW_UPDATE",
                        "flags": [],
                        "payload_bytes": 0,
                        "window_increment": body.increment,
                    }
                ],
            }
        )

    @app.post("/api/connections/{connection_id}/close")
    def close_connection(connection_id: str) -> dict[str, Any]:
        return workbench().close_connection(connection_id)

    @app.get("/api/connections/{connection_id}/recovery")
    def connection_recovery(connection_id: str) -> dict[str, Any]:
        return workbench().get_recovery_report(connection_id)

    @app.get("/api/recovery")
    def recovery() -> dict[str, Any]:
        return workbench().get_recovery_report()

    return app


DASHBOARD_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>HTTP/2 多路复用诊断工作台</title>
  <style>
    :root { --bg:#f5f7fb; --card:#fff; --ink:#18202f; --muted:#667085; --line:#dfe4ee; --accent:#0b63ce; --warn:#a15c00; --bad:#b42318; --ok:#087443; }
    * { box-sizing:border-box; }
    body { margin:0; font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }
    header { background:#10233f; color:#fff; padding:18px 24px; }
    header h1 { margin:0; font-size:20px; } header p { margin:4px 0 0; color:#bfd0e8; }
    main { max-width:1500px; margin:0 auto; padding:20px; display:grid; gap:16px; }
    .toolbar,.card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:16px; }
    .toolbar { display:flex; gap:10px; flex-wrap:wrap; align-items:end; }
    label { display:grid; gap:4px; color:var(--muted); font-size:12px; }
    input,select,textarea,button { font:inherit; }
    input,select,textarea { border:1px solid #c8d0de; border-radius:7px; padding:7px 9px; background:#fff; color:var(--ink); }
    textarea { width:100%; min-height:150px; font-family:ui-monospace,SFMono-Regular,Consolas,monospace; font-size:12px; }
    button { border:0; border-radius:7px; padding:8px 12px; background:var(--accent); color:#fff; cursor:pointer; font-weight:600; }
    button.secondary { background:#e8eef8; color:#17365f; } button.danger { background:#fbe9e7; color:var(--bad); }
    .grid4 { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:12px; }
    .metric { border:1px solid var(--line); border-radius:9px; padding:12px; background:#fbfcfe; }
    .metric .k { color:var(--muted); font-size:12px; } .metric .v { font-size:24px; font-weight:700; margin-top:3px; }
    h2 { font-size:16px; margin:0 0 12px; } .muted { color:var(--muted); }
    table { width:100%; border-collapse:collapse; } th,td { border-bottom:1px solid var(--line); padding:8px; text-align:left; vertical-align:top; }
    th { font-size:12px; color:var(--muted); background:#f8fafd; position:sticky; top:0; }
    .scroll { max-height:420px; overflow:auto; border:1px solid var(--line); border-radius:8px; }
    .state { display:inline-block; border-radius:99px; padding:2px 8px; font-size:12px; font-weight:700; background:#eef2f7; }
    .OPEN,.HALF_CLOSED_LOCAL,.HALF_CLOSED_REMOTE { background:#e7f6ee; color:var(--ok); }
    .RESET,.CLOSED { background:#fbe9e7; color:var(--bad); } .IDLE { background:#fff4db; color:var(--warn); }
    .error { border:1px solid #f3b9b3; background:#fff3f1; color:var(--bad); border-radius:8px; padding:10px; white-space:pre-wrap; }
    .ok { color:var(--ok); font-weight:700; } .bad { color:var(--bad); font-weight:700; }
    .two { display:grid; grid-template-columns:1.05fr .95fr; gap:16px; }
    .mini { font-size:12px; color:var(--muted); } code { background:#eef2f7; padding:1px 4px; border-radius:4px; }
    @media (max-width:1000px) { .grid4 { grid-template-columns:repeat(2,1fr); } .two { grid-template-columns:1fr; } }
  </style>
</head>
<body>
<header><h1>HTTP/2 多路复用诊断工作台</h1><p>本地回环 · 连接/流窗口 · 加权调度轨迹 · 崩溃恢复边界</p></header>
<main>
  <section class="toolbar">
    <label>连接<select id="connectionSelect"><option value="">选择连接</option></select></label>
    <button id="refreshBtn">刷新快照</button>
    <button class="secondary" id="sampleBtn">载入演示样本</button>
    <button class="danger" id="closeBtn">关闭连接</button>
    <span id="statusLine" class="mini"></span>
  </section>

  <section class="grid4">
    <div class="metric"><div class="k">连接窗口余量</div><div class="v" id="connWindow">-</div><div class="mini" id="connWindowInit"></div></div>
    <div class="metric"><div class="k">流数量</div><div class="v" id="streamCount">-</div><div class="mini" id="closedFlag"></div></div>
    <div class="metric"><div class="k">待发送 DATA</div><div class="v" id="pendingBytes">-</div><div class="mini" id="pendingJobs"></div></div>
    <div class="metric"><div class="k">协议违例</div><div class="v" id="violations">-</div><div class="mini">超上限 WINDOW_UPDATE 按初始窗口截断</div></div>
  </section>

  <div id="errorBox" class="error" style="display:none"></div>

  <section class="two">
    <div class="card">
      <h2>流窗口与状态</h2>
      <div class="scroll"><table><thead><tr><th>流</th><th>状态</th><th>流窗口</th><th>初始窗口</th><th>未确认</th><th>待发送</th><th>优先级/权重</th><th>累计发送</th></tr></thead><tbody id="streamRows"></tbody></table></div>
    </div>
    <div class="card">
      <h2>样本提交与调度</h2>
      <textarea id="sampleJson" spellcheck="false"></textarea>
      <div style="display:flex;gap:8px;margin-top:8px;flex-wrap:wrap">
        <button id="submitBtn">提交样本</button>
        <button class="secondary" id="scheduleBtn">调度当前样本</button>
        <button class="secondary" id="scheduleConnBtn">调度连接全部待发</button>
      </div>
      <p class="mini">演示样本使用固定本地帧序列；不会访问外部网络。窗口不足时接口返回 <code>E_FLOW_CONTROL_BLOCKED</code>，轨迹仍可在下方查询。</p>
    </div>
  </section>

  <section class="card">
    <h2>本轮调度次序与阻塞原因</h2>
    <div class="scroll"><table><thead><tr><th>轮次</th><th>顺序</th><th>流</th><th>字节</th><th>优先级/权重</th><th>状态迁移</th><th>选择依据</th></tr></thead><tbody id="traceRows"></tbody></table></div>
    <h2 style="margin-top:16px">阻塞诊断</h2>
    <div class="scroll"><table><thead><tr><th>轮次</th><th>流</th><th>业务码</th><th>连接余量</th><th>流余量</th><th>所需</th><th>原因</th></tr></thead><tbody id="blockedRows"></tbody></table></div>
  </section>
</main>
<script>
const $ = (id) => document.getElementById(id);
let currentSampleId = null;
const escapeHtml = (s) => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function showError(value) {
  const box = $('errorBox');
  if (!value) { box.style.display = 'none'; box.textContent = ''; return; }
  box.style.display = 'block';
  box.textContent = typeof value === 'string' ? value : JSON.stringify(value, null, 2);
}

async function api(path, options = {}) {
  const response = await fetch(path, {headers: {'Content-Type': 'application/json'}, ...options});
  let body;
  try { body = await response.json(); } catch { body = {error: {code: 'E_HTTP', message: await response.text()}}; }
  if (!response.ok) { showError(body); throw new Error(body?.error?.code || response.status); }
  showError(null);
  return body;
}

async function loadConnections(selectId) {
  const data = await api('/api/connections');
  const select = $('connectionSelect');
  select.innerHTML = '<option value="">选择连接</option>' + data.connections.map(c =>
    `<option value="${escapeHtml(c.connection_id)}">${escapeHtml(c.connection_id)} · 窗口 ${c.connection_window}</option>`).join('');
  if (selectId) select.value = selectId;
  else if (data.connections.length) select.value = data.connections[0].connection_id;
}

async function refresh() {
  const connectionId = $('connectionSelect').value;
  if (!connectionId) {
    $('streamRows').innerHTML = '<tr><td colspan="8" class="muted">暂无连接，可在右侧提交样本。</td></tr>';
    return;
  }
  const [snap, trace] = await Promise.all([
    api(`/api/connections/${encodeURIComponent(connectionId)}`),
    api(`/api/connections/${encodeURIComponent(connectionId)}/trace?limit=10`)
  ]);
  $('connWindow').textContent = snap.connection_window.toLocaleString();
  $('connWindowInit').textContent = `初始 ${snap.initial_connection_window.toLocaleString()}`;
  $('streamCount').textContent = snap.streams.length;
  $('closedFlag').textContent = snap.closed ? '连接已关闭' : '连接可用';
  $('pendingBytes').textContent = snap.pending_bytes.toLocaleString();
  $('pendingJobs').textContent = `${snap.pending_jobs} 个未完成发送任务`;
  $('violations').textContent = snap.protocol_violations;
  $('streamRows').innerHTML = snap.streams.map(s => `<tr>
    <td>${s.stream_id}</td><td><span class="state ${escapeHtml(s.state)}">${escapeHtml(s.state)}</span></td>
    <td>${s.window.toLocaleString()}</td><td>${s.initial_window.toLocaleString()}</td><td>${s.sent_unacked.toLocaleString()}</td>
    <td>${s.pending_bytes.toLocaleString()}</td><td>${s.priority} / ${s.weight}</td><td>${s.sent_total.toLocaleString()}</td></tr>`).join('')
    || '<tr><td colspan="8" class="muted">暂无流。</td></tr>';
  const runs = trace.runs || [];
  const latest = runs.length ? runs[runs.length - 1] : null;
  currentSampleId = latest?.sample_id || currentSampleId;
  const entries = latest?.trace || [];
  $('traceRows').innerHTML = entries.map(e => `<tr><td>${e.round_index}</td><td>${e.seq}</td><td>${e.stream_id}</td><td>${e.bytes.toLocaleString()}</td><td>${e.priority} / ${e.weight}</td><td>${escapeHtml(e.state_before)} → ${escapeHtml(e.state_after)}</td><td class="mini">${escapeHtml(e.reason)}</td></tr>`).join('')
    || '<tr><td colspan="7" class="muted">尚无已提交调度轨迹。</td></tr>';
  const blocked = latest?.blocked || [];
  $('blockedRows').innerHTML = blocked.map(e => `<tr><td>${e.round_index}</td><td>${e.stream_id}</td><td class="bad">${escapeHtml(e.code)}</td><td>${e.connection_window}</td><td>${e.stream_window}</td><td>${e.required_bytes}</td><td>${escapeHtml(e.reason)}</td></tr>`).join('')
    || '<tr><td colspan="7" class="muted">无阻塞。</td></tr>';
  const sum = runs.map(r => `${r.run_id}:${r.status}`).join(' | ');
  $('statusLine').textContent = sum ? `最近运行 ${sum}` : '尚未触发调度';
}

$('refreshBtn').onclick = () => refresh().catch(()=>{});
$('connectionSelect').onchange = () => refresh().catch(()=>{});
$('sampleBtn').onclick = async () => {
  const demo = await api('/api/demo/sample');
  $('sampleJson').value = JSON.stringify(demo, null, 2);
  $('connectionSelect').value = demo.connection_id;
};
$('submitBtn').onclick = async () => {
  try {
    const payload = JSON.parse($('sampleJson').value);
    const result = await api('/api/samples', {method:'POST', body: JSON.stringify(payload)});
    currentSampleId = result.sample.sample_id;
    await loadConnections(result.sample.connection_id);
    await refresh();
    $('statusLine').textContent = `已提交 ${result.sample.sample_id}，待发送 ${result.queued_data.bytes.toLocaleString()} 字节`;
  } catch (e) { if (!(e instanceof SyntaxError)) return; showError('样本 JSON 解析失败: ' + e.message); }
};
$('scheduleBtn').onclick = async () => {
  if (!currentSampleId) { showError('请先提交样本或选择已有连接轨迹。'); return; }
  try {
    await api(`/api/samples/${encodeURIComponent(currentSampleId)}/schedule`, {method:'POST', body:'{}'});
  } catch (e) { /* 错误体已展示，仍刷新已提交轨迹 */ }
  await loadConnections($('connectionSelect').value);
  await refresh();
};
$('scheduleConnBtn').onclick = async () => {
  const id = $('connectionSelect').value;
  if (!id) return;
  try { await api(`/api/connections/${encodeURIComponent(id)}/schedule`, {method:'POST', body:'{}'}); }
  catch (e) { /* 错误体已展示 */ }
  await refresh();
};
$('closeBtn').onclick = async () => {
  const id = $('connectionSelect').value;
  if (!id || !confirm(`关闭连接 ${id}？`)) return;
  await api(`/api/connections/${encodeURIComponent(id)}/close`, {method:'POST'});
  await refresh();
};

(async () => {
  try {
    const demo = await api('/api/demo/sample');
    $('sampleJson').value = JSON.stringify(demo, null, 2);
    await loadConnections();
    await refresh();
  } catch (e) { showError(String(e)); }
})();
</script>
</body>
</html>
"""
