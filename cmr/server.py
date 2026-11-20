"""Local HTTP workbench: FastAPI app + diagnostic page."""

import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .errors import CmrError
from .inject import INJECTION_KINDS
from .service import Workbench

HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>跨模态检索索引工作台</title>
<style>
 body{font-family:ui-monospace,Menlo,Consolas,monospace;margin:24px;background:#0f1420;color:#dfe6f3}
 h1{font-size:20px} h2{font-size:15px;color:#8fd3ff;margin-top:28px}
 section{background:#171e30;border:1px solid #2a3550;border-radius:8px;padding:14px;margin:10px 0}
 table{border-collapse:collapse;width:100%;font-size:12px}
 td,th{border:1px solid #2a3550;padding:4px 8px;text-align:left;vertical-align:top}
 th{background:#1d2740}
 input,select,button{background:#0d1322;color:#dfe6f3;border:1px solid #3a4a70;border-radius:4px;padding:6px 8px;font:inherit}
 button{cursor:pointer;background:#1d2c4d}
 button:hover{background:#27406e}
 pre{background:#0b101d;padding:10px;border-radius:6px;overflow:auto;font-size:11px;max-height:320px}
 .ok{color:#7ee787}.bad{color:#ff7b72}.dim{color:#8b98b8}
 img.thumb{width:48px;height:48px;image-rendering:pixelated;border-radius:4px;vertical-align:middle}
 .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin:6px 0}
 code{color:#ffd479}
</style>
</head>
<body>
<h1>图文跨模态检索索引工作台 <span class="dim">cmr workbench</span></h1>

<section>
<h2>操作</h2>
<div class="row">
 <button onclick="callApi('POST','/api/build')">触发构建</button>
 <select id="fromStage"><option value="">自动(增量)</option><option>features</option>
 <option>projection</option><option>index</option><option>recall</option></select>
 <button onclick="callApi('POST','/api/stop')">请求停止</button>
 <button onclick="callApi('POST','/api/resume')">恢复构建</button>
 <select id="injectKind"></select>
 <button onclick="doInject()">注入异常</button>
 <button onclick="refresh()">刷新状态</button>
</div>
<pre id="opResult">// 操作结果</pre>
</section>

<section>
<h2>文本检索图像</h2>
<div class="row">
 <input id="qtext" size="46" placeholder="例如:红色圆形 red circle 渐变背景">
 <input id="qk_t" size="3" value="3"> <button onclick="doQueryText()">查询</button>
</div>
<div id="t2iResults"></div>
</section>

<section>
<h2>图像检索文本</h2>
<div class="row">
 <select id="qimg"></select>
 <input id="qk_i" size="3" value="3"> <button onclick="doQueryImage()">查询</button>
</div>
<div id="i2tResults"></div>
</section>

<section>
<h2>索引规模与阶段指纹</h2>
<div id="indexMeta"></div>
<div id="stageFps"></div>
</section>

<section>
<h2>失效传播与重建范围</h2>
<div id="invalidation"></div>
</section>

<section>
<h2>注入记录</h2>
<div id="injections"></div>
</section>

<section>
<h2>检查点</h2>
<div id="checkpoints"></div>
</section>

<script>
const INJECT_KINDS = __INJECT_KINDS__;
const $ = id => document.getElementById(id);
const fmt = o => JSON.stringify(o, null, 2);

async function callApi(method, url, body) {
  const from = $('fromStage').value;
  if (url === '/api/build' && from) url += '?from_stage=' + from;
  const opt = {method, headers:{'Content-Type':'application/json'}};
  if (body) opt.body = JSON.stringify(body);
  const r = await fetch(url, opt);
  const j = await r.json();
  $('opResult').textContent = fmt(j);
  refresh();
  return j;
}
function doInject(){ callApi('POST','/api/inject/' + $('injectKind').value); }

function renderHits(hits){
  if(!hits || !hits.length) return '<span class="dim">无</span>';
  return hits.map(h=>`<code>${h.term}</code>(w=${h.contribution.toFixed(4)})`).join(' ');
}

function renderT2I(j){
  if(j.error){ $('t2iResults').innerHTML = '<pre class="bad">'+fmt(j)+'</pre>'; return; }
  let h = '<table><tr><th>#</th><th>图像</th><th>分片</th><th>score</th><th>cosine</th><th>倒排权重</th><th>命中项</th></tr>';
  for(const r of j.results){
    h += `<tr><td>${r.rank}</td><td><img class="thumb" src="/fragments/${r.image_file.split('/').pop()}"></td>`+
         `<td>${r.shard}</td><td>${r.score.toFixed(6)}</td><td>${r.cosine.toFixed(6)}</td>`+
         `<td>${r.inverted.weight.toFixed(6)}</td><td>${renderHits(r.inverted.hits.slice(0,6))}</td></tr>`;
  }
  $('t2iResults').innerHTML = h + '</table>';
}
function renderI2T(j){
  if(j.error){ $('i2tResults').innerHTML = '<pre class="bad">'+fmt(j)+'</pre>'; return; }
  let h = '<table><tr><th>#</th><th>分片</th><th>caption</th><th>score</th><th>cosine</th><th>倒排权重</th></tr>';
  for(const r of j.results){
    h += `<tr><td>${r.rank}</td><td>${r.shard}</td><td>${r.caption}</td>`+
         `<td>${r.score.toFixed(6)}</td><td>${r.cosine.toFixed(6)}</td><td>${r.inverted.weight.toFixed(6)}</td></tr>`;
  }
  $('i2tResults').innerHTML = h + '</table>';
}
async function doQueryText(){
  const j = await callApi('POST','/api/query/text',{text:$('qtext').value,k:parseInt($('qk_t').value)});
  renderT2I(j);
}
async function doQueryImage(){
  const j = await callApi('POST','/api/query/image',{image:'pair:'+$('qimg').value,k:parseInt($('qk_i').value)});
  renderI2T(j);
}

async function refresh(){
  const s = await (await fetch('/api/status')).json();
  const m = s.index_meta;
  $('indexMeta').innerHTML = m ?
    `<table><tr><th>分片数</th><th>向量数</th><th>文本词项</th><th>视觉词项</th><th>索引摘要</th></tr>
     <tr><td>${m.num_shards}</td><td>${m.num_vectors}</td><td>${m.num_text_terms}</td>
     <td>${m.num_visual_terms}</td><td><code>${m.index_digest.slice(0,16)}…</code></td></tr></table>`
    : '<span class="dim">索引尚未构建</span>';
  const fps = s.current_stage_fps || {}, pub = s.published_stage_fps || {};
  let fh = '<table><tr><th>阶段</th><th>当前指纹</th><th>已发布指纹</th><th>状态</th></tr>';
  for(const st of ['features','projection','index','recall']){
    const cur = fps[st]||'-', pb = pub[st]||'-';
    const same = cur===pb;
    fh += `<tr><td>${st}</td><td><code>${cur.slice(0,14)}…</code></td><td><code>${pb.slice(0,14)}…</code></td>
           <td class="${same?'ok':'bad'}">${same?'一致':'失效'}</td></tr>`;
  }
  $('stageFps').innerHTML = fh + '</table>' +
    `<p>召回校验: ${s.recall ? (s.recall.ok?'<span class="ok">通过</span>':'<span class="bad">失败</span>')+' digest='+s.recall.digest.slice(0,16)+'…' : '<span class="dim">无</span>'}</p>';
  const inv = s.last_invalidation;
  $('invalidation').innerHTML = inv ?
    `<table><tr><th>原因</th><th>重建范围</th></tr><tr><td>${(inv.reason||[]).join('<br>')}</td>
     <td>${(inv.rebuild_scope||[]).join(' → ')||'-'}</td></tr></table>` : '<span class="dim">无</span>';
  const injs = s.injections||[];
  $('injections').innerHTML = injs.length ?
    '<table><tr><th>注入编号</th><th>类型</th><th>业务码</th><th>阶段</th><th>位置</th></tr>' +
    injs.map(i=>`<tr><td>${i.inject_id}</td><td>${i.kind}</td><td class="bad">${i.result?i.result.code:'-'}</td>
      <td>${i.result?(i.result.stage||'-'):'-'}</td><td>${i.result?(i.result.position||'-'):'-'}</td></tr>`).join('') + '</table>'
    : '<span class="dim">无注入记录</span>';
  const cks = s.checkpoints||[];
  $('checkpoints').innerHTML = cks.length ?
    '<table><tr><th>seq</th><th>分片</th><th>阶段</th><th>产物摘要</th></tr>' +
    cks.slice(-12).map(c=>`<tr><td>${c.seq}</td><td>${c.shard}</td><td>${c.stage}</td>
      <td><code>${(c.artifact_digest||'').slice(0,16)}…</code></td></tr>`).join('') + '</table>'
    : '<span class="dim">无检查点</span>';
  if(m && !$('qimg').options.length){
    for(let i=0;i<m.num_shards;i++){
      const o=document.createElement('option');
      o.value='pair_'+String(i).padStart(2,'0'); o.textContent=o.value;
      $('qimg').appendChild(o);
    }
  }
}

(async function init(){
  $('injectKind').innerHTML = INJECT_KINDS.map(k=>`<option>${k}</option>`).join('');
  await refresh();
})();
</script>
</body>
</html>
"""


def create_app(data_dir="data", state_dir="state"):
    wb = Workbench(data_dir, state_dir)
    app = FastAPI(title="cmr-workbench")

    @app.exception_handler(CmrError)
    async def cmr_error_handler(_request: Request, exc: CmrError):
        return JSONResponse(status_code=422, content=exc.to_dict())

    @app.get("/", response_class=HTMLResponse)
    async def index_page():
        html = HTML_PAGE.replace("__INJECT_KINDS__", repr(INJECTION_KINDS))
        return HTMLResponse(html)

    @app.post("/api/build")
    async def build(from_stage: str = None, stop_after: int = None):
        return wb.build(from_stage=from_stage, stop_after=stop_after)

    @app.post("/api/pairs/import")
    async def import_pairs(payload: dict):
        return wb.import_pairs(payload["jsonl"])

    @app.post("/api/query/text")
    async def query_text_api(payload: dict):
        return wb.query_text(payload.get("text", ""), payload.get("k"))

    @app.post("/api/query/image")
    async def query_image_api(payload: dict):
        if "vector" in payload:
            return wb.query_image_vector(payload["vector"], payload.get("k"))
        return wb.query_image(payload.get("image", ""), payload.get("k"))

    @app.get("/api/status")
    async def status():
        return wb.status()

    @app.get("/api/injections")
    async def injections():
        return wb.status()["injections"]

    @app.post("/api/inject/{kind}")
    async def inject(kind: str):
        return wb.inject(kind)

    @app.post("/api/stop")
    async def stop():
        return wb.stop()

    @app.post("/api/resume")
    async def resume():
        return wb.resume()

    @app.post("/api/publish")
    async def publish(payload: dict):
        return wb.publish(payload["record"], payload.get("expected_seq"))

    frag_dir = os.path.join(data_dir, "fragments")
    os.makedirs(frag_dir, exist_ok=True)
    app.mount("/fragments", StaticFiles(directory=frag_dir), name="fragments")
    return app
