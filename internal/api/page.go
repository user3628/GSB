package api

const indexHTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CSRouter 意图与优先级联判控制台</title>
<style>
body{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;margin:2rem;color:#172033;background:#f7f8fb}
main{max-width:1080px;margin:auto}
section{background:#fff;border:1px solid #dfe4ee;border-radius:12px;padding:1rem 1.25rem;margin:1rem 0;box-shadow:0 1px 3px #0001}
button{border:0;border-radius:8px;background:#2456d6;color:#fff;padding:.55rem .9rem;margin:.2rem;cursor:pointer}
button.secondary{background:#475569}button.danger{background:#b42318}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
pre{background:#0b1220;color:#d7e3ff;padding:1rem;border-radius:10px;overflow:auto;max-height:420px}
.muted{color:#667085}.row{display:flex;gap:.6rem;flex-wrap:wrap;align-items:center}
input,select{padding:.45rem;border:1px solid #cbd5e1;border-radius:8px}
</style>
</head>
<body><main>
<h1>CSRouter 确定性联判服务</h1>
<p class="muted">仅绑定回环地址。固定种子生成两档 JSONL，执行规范化、意图、优先级、验证汇总与参考比对。</p>
<section>
<div class="row">
<label>档位 <select id="tier"><option value="small">small / 400 / 8</option><option value="medium">medium / 4000 / 24</option></select></label>
<button onclick="post('/api/generate')">1. 生成并导入</button>
<button onclick="get('/api/validate')">校验行列</button>
<button onclick="post('/api/run')">2. 全量执行</button>
<button class="secondary" onclick="post('/api/resume')">恢复执行</button>
<button class="danger" onclick="post('/api/stop')">优雅停止</button>
</div>
<div class="row">
<button onclick="get('/api/status')">阶段状态/指纹</button>
<button onclick="get('/api/strategy')">策略取舍</button>
<button onclick="get('/api/priority-details?comparison=MISS')">漏判明细</button>
<button onclick="get('/api/priority-details?comparison=FALSE_POSITIVE')">误判明细</button>
<button onclick="get('/api/report')">验证报告</button>
<button class="secondary" onclick="postJSON('/api/shards/replace',{demo:true})">单分片替换</button>
<button class="secondary" onclick="changeAlpha()">平滑强度改为 0.5</button>
<button class="secondary" onclick="zeroAlpha()">制造 alpha=0 错误</button>
</div>
</section>
<section><pre id="out">就绪。响应将显示在这里。</pre></section>
</main>
<script>
function tier(){return document.getElementById('tier').value}
function show(x){document.getElementById('out').textContent=typeof x==='string'?x:JSON.stringify(x,null,2)}
async function get(path){const r=await fetch(path+(path.includes('?')?'&':'?')+'tier='+encodeURIComponent(tier()));show(await r.json())}
async function post(path){const r=await fetch(path+'?tier='+encodeURIComponent(tier()),{method:'POST'});show(await r.json())}
async function postJSON(path,body){const r=await fetch(path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});show(await r.json())}
function changeAlpha(){postJSON('/api/config/change',{tier:tier(),key:'smoothing_alpha',value:0.5,note:'UI deterministic change'})}
function zeroAlpha(){postJSON('/api/config/change',{tier:tier(),key:'smoothing_alpha',value:0,note:'trigger structured error'})}
</script>
</body></html>`
