"""Read-only command-line and static HTML diagnostics rendering."""

from __future__ import annotations

import base64
import html
from pathlib import Path
from typing import Any, Iterable

from .checkpoints import CheckpointStore
from .config import WorkbenchConfig
from .pipeline import ShardDiagnostic


def render_text(diagnostics: Iterable[ShardDiagnostic]) -> str:
    lines: list[str] = []
    for diagnostic in diagnostics:
        shard = diagnostic.shard
        earliest = diagnostic.earliest_invalid or "none"
        lines.append(
            f"[{shard.id}] earliest_invalid={earliest} png={diagnostic.png_sha256 or '<missing>'}"
        )
        rebuild = [stage.stage for stage in diagnostic.stages if stage.invalid]
        reuse = [stage.stage for stage in diagnostic.stages if not stage.invalid]
        lines.append(f"  rebuild_plan={','.join(rebuild) or 'none'} reuse_plan={','.join(reuse) or 'none'}")
        for stage in diagnostic.stages:
            state = "OK" if not stage.invalid else "INVALID"
            sequence = stage.record.sequence if stage.record is not None else "-"
            fingerprint = (
                stage.record.input_fingerprint[:12] if stage.record is not None else "-"
            )
            line = f"  {stage.stage:8s} {state:7s} seq={sequence:>3} fp={fingerprint}"
            if stage.invalid and stage.reason:
                line += f" reason={stage.reason}"
            if stage.changed_fields:
                line += f" changed={','.join(stage.changed_fields)}"
            if stage.trajectory:
                first = stage.trajectory[0]
                last = stage.trajectory[-1]
                line += (
                    f" trajectory={len(stage.trajectory)} steps"
                    f" mean[{first['mean']}..{last['mean']}]"
                )
            if stage.clamp_counts is not None:
                line += f" clamps=pre:{stage.clamp_counts['pre']}/post:{stage.clamp_counts['post']}"
            lines.append(line)
    return "\n".join(lines)


def write_html_dashboard(
    diagnostics: Iterable[ShardDiagnostic],
    path: Path,
    *,
    config: WorkbenchConfig | None = None,
) -> None:
    diagnostics = list(diagnostics)
    store = CheckpointStore(config.root) if config is not None else None
    cards: list[str] = []
    for diagnostic in diagnostics:
        shard = diagnostic.shard
        earliest = diagnostic.earliest_invalid or "none"
        stage_badges = " <span class=\"arrow\">→</span> ".join(
            f"<span class=\"badge {'bad' if stage.invalid else 'ok'}\">{html.escape(stage.stage)}</span>"
            for stage in diagnostic.stages
        )
        stage_rows: list[str] = []
        trajectory_rows: list[str] = []
        for stage in diagnostic.stages:
            record = stage.record
            changed = ", ".join(stage.changed_fields) if stage.changed_fields else "—"
            stage_rows.append(
                "<tr>"
                f"<td>{html.escape(stage.stage)}</td>"
                f"<td class=\"{'bad-text' if stage.invalid else 'ok-text'}\">{'INVALID' if stage.invalid else 'VALID'}</td>"
                f"<td>{record.sequence if record is not None else '—'}</td>"
                f"<td class=\"mono\">{(record.input_fingerprint[:16] + '…') if record is not None else '—'}</td>"
                f"<td>{html.escape(stage.reason or '—')}</td>"
                f"<td>{html.escape(changed)}</td>"
                f"<td>{html.escape(stage.invalidated_by or '—')}</td>"
                "</tr>"
            )
            for point in stage.trajectory:
                trajectory_rows.append(
                    "<tr>"
                    f"<td>{html.escape(stage.stage)}</td>"
                    f"<td>{int(point['step'])}</td>"
                    f"<td class=\"mono\">{float(point['mean']):.12f}</td>"
                    f"<td class=\"mono\">{float(point['variance']):.12f}</td>"
                    "</tr>"
                )

        png_html = "<div class=\"missing\">PNG 缺失或尚未生成</div>"
        if store is not None:
            output = store.output_path(shard.id)
            if output.is_file():
                payload = base64.b64encode(output.read_bytes()).decode("ascii")
                png_html = (
                    f"<img class=\"preview\" alt=\"shard {html.escape(shard.id)} preview\" "
                    f"src=\"data:image/png;base64,{payload}\">"
                )
        clamp = next((stage.clamp_counts for stage in diagnostic.stages if stage.clamp_counts), None)
        clamp_text = (
            f"pre-clamp {clamp['pre']} / post-clamp {clamp['post']}" if clamp else "尚未量化"
        )
        cards.append(
            f"""
<section class="card">
  <div class="card-head">
    <div>
      <h2>{html.escape(shard.id)}</h2>
      <p class="prompt">{html.escape(shard.prompt)}</p>
    </div>
    {png_html}
  </div>
  <div class="meta">
    <span>guidance <b>{shard.guidance_scale}</b></span>
    <span>steps <b>{shard.steps}</b></span>
    <span>seed <b>{shard.seed}</b></span>
    <span>reference <b>{html.escape(shard.reference or 'none')}</b></span>
    <span>blend <b>{shard.blend_strength}</b></span>
    <span>clamp <b>{html.escape(clamp_text)}</b></span>
  </div>
  <h3>失效链</h3>
  <div class="chain">{stage_badges}</div>
  <p class="summary">最早失效阶段：<b>{html.escape(earliest)}</b> · PNG SHA-256：<span class="mono">{html.escape(diagnostic.png_sha256 or '—')}</span></p>
  {f'<p class="error">参考图错误：{html.escape(diagnostic.reference_error)}</p>' if diagnostic.reference_error else ''}
  <h3>阶段状态</h3>
  <div class="scroll"><table>
    <thead><tr><th>阶段</th><th>状态</th><th>序号</th><th>输入指纹</th><th>原因</th><th>变化字段</th><th>失效来源</th></tr></thead>
    <tbody>{''.join(stage_rows)}</tbody>
  </table></div>
  <details><summary>逐步轨迹摘要 ({sum(len(stage.trajectory) for stage in diagnostic.stages)} 步)</summary>
    <div class="scroll"><table>
      <thead><tr><th>阶段</th><th>步</th><th>均值</th><th>方差</th></tr></thead>
      <tbody>{''.join(trajectory_rows)}</tbody>
    </table></div>
  </details>
</section>"""
        )

    document = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DetDiff 本地诊断工作台</title>
<style>
:root {{ color-scheme: dark; --bg:#0b1020; --panel:#141b31; --line:#2a3553; --text:#e7ecff; --muted:#98a4c8; --ok:#58d68d; --bad:#ff6b81; --accent:#77a8ff; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; padding:28px; background:radial-gradient(circle at top left,#182449,#0b1020 55%); color:var(--text); font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif; }}
header {{ max-width:1220px; margin:0 auto 22px; }}
h1 {{ margin:0 0 6px; font-size:25px; letter-spacing:.02em; }}
.sub {{ color:var(--muted); max-width:900px; }}
main {{ max-width:1220px; margin:0 auto; display:grid; gap:18px; }}
.card {{ background:linear-gradient(145deg,rgba(26,36,64,.96),rgba(17,24,44,.96)); border:1px solid var(--line); border-radius:16px; padding:20px; box-shadow:0 18px 50px rgba(0,0,0,.25); }}
.card-head {{ display:flex; justify-content:space-between; gap:20px; align-items:flex-start; }}
h2 {{ margin:0; font-size:20px; }}
h3 {{ font-size:13px; color:var(--muted); text-transform:uppercase; letter-spacing:.09em; margin:18px 0 8px; }}
.prompt {{ margin:5px 0 0; color:#c8d3f3; }}
.preview {{ width:144px; height:144px; image-rendering:pixelated; border-radius:10px; border:1px solid #3d4d76; background:#080c18; }}
.missing {{ width:144px; min-height:64px; display:grid; place-items:center; color:var(--bad); border:1px dashed #633040; border-radius:10px; padding:10px; text-align:center; }}
.meta {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:14px; }}
.meta span {{ background:#0d1427; border:1px solid var(--line); border-radius:999px; padding:4px 10px; color:var(--muted); }}
.meta b {{ color:var(--text); }}
.chain {{ display:flex; align-items:center; flex-wrap:wrap; gap:6px; }}
.arrow {{ color:var(--muted); }}
.badge {{ border-radius:8px; padding:5px 11px; border:1px solid; font-weight:650; }}
.badge.ok {{ color:var(--ok); border-color:#2e6c4b; background:#102a1e; }}
.badge.bad {{ color:var(--bad); border-color:#733444; background:#2b121a; }}
.summary {{ color:var(--muted); }}
.error {{ color:var(--bad); background:#2b121a; border:1px solid #733444; border-radius:9px; padding:8px 10px; }}
.ok-text {{ color:var(--ok); }} .bad-text {{ color:var(--bad); font-weight:650; }}
.mono {{ font-family:ui-monospace,SFMono-Regular,Consolas,monospace; font-size:12px; overflow-wrap:anywhere; }}
.scroll {{ overflow:auto; max-width:100%; }}
table {{ width:100%; border-collapse:collapse; min-width:760px; }}
th,td {{ text-align:left; padding:8px 9px; border-bottom:1px solid #232e4b; vertical-align:top; }}
th {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.06em; }}
details {{ margin-top:12px; }} summary {{ cursor:pointer; color:var(--accent); }}
footer {{ max-width:1220px; margin:22px auto 0; color:var(--muted); font-size:12px; }}
</style>
</head>
<body>
<header>
  <h1>DetDiff 本地确定性扩散管线</h1>
  <div class="sub">只读诊断快照：阶段指纹、失效传播、逐步轨迹、量化钳制与当前 PNG 摘要。页面不包含外部资源或网络依赖。</div>
</header>
<main>{''.join(cards)}</main>
<footer>生成方式：detdiff diagnostics --workspace &lt;workspace&gt; --html &lt;file&gt; · 查询过程不触发构建</footer>
</body>
</html>
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(document, encoding="utf-8")

