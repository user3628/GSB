"""Deterministic report exports and the self-contained HTML diagnostic
workbench (no external assets, no network access).
"""
from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict

from .config import canonical_json
from .checkpoints import list_checkpoints


def _reports_dir(workspace: str) -> str:
    directory = os.path.join(workspace, "reports")
    os.makedirs(directory, exist_ok=True)
    return directory


def export_json_report(workspace: str, state: Dict[str, Any]) -> str:
    path = os.path.join(_reports_dir(workspace), "report.json")
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(state, sort_keys=True, separators=(",", ":"),
                                ensure_ascii=False, allow_nan=False))
    return path


def export_error_curves(workspace: str) -> str:
    """One row per (fold, forecast step) so error curves can be plotted."""
    path = os.path.join(_reports_dir(workspace), "error_curves.csv")
    checkpoints = list_checkpoints(workspace)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["shard_id", "series_id", "fold_index", "step",
                         "timestamp_ms", "actual", "prediction", "error",
                         "abs_error", "squared_error", "interval_covered",
                         "has_changepoint"])
        for record in checkpoints:
            for fold in sorted(record["results"],
                               key=lambda item: (item["series_id"],
                                                 item["fold_index"])):
                for step in range(len(fold["predictions"])):
                    writer.writerow([
                        fold["shard_id"], fold["series_id"],
                        fold["fold_index"], step,
                        fold["timestamps"][step],
                        fold["actual"][step],
                        fold["predictions"][step],
                        fold["errors"][step],
                        fold["abs_errors"][step],
                        fold["squared_errors"][step],
                        int(fold["interval_covered"][step]),
                        int(fold["has_changepoint"]),
                    ])
    return path


def export_budget_details(workspace: str, state: Dict[str, Any]) -> str:
    path = os.path.join(_reports_dir(workspace), "budget_details.csv")
    checkpoints = list_checkpoints(workspace)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["category", "shard_id", "series_id", "fold_index",
                         "worker_id", "code", "detail"])
        for record in checkpoints:
            for fold in record["results"]:
                writer.writerow(["accepted", fold["shard_id"],
                                 fold["series_id"], fold["fold_index"],
                                 fold["worker_id"], "OK",
                                 fold["method"]])
            for failure in record["failures"]:
                writer.writerow(["failed", failure["shard_id"],
                                 failure["series_id"],
                                 failure["fold_index"],
                                 failure["worker_id"], failure["code"],
                                 failure["message"]])
        for rejected in state.get("rejected_folds", []):
            writer.writerow(["rejected", rejected["shard_id"],
                             rejected["series_id"], rejected["fold_index"],
                             "", rejected["code"], rejected["reason"]])
        for not_started in state.get("not_started_folds", []):
            writer.writerow(["not_started", not_started["shard_id"],
                             not_started["series_id"],
                             not_started["fold_index"],
                             not_started.get("worker_id", ""),
                             not_started["code"], not_started["reason"]])
    return path


def export_html_dashboard(workspace: str, state: Dict[str, Any]) -> str:
    path = os.path.join(_reports_dir(workspace), "dashboard.html")
    payload = canonical_json(state)
    html = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>时序预测滚动回测诊断工作台</title>
<style>
body { font-family: Segoe UI, Arial, sans-serif; margin: 24px; color:#1f2933; }
h1 { font-size: 20px; } h2 { font-size: 16px; margin-top: 28px; }
table { border-collapse: collapse; margin: 8px 0; font-size: 13px; }
th, td { border: 1px solid #cbd2d9; padding: 4px 10px; text-align: right; }
th { background: #f5f7fa; } td.label, th.label { text-align: left; }
.ok { color:#0b6e4f; } .warn { color:#b35900; } .err { color:#b00020; }
pre { background:#f5f7fa; padding:12px; overflow:auto; font-size:12px; }
canvas { border:1px solid #cbd2d9; margin:8px 0; background:#fff; }
</style>
</head>
<body>
<h1>时序预测滚动回测与并行调度诊断工作台</h1>
<p>本页面为本地自包含报告，无外部资源与网络访问。</p>
<div id="root"></div>
<script>
const STATE = __PAYLOAD__;
const fmt = (v) => v === null || v === undefined ? "—"
    : (typeof v === "number" ? v.toFixed(6) : String(v));
function table(headers, rows, labelCols=[]) {
  let html = "<table><thead><tr>" + headers.map(h =>
    `<th class="${labelCols.includes(h) ? 'label' : ''}">${h}</th>`
  ).join("") + "</tr></thead><tbody>";
  for (const row of rows) {
    html += "<tr>" + row.map((cell, i) =>
      `<td class="${labelCols.includes(headers[i]) ? 'label' : ''}">${
        typeof cell === "boolean" ? (cell ? "是" : "否") : fmt(cell)
      }</td>`).join("") + "</tr>";
  }
  return html + "</tbody></table>";
}
function render() {
  const root = document.getElementById("root");
  const metrics = STATE.metrics || {summary:{}, by_series:[], by_shard:[]};
  const summary = metrics.summary || {};
  let html = "<h2>运行状态</h2>";
  html += table(["字段", "值"], [
    ["run_status", STATE.run_status], ["stage", STATE.stage],
    ["method", STATE.method],
    ["config_fingerprint", STATE.config_fingerprint],
    ["manifest_fingerprint", STATE.manifest_fingerprint],
    ["aggregate_fingerprint", STATE.aggregate_fingerprint || null]
  ], ["字段"]);
  html += "<h2>聚合指标</h2>";
  html += table(["指标", "值"], [
    ["fold_count", summary.fold_count], ["step_count", summary.step_count],
    ["MAE", summary.mae], ["RMSE", summary.rmse], ["MAPE", summary.mape],
    ["MAPE 零分母跳过数", summary.mape_skipped_zero_denominator],
    ["覆盖率", summary.coverage]
  ], ["指标"]);
  html += "<h2>按序列指标</h2>";
  html += table(["series_id", "fold_count", "mae", "rmse", "mape",
                 "coverage"], (metrics.by_series || []).map(r =>
    [r.series_id, r.fold_count, r.mae, r.rmse, r.mape, r.coverage]),
    ["series_id"]);
  html += "<h2>按分片指标</h2>";
  html += table(["shard_id", "fold_count", "mae", "rmse", "mape",
                 "coverage"], (metrics.by_shard || []).map(r =>
    [r.shard_id, r.fold_count, r.mae, r.rmse, r.mape, r.coverage]),
    ["shard_id"]);
  html += "<h2>预算消耗</h2>";
  const sched = STATE.scheduler || {};
  html += table(["字段", "值"], Object.entries(sched).map(([k, v]) =>
    [k, typeof v === "object" ? JSON.stringify(v) : v]), ["字段"]);
  html += "<h2>未处理 / 被拒绝折</h2>";
  html += table(["shard_id", "series_id", "fold_index", "code", "reason"],
    (STATE.rejected_folds || []).map(r => [r.shard_id, r.series_id,
      r.fold_index, r.code, r.reason]),
    ["shard_id", "series_id", "code", "reason"]);
  html += "<h2>失效传播</h2>";
  html += table(["shard_id", "reason", "previous_fingerprint",
                 "current_fingerprint"],
    (STATE.invalidation || []).map(r => [r.shard_id || r.scope, r.reason,
      r.previous_fingerprint || r.previous,
      r.current_fingerprint || r.current]),
    ["shard_id", "reason"]);
  html += "<h2>序列业务错误</h2>";
  html += table(["series_id", "code", "message"],
    (STATE.series_errors || []).map(r => [r.series_id, r.code, r.message]),
    ["series_id", "code", "message"]);
  html += "<h2>逐折平均绝对误差曲线</h2><canvas id=\"curve\" width=\"900\" "
       + "height=\"260\"></canvas>";
  html += "<h2>原始状态 JSON</h2><pre>"
       + JSON.stringify(STATE, null, 2) + "</pre>";
  root.innerHTML = html;
  drawCurve();
}
function drawCurve() {
  const canvas = document.getElementById("curve");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const curve = STATE.fold_curve || [];
  if (!curve.length) {
    ctx.fillStyle = "#b35900";
    ctx.fillText("当前阶段尚无可绘制的逐折误差数据", 40, 40);
    return;
  }
  const width = canvas.width - 80, height = canvas.height - 60;
  const maxMae = Math.max(...curve.map(p => p.mae));
  ctx.strokeStyle = "#9aa5b1";
  ctx.beginPath(); ctx.moveTo(40, 20); ctx.lineTo(40, height + 20);
  ctx.lineTo(width + 40, height + 20); ctx.stroke();
  ctx.strokeStyle = "#0b6e4f"; ctx.beginPath();
  curve.forEach((point, i) => {
    const x = 40 + (curve.length === 1 ? width / 2
        : i * width / (curve.length - 1));
    const y = 20 + height - (maxMae === 0 ? 0
        : point.mae / maxMae * height);
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.stroke();
  ctx.fillStyle = "#1f2933";
  ctx.fillText("逐折平均绝对误差 (MAE)", 40, 14);
}
render();
</script>
</body>
</html>
"""
    html = html.replace("__PAYLOAD__", payload)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(html)
    return path


def export_all(workspace: str, state: Dict[str, Any]) -> Dict[str, str]:
    return {
        "json": export_json_report(workspace, state),
        "error_curves": export_error_curves(workspace),
        "budget": export_budget_details(workspace, state),
        "dashboard": export_html_dashboard(workspace, state),
    }
