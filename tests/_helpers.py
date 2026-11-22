"""测试公共夹具：构建临时工作区与确定性输入。"""
import os
from qrwb.config import load_config, write_default_config
from qrwb.sampling import generate_workspace_inputs
from qrwb.workspace import load_workspace
from qrwb.pipeline import Pipeline


def make_workspace(tmpdir: str, shards: int = 2, rows: int = 8,
                   config_overrides=None) -> str:
    ws = os.path.join(tmpdir, "ws")
    os.makedirs(ws, exist_ok=True)
    write_default_config(os.path.join(ws, "config.yaml"))
    cfg = load_config(os.path.join(ws, "config.yaml"))
    if config_overrides:
        import yaml
        data = cfg.raw
        for dotted, value in config_overrides.items():
            cursor = data
            parts = dotted.split(".")
            for part in parts[:-1]:
                cursor = cursor[part]
            cursor[parts[-1]] = value
        with open(os.path.join(ws, "config.yaml"), "w",
                  encoding="utf-8", newline="\n") as handle:
            yaml.safe_dump(data, handle, sort_keys=True,
                           allow_unicode=True)
        cfg = load_config(os.path.join(ws, "config.yaml"))
    generate_workspace_inputs(cfg, os.path.join(ws, "inputs"),
                              shard_count=shards, rows_per_shard=rows)
    return ws


def make_pipeline(ws: str) -> Pipeline:
    cfg = load_config(os.path.join(ws, "config.yaml"))
    resources, records, manifest = load_workspace(ws, cfg)
    return Pipeline(ws, cfg, resources, records, manifest)
