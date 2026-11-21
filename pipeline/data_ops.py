"""单样本编辑与单分片替换入口（确定性原地修改）。"""

import os

from pipeline.shards import read_json_array, read_jsonl, write_json_array, write_jsonl


def edit_training_sample(workspace, sample_id, new_text=None, new_label=None):
    """在训练分片中定位单条样本并修改，未找到时报错。返回被改样本。"""
    from pipeline.workspace import Workspace

    ws = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
    for name in sorted(os.listdir(ws.train_dir)):
        if not name.endswith(".jsonl"):
            continue
        path = os.path.join(ws.train_dir, name)
        records = read_jsonl(path)
        changed = False
        for record in records:
            if record["sample_id"] == sample_id:
                if new_text is not None:
                    record["text"] = new_text
                if new_label is not None:
                    record["label"] = new_label
                changed = True
        if changed:
            write_jsonl(path, records)
            return {"sample_id": sample_id, "shard": name}
    raise KeyError(f"未找到样本 {sample_id}")


def replace_training_shard(workspace, shard_name, records):
    """整分片替换（文件名不变，内容整体覆盖）。"""
    from pipeline.workspace import Workspace

    ws = workspace if isinstance(workspace, Workspace) else workspace
    path = os.path.join(ws.train_dir, shard_name)
    write_jsonl(path, records)
    return path


def replace_inference_shard(workspace, shard_name, requests):
    from pipeline.workspace import Workspace

    ws = workspace if isinstance(workspace, Workspace) else workspace
    path = os.path.join(ws.infer_dir, shard_name)
    write_json_array(path, requests)
    return path


def tamper_model_version_in_request(workspace, request_id, wrong_version):
    """把单条推理请求引用的模型版本改成错误版本，用于错误码演示。"""
    from pipeline.workspace import Workspace

    ws = workspace if isinstance(workspace, Workspace) else workspace
    for name in sorted(os.listdir(ws.infer_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(ws.infer_dir, name)
        requests = read_json_array(path)
        changed = False
        for request in requests:
            if request["request_id"] == request_id:
                request["ref_model_version"] = wrong_version
                changed = True
        if changed:
            write_json_array(path, requests)
            return {"request_id": request_id, "shard": name, "wrong_version": wrong_version}
    raise KeyError(f"未找到请求 {request_id}")


def refresh_inference_model_versions(workspace):
    """训练侧变化导致模型版本变化后，把所有推理请求的引用版本刷新为当前有效版本。

    刷新本身只改推理分片内容（确定性重写），因此 stage3/stage4 会随之失效重建；
    stage1/stage2 不受影响、继续复用。
    """
    from pipeline.config import load_config
    from pipeline.stages import compute_model_version, stage2_input_fingerprint
    from pipeline.workspace import Workspace

    ws = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
    config = load_config(ws.config_path)
    from pipeline.atomicio import read_json

    stage1 = read_json(ws.artifact_path("stage1_features"))[0]
    current_version = compute_model_version(stage2_input_fingerprint(stage1, config))
    changed_files = []
    for name in sorted(os.listdir(ws.infer_dir)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(ws.infer_dir, name)
        requests = read_json_array(path)
        for request in requests:
            request["ref_model_version"] = current_version
        write_json_array(path, requests)
        changed_files.append(name)
    return {"model_version": current_version, "files": changed_files}
