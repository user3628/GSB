"""本地 HTTP 接口演示：启动服务 -> 生成数据 -> 分阶段执行 -> 查询 -> 增量校验。"""

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def request(method, url, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    workspace = os.path.join(ROOT, "demo-workspace", "http-demo")
    if os.path.exists(workspace):
        shutil.rmtree(workspace)
    server = subprocess.Popen(
        [sys.executable, "-m", "pipeline.cli", "serve", "--workspace", workspace, "--port", str(port)],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base = f"http://127.0.0.1:{port}"

    def call(path, payload=None, method="POST"):
        return request(method, base + path, payload)

    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(base + "/status", timeout=1)
                break
            except OSError:
                time.sleep(0.1)
        print("POST /generate ->", json.dumps(call("/generate", {}), ensure_ascii=False))
        print("POST /run ->", json.dumps(call("/run", {"through_stage": "stage4_summary"}), ensure_ascii=False))
        status = request("GET", base + "/status")
        print("GET  /status sequences ->", [c["sequence"] for c in status["checkpoints"]])
        print(
            "POST /edit-sample ->",
            json.dumps(
                call(
                    "/edit-sample",
                    {"sample_id": "train-0001", "text": "包装很好价格优惠非常喜欢", "label": 1},
                ),
                ensure_ascii=False,
            ),
        )
        print("POST /run stage1-2 ->", json.dumps(call("/run", {"through_stage": "stage2_model"}), ensure_ascii=False))
        refresh = __import__("pipeline.pipeline", fromlist=["Pipeline"]).Pipeline(workspace)
        from pipeline.data_ops import refresh_inference_model_versions

        print("refresh model refs ->", json.dumps(refresh_inference_model_versions(refresh.ws), ensure_ascii=False))
        print("POST /run (incremental) ->", json.dumps(call("/run", {}), ensure_ascii=False))
        print("POST /validate ->", json.dumps(call("/validate", {"scope": "stage4_summary"}), ensure_ascii=False))
        status = request("GET", base + "/status")
        print("invalidation events ->", json.dumps(status["invalidation_events"], ensure_ascii=False))
    finally:
        server.terminate()
        server.wait(timeout=10)


if __name__ == "__main__":
    main()
