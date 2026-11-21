"""强制终止恢复演示。

子进程执行管线时由父进程在其运行中途 SIGKILL（Windows 下为 terminate），
随后新进程从检查点恢复，并通过与从零全量构建的逐字节比对证明一致性。
"""

import multiprocessing
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.generator import generate_workspace
from pipeline.pipeline import Pipeline


def _worker(workspace):
    Pipeline(workspace).run()


def main():
    root = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "demo-workspace",
        "kill-recovery",
    )
    if os.path.exists(root):
        shutil.rmtree(root)
    generate_workspace(root)

    process = multiprocessing.Process(target=_worker, args=(root,))
    process.start()
    checkpoint_dir = os.path.join(root, "checkpoints")
    deadline = time.time() + 10
    while time.time() < deadline:
        records = [
            name
            for name in os.listdir(checkpoint_dir)
            if name.startswith("record-") and name.endswith(".json")
        ] if os.path.isdir(checkpoint_dir) else []
        if process.exitcode is not None:
            break
        if 0 < len(records) < 4:
            break
        time.sleep(0.001)
    process.kill()  # 强制终止，不写退出报告
    process.join()
    print(f"子进程已被强制终止, exitcode={process.exitcode}")

    pipe = Pipeline(root)
    mid_status = pipe.status()
    print("恢复前检查点序号:", [c["sequence"] for c in mid_status["checkpoints"]])

    report = pipe.run()
    print("恢复执行报告:", report)
    final_status = pipe.status()
    sequences = [c["sequence"] for c in final_status["checkpoints"]]
    print("恢复后检查点序号:", sequences)
    assert sequences == [1, 2, 3, 4], sequences

    validation = pipe.validate_incremental()
    print("恢复后与全量构建逐字节比对:", validation)
    assert validation["byte_identical"] is True
    print("强制终止恢复边界验证通过")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
