"""工作区目录布局与路径管理。"""

import os

from pipeline.atomicio import FileLock


class Workspace:
    def __init__(self, root):
        self.root = os.path.abspath(root)
        self.data_dir = os.path.join(self.root, "data")
        self.train_dir = os.path.join(self.data_dir, "train")
        self.infer_dir = os.path.join(self.data_dir, "infer")
        self.config_path = os.path.join(self.root, "config", "pipeline.yaml")
        self.artifact_dir = os.path.join(self.root, "artifacts")
        self.checkpoint_dir = os.path.join(self.root, "checkpoints")
        self.quarantine_dir = os.path.join(self.root, "quarantine")
        self.state_dir = os.path.join(self.root, "state")
        self.tmp_dir = os.path.join(self.root, "tmp")
        self.lock_path = os.path.join(self.state_dir, "workspace.lock")
        self.invalidation_log = os.path.join(self.state_dir, "invalidation.log")

    def ensure_dirs(self):
        for path in (
            self.train_dir,
            self.infer_dir,
            os.path.dirname(self.config_path),
            self.artifact_dir,
            self.checkpoint_dir,
            self.quarantine_dir,
            self.state_dir,
            self.tmp_dir,
        ):
            os.makedirs(path, exist_ok=True)
        return self

    def artifact_path(self, stage):
        return os.path.join(self.artifact_dir, stage + ".json")

    def lock(self):
        return FileLock(self.lock_path)

    def stage_artifact_names(self):
        from pipeline import STAGES

        return {stage: stage + ".json" for stage in STAGES}
