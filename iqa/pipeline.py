"""Four-stage orchestration with incremental rebuild, stop/resume and reports."""

import os

from .config import Config
from .injection import Injection
from .checkpoints import CheckpointStore, STAGE_ORDER, atomic_write_json
from .errors import BusinessError
from .hashing import canonical, digest
from . import normalize as stage1
from . import metrics as stage2
from . import semantic as stage3
from . import fusion as stage4


class Pipeline:
    def __init__(self, work_dir, config=None, injection=None):
        self.work_dir = work_dir
        self.config = (config or Config()).validate()
        self.injection = injection or Injection()
        self.store = CheckpointStore(work_dir)
        self.images_dir = os.path.join(work_dir, "images")
        self.reference_dir = os.path.join(work_dir, "references")
        self.metadata_path = os.path.join(work_dir, "metadata.json")
        self.outputs_dir = os.path.join(work_dir, "outputs")
        os.makedirs(self.outputs_dir, exist_ok=True)

    def load_metadata(self):
        import json

        with open(self.metadata_path, "r", encoding="utf-8") as handle:
            return json.loads(handle.read())

    def _reference_path(self, metadata):
        name = metadata.get("reference_file")
        if not name:
            return ""
        return os.path.join(self.reference_dir, name)

    def _reuse_or_run(self, image_id, stage_name, existing, builder, previous_record=None):
        if existing is not None and existing.get("fingerprint") == builder["fingerprint"]:
            return existing, True
        record = builder
        if existing is not None:
            sequence = self.store.publish(
                image_id,
                stage_name,
                record,
                expected_previous=previous_record,
                force_sequence=existing["sequence"],
                allow_replace=True,
            )
        else:
            sequence = self.store.publish(
                image_id, stage_name, record, expected_previous=previous_record
            )
        record["sequence"] = sequence
        return record, False

    def run_image(self, metadata):
        image_id = metadata["image_id"]
        image_path = os.path.join(self.images_dir, metadata["file"])
        diagnostics = []
        invalidations = []
        stage_status = {}
        final_record = None
        comparison = None

        # Stage 1
        try:
            existing1 = self.store.load(image_id, stage1.STAGE)
            built1, pixels = stage1.run(image_path, metadata, self.injection)
            record1, reused1 = self._reuse_or_run(image_id, stage1.STAGE, existing1, built1)
            stage_status[stage1.STAGE] = "reused" if reused1 else "rebuilt"
            if not reused1:
                invalidations.append(stage1.STAGE)
        except BusinessError as error:
            diagnostics.append(error.to_dict())
            stage_status[stage1.STAGE] = f"failed:{error.code}"
            for downstream in STAGE_ORDER[1:]:
                stage_status[downstream] = "blocked"
            return self._image_result(metadata, stage_status, diagnostics, invalidations, None, None)

        if self.store.stop_requested():
            return self._image_result(metadata, stage_status, diagnostics, invalidations, None, None, stopped=True)

        # Stage 2
        try:
            existing2 = self.store.load(image_id, stage2.STAGE)
            built2 = stage2.run(pixels, metadata, self._reference_path(metadata), self.injection)
            record2, reused2 = self._reuse_or_run(
                image_id, stage2.STAGE, existing2, built2, previous_record=record1
            )
            stage_status[stage2.STAGE] = "reused" if reused2 else "rebuilt"
            if not reused2:
                invalidations.append(stage2.STAGE)
        except BusinessError as error:
            diagnostics.append(error.to_dict())
            stage_status[stage2.STAGE] = f"failed:{error.code}"
            for downstream in STAGE_ORDER[2:]:
                stage_status[downstream] = "blocked"
            return self._image_result(metadata, stage_status, diagnostics, invalidations, None, record1)

        if self.store.stop_requested():
            return self._image_result(metadata, stage_status, diagnostics, invalidations, None, record1, stopped=True)

        # Stage 3
        existing3 = self.store.load(image_id, stage3.STAGE)
        built3 = stage3.run(pixels, metadata)
        record3, reused3 = self._reuse_or_run(
            image_id, stage3.STAGE, existing3, built3, previous_record=record2
        )
        stage_status[stage3.STAGE] = "reused" if reused3 else "rebuilt"
        if not reused3:
            invalidations.append(stage3.STAGE)

        if self.store.stop_requested():
            return self._image_result(metadata, stage_status, diagnostics, invalidations, None, record1, stopped=True)

        # Stage 4
        try:
            existing4 = self.store.load(image_id, stage4.STAGE)
            config = self.config
            if self.injection.kind == "negative_weight" and self.injection.targets(image_id):
                from dataclasses import replace

                weights = dict(config.weights)
                weights["sharpness"] = float(self.injection.value)
                config = replace(config, weights=weights)
            built4 = stage4.fuse(record2, record3, config)
            record4, reused4 = self._reuse_or_run(
                image_id, stage4.STAGE, existing4, built4, previous_record=record3
            )
            stage_status[stage4.STAGE] = "reused" if reused4 else "rebuilt"
            if not reused4:
                invalidations.append(stage4.STAGE)
            final_record = record4
            comparison = stage4.compare_expected(record4, metadata)
        except BusinessError as error:
            diagnostics.append(error.to_dict())
            stage_status[stage4.STAGE] = f"failed:{error.code}"

        return self._image_result(metadata, stage_status, diagnostics, invalidations, final_record, record1, comparison)

    def _image_result(self, metadata, stage_status, diagnostics, invalidations, final_record, stage1_record, comparison=None, stopped=False):
        return {
            "image_id": metadata["image_id"],
            "operator": metadata.get("operator"),
            "stage_status": stage_status,
            "invalidated_stages": invalidations,
            "diagnostics": diagnostics,
            "stopped": stopped,
            "stage1_summary": None if stage1_record is None else stage1_record["product"]["summary"],
            "final": None if final_record is None else final_record["product"],
            "expected_check": comparison,
        }

    @staticmethod
    def _stable_summary(summary):
        """Strip run-specific actions so the fixed summary is byte-stable."""
        stable = {key: value for key, value in summary.items() if key != "results"}
        stable["results"] = []
        for result in summary["results"]:
            stable["results"].append(
                {
                    key: value
                    for key, value in result.items()
                    if key not in ("stage_status", "invalidated_stages", "stopped")
                }
            )
        return stable

    def run(self):
        metadata_list = self.load_metadata()
        results = []
        stopped = False
        for metadata in metadata_list:
            result = self.run_image(metadata)
            results.append(result)
            if result.get("stopped"):
                stopped = True
                break
        summary = self._build_summary(results, stopped)
        self._write_outputs(summary)
        return summary

    def _build_summary(self, results, stopped):
        out_of_range = []
        diagnostics = []
        grades = {"excellent": 0, "good": 0, "poor": 0}
        fingerprints = {}
        for result in results:
            if result["expected_check"] is not None and not result["expected_check"]["within_range"]:
                out_of_range.append(result["expected_check"])
            diagnostics.extend(result["diagnostics"])
            if result["final"] is not None:
                grades[result["final"]["grade"]] += 1
            for stage in STAGE_ORDER:
                record = self.store.load(result["image_id"], stage)
                if record is not None:
                    fingerprints[f"{result['image_id']}:{stage}"] = record["fingerprint"]
        sequences = [
            record["sequence"] for record in self.store.all_records()
        ]
        return {
            "schema_version": 1,
            "config_version": self.config.version(),
            "injection": self.injection.to_dict(),
            "stopped": stopped,
            "image_count": len(results),
            "grade_counts": grades,
            "out_of_range": out_of_range,
            "diagnostics": diagnostics,
            "fingerprints": fingerprints,
            "max_sequence": max(sequences) if sequences else 0,
            "results": results,
        }

    def _write_outputs(self, summary):
        atomic_write_json(
            os.path.join(self.outputs_dir, "summary.json"), self._stable_summary(summary)
        )
        atomic_write_json(
            os.path.join(self.outputs_dir, "run_report.json"), summary
        )
        diagnostic_report = {
            "diagnostics": summary["diagnostics"],
            "out_of_range": summary["out_of_range"],
            "invalidations": [
                {"image_id": result["image_id"], "stages": result["invalidated_stages"]}
                for result in summary["results"]
            ],
        }
        atomic_write_json(os.path.join(self.outputs_dir, "diagnostics.json"), diagnostic_report)

    def status(self):
        metadata_list = self.load_metadata()
        payload = []
        for metadata in metadata_list:
            image_id = metadata["image_id"]
            stages = {}
            for stage in STAGE_ORDER:
                record = self.store.load(image_id, stage)
                stages[stage] = None if record is None else {
                    "sequence": record["sequence"],
                    "fingerprint": record["fingerprint"],
                    "input_fingerprint": record["input_fingerprint"],
                    "product_digest": record["product_digest"],
                }
            payload.append({"image_id": image_id, "stages": stages})
        return {
            "config_version": self.config.version(),
            "stop_requested": self.store.stop_requested(),
            "images": payload,
        }
