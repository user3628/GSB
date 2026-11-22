import json
import os
import shutil
import tempfile
import unittest
from multiprocessing import Process

from contract_chain.checkpoints import CheckpointStore
from contract_chain.codec import canonical_bytes, digest, read_json
from contract_chain.config import PipelineConfig
from contract_chain.detect import iou_pixels, run_detection
from contract_chain.errors import (
    EvidenceBrokenError,
    IncrementalMismatchError,
    InvalidStageError,
    RefParseError,
    StaleCheckpointError,
    UnitUnresolvedError,
)
from contract_chain.evidence import (
    build_evidence,
    export_manifest,
    import_manifest,
    reconstruct_decisions,
)
from contract_chain.httpapi import serve
from contract_chain.normalize import (
    normalize_duration,
    normalize_month,
    normalize_penalty,
    run_normalize,
)
from contract_chain.parse import extract_locations
from contract_chain.parse import normalize_text, parse_chinese_numeral, run_parse
from contract_chain.pipeline import Pipeline
from contract_chain.resolve import run_resolve
from contract_chain.samplegen import build_sample
from urllib import request as urlrequest


def _lock_worker(target):
    try:
        from contract_chain.checkpoints import FileLock
        FileLock(target, timeout=0.2).__enter__()
    except OSError:
        os._exit(10)


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="cc-test-")
        self.workspace = os.path.join(self.tempdir, "ws")
        os.makedirs(self.workspace, exist_ok=True)
        self.sample = build_sample()
        self.config = PipelineConfig()
        self.pipeline = Pipeline(self.workspace, self.config)
        self.pipeline.load_sample(self.sample)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def run_full(self):
        return self.pipeline.run_full(self.sample)


class DetectionTests(BaseCase):
    def test_iou_pixel_area_and_matching(self):
        iou = iou_pixels([0.1, 0.1, 0.2, 0.2], [0.1, 0.1, 0.2, 0.2], 100, 100)
        self.assertEqual(float(iou), 1.0)
        product = run_detection(self.sample, self.config.detect, self.sample["seed"])
        self.assertEqual(product["stats"]["candidate_count"], 28)
        for page in product["pages"]:
            region_ids = [match["region_id"] for match in page["matches"]]
            self.assertEqual(len(region_ids), len(set(region_ids)))
            candidate_ids = [match["candidate_id"] for match in page["matches"]]
            self.assertEqual(len(candidate_ids), len(set(candidate_ids)))
            for candidate_id in page["background_candidate_ids"]:
                self.assertNotIn(candidate_id, candidate_ids)
        self.assertGreater(product["stats"]["background_count"], 0)
        self.assertTrue(all(d["marked_as"] == "background" for d in product["diagnostics"]))

    def test_byte_deterministic_detection(self):
        first = canonical_bytes(run_detection(self.sample, self.config.detect, 20241123))
        second = canonical_bytes(run_detection(build_sample(), self.config.detect, 20241123))
        self.assertEqual(first, second)

    def test_threshold_change_changes_fingerprint(self):
        result = self.run_full()
        baseline = result["fingerprints"]["detect"]
        changed_config = PipelineConfig.from_dict(self.config.to_dict())
        object.__setattr__(changed_config.detect, "iou_threshold", "0.9")
        changed_pipeline = Pipeline(os.path.join(self.tempdir, "ws2"), changed_config)
        changed_pipeline.load_sample(self.sample)
        changed = changed_pipeline.run_full(self.sample)
        self.assertNotEqual(baseline, changed["fingerprints"]["detect"])


class ParseTests(BaseCase):
    def test_text_normalization_and_offsets(self):
        text, mapping = normalize_text(" 第 3 . 2 条\tSection 3.2 ")
        self.assertNotIn("\t", text)
        self.assertEqual(len(text), len(mapping))
        self.assertEqual(parse_chinese_numeral("十二"), 12)

    def test_multi_language_locations_and_parse_error(self):
        detection = run_detection(self.sample, self.config.detect, self.sample["seed"])
        from contract_chain.detect import region_binding_index

        bindings = region_binding_index(self.sample, detection)
        product = run_parse(self.sample, bindings, self.config.parse)
        ref4 = next(entry for entry in product["entries"] if entry["ref_id"] == "ref-0004")
        self.assertEqual(ref4["parse_error"]["code"], RefParseError.code)
        self.assertEqual(ref4["parse_error"]["details"]["ref_id"], "ref-0004")
        ref3 = next(entry for entry in product["entries"] if entry["ref_id"] == "ref-0003")
        self.assertEqual(len(ref3["location_candidates"]), 2)
        self.assertTrue(ref3["location_candidates"][0]["preferred"])
        self.assertEqual(ref3["preferred_location"], {"article": 4, "clause": 0})
        ref2 = next(entry for entry in product["entries"] if entry["ref_id"] == "ref-0002")
        self.assertIn({"article": 3, "clause": 2},
                      [{"article": c["article"], "clause": c["clause"]} for c in ref2["location_candidates"]])
        for entry in product["entries"]:
            self.assertIn("gt_xywh", entry["region_binding"])


class NormalizeTests(BaseCase):
    def test_fixed_point_conversion(self):
        cfg = self.config.normalize
        duration, err = normalize_duration("12个月", "x", cfg)
        self.assertIsNone(err)
        self.assertEqual(duration["point"]["scaled"], 1200)
        self.assertEqual(duration["point"]["factor"], "1")
        penalty, err = normalize_penalty("5%", "x", cfg)
        self.assertEqual(penalty["point"]["scaled"], 50000)
        month, err = normalize_month("2024年3月", "x", cfg)
        self.assertEqual(month["point"]["scaled"], 24291)

    def test_unit_and_interval_errors(self):
        cfg = self.config.normalize
        _, err = normalize_duration("若干个月", "ref-x", cfg)
        self.assertEqual(err["code"], UnitUnresolvedError.code)
        _, err = normalize_penalty("5", "ref-x", cfg)
        self.assertEqual(err["details"]["term"], "penalty")
        _, err = normalize_duration("（24, 12）个月", "ref-x", cfg)
        self.assertEqual(err["details"]["reason"], "interval endpoints conflict")

    def test_normalized_products_keep_region_coordinates(self):
        self.run_full()
        product = read_json(os.path.join(self.workspace, "stages", "normalize.json"))
        for result in product["results"]:
            self.assertIn("gt_xywh", result["region_binding"])


class ResolveTests(BaseCase):
    def test_unique_winner_and_demotions(self):
        result = self.run_full()
        resolve = read_json(os.path.join(self.workspace, "stages", "resolve.json"))
        for group in resolve["groups"]:
            self.assertEqual(group["ref_ids"].count(group["winner_ref_id"]), 1)
            losers = [ref for ref in group["ref_ids"] if ref != group["winner_ref_id"]]
            if group["conflict"]:
                self.assertTrue(losers)
        self.assertEqual(resolve["stats"]["conflict_group_count"], 2)
        self.assertTrue(all(d["reason"] == "conflict_loser" for d in resolve["demotions"]))

    def test_order_independence(self):
        from contract_chain.detect import region_binding_index
        detection = run_detection(self.sample, self.config.detect, self.sample["seed"])
        bindings = region_binding_index(self.sample, detection)
        parse_a = run_parse(self.sample, bindings, self.config.parse)
        norm_a = run_normalize(parse_a, self.config.normalize)
        resolve_a = canonical_bytes(run_resolve(parse_a, norm_a, self.config.resolve))

        shuffled_sample = json.loads(json.dumps(self.sample))
        for page in shuffled_sample["pages"]:
            for region in page["regions"]:
                region["refs"].reverse()
        parse_b = run_parse(shuffled_sample, bindings, self.config.parse)
        # restore deterministic ref ordering before feeding the resolver, as stage products are sorted
        parse_b["entries"] = list(reversed(parse_b["entries"]))
        norm_b = run_normalize(parse_b, self.config.normalize)
        resolve_b = canonical_bytes(run_resolve(parse_b, norm_b, self.config.resolve))
        self.assertEqual(resolve_a, resolve_b)


class EvidenceTests(BaseCase):
    def test_export_import_rebuild_and_broken_link(self):
        self.run_full()
        manifest_path = os.path.join(self.workspace, "exported.json")
        manifest = import_manifest(os.path.join(self.workspace, "evidence_manifest.json"))
        export_manifest(manifest, manifest_path)
        reloaded = import_manifest(manifest_path)
        rebuilt = reconstruct_decisions(reloaded)
        resolve = read_json(os.path.join(self.workspace, "stages", "resolve.json"))
        winners = {g["winner_ref_id"] for g in resolve["groups"]}
        demoted = {d["ref_id"] for d in resolve["demotions"]}
        for ref_id, verdict in rebuilt.items():
            if ref_id in winners:
                self.assertEqual(verdict["verdict"], "adopted")
            elif ref_id in demoted:
                self.assertEqual(verdict["verdict"], "demoted")
            self.assertEqual(len(verdict["region_gt_xywh"]), 4)
        broken = json.loads(json.dumps(reloaded))
        broken["records"][0]["links"]["location_parse"] = None
        broken_path = os.path.join(self.workspace, "broken.json")
        with open(broken_path, "w", encoding="utf-8") as handle:
            json.dump(broken, handle)
        with self.assertRaises(EvidenceBrokenError) as context:
            import_manifest(broken_path)
        self.assertIn("location_parse", context.exception.details["missing_link_names"])


class CheckpointTests(BaseCase):
    def test_atomic_continuous_checkpoints_and_stop_resume(self):
        self.pipeline.store.request_stop()
        stopped = self.pipeline.run(self.sample)
        self.assertIsNotNone(stopped["stopped_at"])
        partial_records = self.pipeline.store.records()
        seqs = [record["seq"] for record in partial_records]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))
        resumed = self.pipeline.resume()
        self.assertIsNone(resumed["stopped_at"])
        full_workspace = os.path.join(self.tempdir, "full")
        full_pipeline = Pipeline(full_workspace, PipelineConfig())
        full_pipeline.run_full(self.sample)
        for stage in ("detect", "parse", "normalize", "resolve"):
            left = canonical_bytes(self.pipeline.store.get_stage(stage))
            right = canonical_bytes(full_pipeline.store.get_stage(stage))
            self.assertEqual(left, right)
        final_seqs = [r["seq"] for r in self.pipeline.store.records()]
        self.assertEqual(final_seqs, [1, 2, 3, 4])

    def test_stale_and_invalid_stage_publishing(self):
        with self.assertRaises(StaleCheckpointError):
            self.pipeline.store.publish(
                "detect", {"x": 1}, "in", "cfg", "fp", proposed_seq=99
            )
        store = CheckpointStore(os.path.join(self.tempdir, "bare"))
        with self.assertRaises(InvalidStageError):
            store.publish("parse", {"x": 1}, "in", "cfg", "fp")

    def test_real_file_lock_is_exclusive(self):
        from contract_chain.checkpoints import FileLock

        target = os.path.join(self.tempdir, "lockdir")
        os.makedirs(target, exist_ok=True)
        holder = FileLock(target, timeout=1)
        holder.__enter__()

        process = Process(target=_lock_worker, args=(target,))
        process.start()
        process.join()
        holder.__exit__(None, None, None)
        self.assertEqual(process.exitcode, 10)


class IncrementalTests(BaseCase):
    def test_incremental_invalidation_and_byte_parity(self):
        self.run_full()
        changed = json.loads(json.dumps(self.sample))
        changed["pages"][0]["regions"][0]["xywh"][0] += 0.05
        result = self.pipeline.run_incremental(changed)
        self.assertEqual(result["earliest_changed_stage"], "detect")
        self.assertTrue(result["parity"]["byte_identical"])

    def test_config_change_propagates_downstream(self):
        self.run_full()
        changed = PipelineConfig.from_dict(self.config.to_dict())
        object.__setattr__(changed.resolve, "language_weight", {"zh": "1.0", "en": "0.8"})
        result = self.pipeline.run_incremental(new_config=changed)
        self.assertEqual(result["earliest_changed_stage"], "resolve")
        self.assertTrue(result["parity"]["byte_identical"])

    def test_mismatch_reports_first_ref_and_stage(self):
        self.run_full()
        healthy_dir = os.path.join(self.tempdir, "healthy-full")
        healthy = Pipeline(healthy_dir, PipelineConfig())
        healthy.run_full(self.sample)
        corrupted_path = self.pipeline.store.stage_product_path("parse")
        corrupted = read_json(corrupted_path)
        corrupted["entries"][0]["normalized_text"] = "CORRUPTED"
        with open(corrupted_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(corrupted, ensure_ascii=False).encode("utf-8").decode("utf-8"))
        with self.assertRaises(IncrementalMismatchError) as context:
            self.pipeline.verify_parity(self.sample)
        self.assertEqual(context.exception.details["first_difference"]["stage"], "parse")
        self.assertIn("ref", context.exception.details["first_difference"]["first_ref_id"])


class HttpTests(BaseCase):
    def test_http_endpoints(self):
        server, _ = serve(self.workspace, "127.0.0.1", 0)
        host, port = server.server_address
        base = "http://%s:%s" % (host, port)
        try:
            with urlrequest.urlopen(base + "/status") as response:
                self.assertEqual(response.status, 200)
            req = urlrequest.Request(
                base + "/run/full",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlrequest.urlopen(req) as response:
                payload = json.loads(response.read().decode("utf-8"))
                self.assertIn("resolve", payload["completed_stages"])
            with urlrequest.urlopen(base + "/reconstruct") as response:
                rebuilt = json.loads(response.read().decode("utf-8"))
                self.assertTrue(rebuilt)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()


class EdgeCoverageTests(BaseCase):
    def test_nfkc_fullwidth_and_raw_offsets(self):
        text, mapping = normalize_text("第３．２条  Section 3.2")
        self.assertEqual(text, "第3.2条 Section 3.2")
        self.assertEqual(len(text), len(mapping))
        locations = extract_locations(text, mapping)
        self.assertEqual(locations[0]["span_raw"], [0, 5])
        self.assertEqual(locations[1]["span_raw"], [7, 18])

    def test_chinese_percent_and_half_open_interval(self):
        cfg = self.config.normalize
        penalty, error = normalize_penalty("百分之五", "r", cfg)
        self.assertIsNone(error)
        self.assertEqual(penalty["point"]["scaled"], 50000)
        duration, error = normalize_duration("[3, 6)个月", "r", cfg)
        self.assertIsNone(error)
        self.assertFalse(duration["lower_open"])
        self.assertTrue(duration["upper_open"])

    def test_detection_candidates_sorted_and_diagnostic_complete(self):
        product = run_detection(self.sample, self.config.detect, self.sample["seed"])
        for page in product["pages"]:
            ids = [c["candidate_id"] for c in page["candidates"]]
            self.assertEqual(ids, sorted(ids))
            matched = {m["candidate_id"] for m in page["matches"]}
            self.assertEqual(
                set(page["background_candidate_ids"]),
                {c["candidate_id"] for c in page["candidates"]} - matched,
            )

    def test_checkpoint_record_contract(self):
        self.run_full()
        for record in self.pipeline.store.records():
            for field_name in ("seq", "stage", "input_digest", "config_digest",
                               "product_digest", "fingerprint"):
                self.assertIn(field_name, record)
            self.assertEqual(len(record["product_digest"]), 64)

    def test_unit_dictionary_change_invalidates_normalize_downstream(self):
        self.run_full()
        changed = PipelineConfig.from_dict(self.config.to_dict())
        factors = dict(changed.normalize.duration_factors)
        factors["月"] = "2"
        object.__setattr__(changed.normalize, "duration_factors", factors)
        result = self.pipeline.run_incremental(new_config=changed)
        self.assertEqual(result["earliest_changed_stage"], "normalize")
        self.assertTrue(result["parity"]["byte_identical"])
