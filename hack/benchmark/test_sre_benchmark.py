from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from run_sre_benchmark import prepare_output
from run_sre_keda import (
    MODEL,
    capacity_evidence_complete,
    reliability_evidence_complete,
    run_resource,
)

ROOT = Path(__file__).resolve().parents[2]


class SREBenchmarkArtifactTest(unittest.TestCase):
    def test_output_directory_is_never_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "artifacts"
            output.mkdir()
            marker = output / "prior-evidence.txt"
            marker.write_text("keep", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not empty"):
                prepare_output(output)
            self.assertEqual(marker.read_text(encoding="utf-8"), "keep")

    def test_fake_benchmark_has_complete_shape_and_valid_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "artifacts"
            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "hack/benchmark/run_sre_benchmark.py"),
                    "--output",
                    str(output),
                    "--image-digest",
                    "sha256:" + "a" * 64,
                ],
                cwd=ROOT,
                check=True,
            )
            manifest = json.loads(
                (output / "manifest.json").read_text(encoding="utf-8")
            )
            runs = json.loads((output / "runs.json").read_text(encoding="utf-8"))
            self.assertTrue(manifest["complete"])
            self.assertFalse(manifest["canonical"])
            self.assertEqual(len(runs), 13)
            self.assertEqual(
                sum(run["total"] for run in runs if run["kind"] == "capacity"), 384
            )
            with gzip.open(output / "cases.jsonl.gz", "rt", encoding="utf-8") as handle:
                cases = [json.loads(line) for line in handle]
            self.assertEqual(len(cases), 416)
            self.assertEqual(
                {
                    item["error_code"]
                    for item in cases
                    if item["run_id"] == "fake-reliability-w16" and item["error_code"]
                },
                {"timeout", "injected_tool_error"},
            )
            self.assertTrue(reliability_evidence_complete(runs, cases))
            self.assertFalse(capacity_evidence_complete(runs))
            for run in runs:
                if run["kind"] != "capacity":
                    continue
                run["cost_usd"] = "0.01"
                run["scale_from_zero_ms"] = 100
                run["scale_to_zero_ms"] = 200
                run["peak_workers"] = run["max_workers"]
            self.assertTrue(capacity_evidence_complete(runs))
            for line in (
                (output / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
            ):
                expected, name = line.split("  ", 1)
                self.assertEqual(
                    hashlib.sha256((output / name).read_bytes()).hexdigest(), expected
                )
            rendered = "".join(
                path.read_text(encoding="utf-8", errors="ignore")
                for path in output.iterdir()
            )
            self.assertNotIn("OPENAI_API_KEY", rendered)

    def test_real_run_manifest_is_digest_pinned_and_contains_no_secret_value(
        self,
    ) -> None:
        marker = "never-serialize-this-api-key"
        resource = run_resource(
            "benchmark-run",
            "agentstorm-system",
            "dataset",
            "scenario",
            "openai-secret",
            "ghcr.io/alphasxd/agentstorm-worker@sha256:" + "a" * 64,
            16,
            "1.25",
            "5",
            True,
        )
        rendered = json.dumps(resource, sort_keys=True)
        self.assertEqual(resource["spec"]["target"]["model"], MODEL)
        self.assertEqual(resource["spec"]["workload"]["concurrencyPerWorker"], 1)
        self.assertEqual(resource["spec"]["scheduling"]["maxWorkers"], 16)
        self.assertIn('"name": "openai-secret"', rendered)
        self.assertNotIn(marker, rendered)


if __name__ == "__main__":
    unittest.main()
