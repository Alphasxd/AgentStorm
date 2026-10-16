from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openai_compatible_mock import diagnosis, load_fixtures, scripted_completion
from run_sre_compatible import (
    BenchmarkError,
    claim_policy,
    execute_mock,
    render_report,
    require_spend_opt_in,
    validate_endpoint,
)
from run_sre_compatible_keda import compatible_run_resource

ROOT = Path(__file__).resolve().parents[2]


class EndpointMetadataTest(unittest.TestCase):
    def test_domestic_endpoint_requires_https_price_and_pinned_model(self) -> None:
        endpoint = validate_endpoint(
            model="qwen2.5-72b-instruct",
            base_url="https://example.invalid/compatible-mode/v1",
            endpoint_class="domestic-cloud",
            input_price="1.25",
            output_price="5",
        )
        self.assertEqual(endpoint["host"], "example.invalid")
        self.assertEqual(endpoint["model_version"], "qwen2.5-72b-instruct")
        self.assertNotIn("api_key", endpoint)

        for kwargs, message in (
            ({"model": "latest"}, "pinned"),
            ({"base_url": "http://example.invalid/v1"}, "https"),
            ({"base_url": "https://user:secret@example.invalid/v1"}, "credentials"),
            ({"input_price": "automatic"}, "decimal"),
            ({"endpoint_class": "ssh"}, "endpoint class"),
        ):
            values = {
                "model": "qwen2.5-72b-instruct",
                "base_url": "https://example.invalid/v1",
                "endpoint_class": "domestic-cloud",
                "input_price": "1",
                "output_price": "2",
            }
            values.update(kwargs)
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(BenchmarkError, message):
                    validate_endpoint(**values)

    def test_self_hosted_metadata_is_required_and_loopback_is_rejected_for_kubernetes(
        self,
    ) -> None:
        with self.assertRaisesRegex(BenchmarkError, "inference_server"):
            validate_endpoint(
                model="Qwen/Qwen2.5-72B-Instruct",
                base_url="http://vllm.inference.svc:8000/v1",
                endpoint_class="self-hosted",
                input_price="0",
                output_price="0",
            )
        endpoint = validate_endpoint(
            model="Qwen/Qwen2.5-72B-Instruct",
            base_url="http://vllm.inference.svc:8000/v1",
            endpoint_class="self-hosted",
            input_price="0",
            output_price="0",
            inference_server="vllm/0.6.3",
            quantization="fp16",
            hardware="1xA100-80GB",
            topology="same-cluster Service vllm.inference.svc:8000",
        )
        self.assertEqual(endpoint["attestation"], "operator-supplied; AgentStorm does not probe GPUs")
        with self.assertRaisesRegex(BenchmarkError, "loopback"):
            validate_endpoint(
                model="Qwen/Qwen2.5-72B-Instruct",
                base_url="http://127.0.0.1:8000/v1",
                endpoint_class="self-hosted",
                input_price="0",
                output_price="0",
                inference_server="vllm/0.6.3",
                quantization="none",
                hardware="1xA100-80GB",
                topology="operator laptop tunnel",
                reject_loopback=True,
            )

    def test_real_modes_refuse_to_start_without_an_explicit_spend_opt_in(self) -> None:
        previous = os.environ.pop("AGENTSTORM_ALLOW_PROVIDER_SPEND", None)
        try:
            with self.assertRaisesRegex(BenchmarkError, "AGENTSTORM_ALLOW_PROVIDER_SPEND"):
                require_spend_opt_in()
        finally:
            if previous is not None:
                os.environ["AGENTSTORM_ALLOW_PROVIDER_SPEND"] = previous

    def test_kubernetes_manifest_pins_endpoint_and_omits_secret_values(self) -> None:
        secret = "never-serialize-this-api-key"
        resource = compatible_run_resource(
            "sreoc-run",
            "agentstorm-system",
            "dataset",
            "scenario",
            "provider-secret",
            "ghcr.io/alphasxd/agentstorm-worker@sha256:" + "a" * 64,
            16,
            "qwen2.5-72b-instruct",
            "https://example.invalid/compatible-mode/v1",
            "1.25",
            "5",
            True,
        )
        rendered = json.dumps(resource, sort_keys=True)
        self.assertEqual(resource["spec"]["target"]["provider"], "openai-compatible")
        self.assertEqual(
            resource["spec"]["target"]["baseURL"],
            "https://example.invalid/compatible-mode/v1",
        )
        self.assertEqual(resource["spec"]["target"]["apiKeySecretRef"]["name"], "provider-secret")
        self.assertNotIn(secret, rendered)
        self.assertNotIn("OPENAI_API_KEY", rendered)

    def test_non_canonical_report_makes_no_performance_claim(self) -> None:
        manifest = {
            "mode": "openai-compatible-keda",
            "complete": False,
            "canonical": False,
            "error": "USD 8 recorded-cost stop reached before the next run",
            "claims": claim_policy("openai-compatible-keda", canonical=False, complete=False),
            "limitations": ["Partial matrices are not publishable."],
        }
        report = render_report(manifest, []).lower()
        for forbidden in ("agents/s", "speedup", "efficiency"):
            self.assertNotIn(forbidden, report)
        self.assertIn("not publishable", report)


class MockProtocolTest(unittest.TestCase):
    def test_scripted_model_follows_the_tool_chain_before_the_diagnosis(self) -> None:
        fixtures = load_fixtures()
        incident = fixtures["INC-001"]
        messages = [
            {"role": "user", "content": "Investigate incident INC-001."},
        ]
        status, payload = scripted_completion(
            {"model": "mock-sre-protocol", "messages": messages}, fixtures
        )
        self.assertEqual(status, 200)
        tool_calls = payload["choices"][0]["message"]["tool_calls"]
        self.assertEqual(
            [call["function"]["name"] for call in tool_calls], ["get_incident"]
        )
        messages.append(payload["choices"][0]["message"])
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_calls[0]["id"],
                "content": json.dumps(
                    {
                        "incident_id": "INC-001",
                        "service": incident["service"],
                        "window": incident["window"],
                    }
                ),
            }
        )
        _, second = scripted_completion(
            {"model": "mock-sre-protocol", "messages": messages}, fixtures
        )
        names = [
            call["function"]["name"]
            for call in second["choices"][0]["message"]["tool_calls"]
        ]
        self.assertEqual(names, ["query_metrics", "search_logs"])
        self.assertEqual(diagnosis(incident)["incident_id"], "INC-001")

    def test_mock_benchmark_is_complete_checksummed_and_secret_free(self) -> None:
        secret = "mock-must-not-record-this-key"
        previous = os.environ.get("OPENAI_API_KEY")
        os.environ["OPENAI_API_KEY"] = secret
        try:
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "artifacts"
                asyncio.run(execute_mock(output, "full", "sha256:" + "b" * 64))
                manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
                runs = json.loads((output / "runs.json").read_text(encoding="utf-8"))
                report = (output / "REPORT.md").read_text(encoding="utf-8").lower()
                self.assertEqual(manifest["mode"], "openai-compatible-mock")
                self.assertTrue(manifest["complete"])
                self.assertFalse(manifest["canonical"])
                self.assertFalse(manifest["claims"]["performance"])
                self.assertEqual(manifest["pricing_usd_per_million_tokens"]["input"], "1")
                self.assertEqual(len(runs), 13)
                self.assertEqual(
                    sum(run["total"] for run in runs if run["kind"] == "capacity"), 384
                )
                for forbidden in ("agents/s", "latency", "speedup", "efficiency", "cost usd"):
                    self.assertNotIn(forbidden, report)
                rendered = "".join(
                    path.read_text(encoding="utf-8", errors="ignore")
                    for path in output.iterdir()
                    if path.suffix != ".gz"
                )
                self.assertNotIn(secret, rendered)
                subprocess.run(
                    ["sha256sum", "-c", "SHA256SUMS"],
                    cwd=output,
                    check=True,
                    capture_output=True,
                )
        finally:
            if previous is None:
                os.environ.pop("OPENAI_API_KEY", None)
            else:
                os.environ["OPENAI_API_KEY"] = previous


if __name__ == "__main__":
    unittest.main()
