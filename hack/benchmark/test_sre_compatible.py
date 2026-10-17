from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openai_compatible_mock import diagnosis, load_fixtures, scripted_completion
from agentstorm_worker.benchmarks.sre import (
    SRE_COMPATIBLE_INPUT_TOKEN_ALLOWANCE,
    SRE_COMPATIBLE_MAX_MODEL_CALLS,
    SRE_COMPATIBLE_MAX_OUTPUT_TOKENS,
    largest_scripted_request_tokens,
)
from run_sre_compatible import (
    REAL_CASE_TIMEOUT_SECONDS,
    BenchmarkError,
    claim_policy,
    execute_mock,
    render_report,
    require_spend_opt_in,
    reserve_benchmark_spend,
    validate_endpoint,
    worst_case_run_usd,
)
from run_sre_compatible_keda import compatible_run_resource, drive_until_budget

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
        self.assertEqual(
            resource["spec"]["workload"]["timeoutSeconds"], REAL_CASE_TIMEOUT_SECONDS
        )
        self.assertNotIn(secret, rendered)
        self.assertNotIn("OPENAI_API_KEY", rendered)

    def test_unreachable_worker_endpoints_are_rejected_and_routable_ones_pass(self) -> None:
        attested = {
            "endpoint_class": "self-hosted",
            "input_price": "0",
            "output_price": "0",
            "inference_server": "vllm/0.6.3",
            "quantization": "none",
            "hardware": "1xA100-80GB",
            "topology": "operator laptop tunnel",
            "reject_loopback": True,
        }
        for base_url in (
            "http://0.0.0.0:8000/v1",
            "http://[::1]:8000/v1",
            "http://[::]:8000/v1",
            "http://localhost.:8000/v1",
            "http://127.0.0.1.:8000/v1",
            "http://[::ffff:127.0.0.1]:8000/v1",
            "http://[::ffff:0.0.0.0]:8000/v1",
            "http://[::ffff:7f00:1]:8000/v1",
            "http://[0:0:0:0:0:ffff:127.0.0.1]:8000/v1",
        ):
            with self.subTest(base_url=base_url):
                with self.assertRaisesRegex(BenchmarkError, "loopback or unspecified"):
                    validate_endpoint(
                        model="Qwen/Qwen2.5-72B-Instruct",
                        base_url=base_url,
                        **attested,
                    )
        endpoint = validate_endpoint(
            model="Qwen/Qwen2.5-72B-Instruct",
            base_url="http://10.1.2.3:8000/v1",
            **attested,
        )
        self.assertEqual(endpoint["host"], "10.1.2.3")
        mapped = validate_endpoint(
            model="Qwen/Qwen2.5-72B-Instruct",
            base_url="http://[::ffff:10.1.2.3]:8000/v1",
            **attested,
        )
        self.assertEqual(mapped["host"], "::ffff:10.1.2.3")

    def test_input_allowance_covers_scripted_requests_and_resent_history(self) -> None:
        measured = largest_scripted_request_tokens()
        resent_history = (SRE_COMPATIBLE_MAX_MODEL_CALLS - 1) * SRE_COMPATIBLE_MAX_OUTPUT_TOKENS
        self.assertGreater(measured, SRE_COMPATIBLE_MAX_OUTPUT_TOKENS)
        self.assertGreater(SRE_COMPATIBLE_INPUT_TOKEN_ALLOWANCE, measured + resent_history)
        calls = Decimal(32 * SRE_COMPATIBLE_MAX_MODEL_CALLS)
        self.assertEqual(
            worst_case_run_usd("1", "0", 1),
            calls * Decimal(SRE_COMPATIBLE_INPUT_TOKEN_ALLOWANCE) / Decimal(1_000_000),
        )
        self.assertEqual(
            worst_case_run_usd("0", "1", 1),
            calls * Decimal(SRE_COMPATIBLE_MAX_OUTPUT_TOKENS) / Decimal(1_000_000),
        )

    def test_spend_reserve_stops_before_one_run_can_pass_the_cap(self) -> None:
        one_attempt = worst_case_run_usd("1", "2", 1)
        self.assertEqual(worst_case_run_usd("1", "2", 2), one_attempt * 2)
        self.assertGreater(one_attempt, worst_case_run_usd("0", "2", 1))
        reserve_benchmark_spend(Decimal(0), "1", "2", 1)
        with self.assertRaisesRegex(BenchmarkError, "estimate"):
            reserve_benchmark_spend(Decimal(0), "0", "70", 1)
        with self.assertRaisesRegex(BenchmarkError, "estimate"):
            reserve_benchmark_spend(Decimal("7.9"), "1", "2", 1)

    def test_keda_budget_stop_skips_a_run_the_estimate_cannot_cover(self) -> None:
        started: list[str] = []

        def start(kind: str, workers: int, round_index: int, sequence: int) -> dict[str, object]:
            del workers, round_index, sequence
            started.append(kind)
            return {"kind": kind, "cost_usd": "7.50", "total": 32}

        runs, error, stopped = drive_until_budget("0", "70", start)
        self.assertEqual(started, [])
        self.assertEqual(runs, [])
        self.assertTrue(stopped)
        self.assertIn("estimate", error or "")

        started.clear()
        runs, error, stopped = drive_until_budget("1", "2", start)
        self.assertEqual(started, ["capacity"])
        self.assertEqual(len(runs), 1)
        self.assertTrue(stopped)
        self.assertIn("estimate", error or "")

        def unknown(kind: str, workers: int, round_index: int, sequence: int) -> dict[str, object]:
            del workers, round_index, sequence
            return {"kind": kind, "cost_usd": None, "total": 32}

        runs, error, stopped = drive_until_budget("1", "2", unknown)
        self.assertEqual(len(runs), 1)
        self.assertFalse(stopped)
        self.assertIn("unknown", error or "")

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
