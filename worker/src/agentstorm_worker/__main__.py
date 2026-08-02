from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys

from .adapters import create_adapter
from .config import load_dataset, load_run_config
from .models import CaseResult, RunSummary
from .results import ResultClient
from .runner import WorkloadRunner, write_results
from .telemetry import telemetry_from_environment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AgentStorm execution worker")
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="execute the assigned dataset shard")
    run.add_argument("--config", default=os.getenv("AGENTSTORM_RUN_CONFIG", "run.json"))
    run.add_argument("--dataset", default=os.getenv("AGENTSTORM_DATASET", "cases.jsonl"))
    run.add_argument("--output", default=os.getenv("AGENTSTORM_OUTPUT_DIR", "out"))
    validate = subparsers.add_parser("validate", help="validate config and dataset inputs")
    validate.add_argument("--config", default=os.getenv("AGENTSTORM_RUN_CONFIG", "run.json"))
    validate.add_argument("--dataset", default=os.getenv("AGENTSTORM_DATASET", "cases.jsonl"))
    return parser


async def execute(args: argparse.Namespace) -> int:
    config = load_run_config(args.config)
    cases = load_dataset(args.dataset)
    if args.command == "validate":
        print(json.dumps({"run_id": config.run_id, "cases": len(cases)}, indent=2))
        return 0

    shard_index = int(os.getenv("AGENTSTORM_SHARD_INDEX", "0"))
    shard_count = int(os.getenv("AGENTSTORM_SHARD_COUNT", "1"))
    telemetry = telemetry_from_environment(config.telemetry)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stop_event.set)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError, ValueError):
            pass
    run_attributes: dict[str, str | int | bool | float] = {
        "agentstorm.run.id": config.run_id,
        "agentstorm.run.namespace": config.source.namespace,
        "agentstorm.run.name": config.source.name,
        "agentstorm.shard.index": shard_index,
        "agentstorm.shard.count": shard_count,
        "gen_ai.provider.name": config.target.provider,
    }
    if config.target.model:
        run_attributes["gen_ai.request.model"] = config.target.model
    result_client: ResultClient | None = None
    registered = False
    try:
        with telemetry.start_span("agentstorm.run", run_attributes) as run_span:
            try:
                result_client = ResultClient.from_environment()
                pre_registered = (
                    os.getenv("AGENTSTORM_RESULT_PRE_REGISTERED", "false").lower()
                    == "true"
                )
                if result_client is not None and not pre_registered:
                    await asyncio.to_thread(result_client.register_run, config, shard_count)
                    registered = True
                elif result_client is not None:
                    registered = True
                adapter = create_adapter(
                    config.target.provider,
                    model=config.target.model,
                    base_url=config.target.base_url,
                )
                runner = WorkloadRunner(
                    config,
                    adapter,
                    shard_index=shard_index,
                    shard_count=shard_count,
                    telemetry=telemetry,
                )
                results, summary = await runner.execute(cases, stop_event=stop_event)
                if stop_event.is_set():
                    await _finish_cancelled(
                        result_client,
                        config.run_id,
                        shard_index,
                        results,
                        summary,
                        args.output,
                    )
                    run_span.set_attribute("agentstorm.run.cancelled", True)
                    return 130
                write_results(args.output, results, summary)
                if result_client is not None:
                    await asyncio.to_thread(
                        result_client.upload_shard,
                        config.run_id,
                        shard_index,
                        results,
                        summary,
                    )
                for name, value in {
                    "agentstorm.run.total": summary.total,
                    "agentstorm.run.succeeded": summary.succeeded,
                    "agentstorm.run.failed": summary.failed,
                    "agentstorm.run.thresholds_passed": summary.thresholds_passed,
                    "gen_ai.usage.input_tokens": summary.input_tokens,
                    "gen_ai.usage.output_tokens": summary.output_tokens,
                }.items():
                    run_span.set_attribute(name, value)
                print(json.dumps(summary.to_dict(), ensure_ascii=False))
                return 0 if summary.thresholds_passed else 2
            except Exception as exc:
                run_span.set_error(type(exc).__name__)
                if registered and result_client is not None:
                    try:
                        await asyncio.wait_for(
                            asyncio.to_thread(
                                result_client.mark_terminal,
                                config.run_id,
                                "harness_failed",
                                "worker_harness_failure",
                            ),
                            timeout=10,
                        )
                    except Exception:
                        print("AgentStorm result sink terminal update failed", file=sys.stderr)
                print("AgentStorm worker harness failure", file=sys.stderr)
                return 3
    finally:
        for signum in installed_signals:
            loop.remove_signal_handler(signum)
        telemetry.shutdown()


async def _finish_cancelled(
    result_client: ResultClient | None,
    run_id: str,
    shard_index: int,
    results: list[CaseResult],
    summary: RunSummary,
    output: str,
) -> None:
    async def flush() -> None:
        if result_client is not None:
            try:
                await asyncio.to_thread(
                    result_client.mark_terminal,
                    run_id,
                    "cancelled",
                    "cancellation_requested",
                )
            except Exception:
                print("AgentStorm result sink cancellation update failed", file=sys.stderr)
        try:
            write_results(output, results, summary)
        except Exception:
            print("AgentStorm partial local result write failed", file=sys.stderr)
        if result_client is not None:
            try:
                await asyncio.to_thread(
                    result_client.upload_shard, run_id, shard_index, results, summary
                )
            except Exception:
                print("AgentStorm partial result upload failed", file=sys.stderr)

    try:
        await asyncio.wait_for(flush(), timeout=10)
    except TimeoutError:
        print("AgentStorm cancellation flush budget exhausted", file=sys.stderr)
    except Exception:
        print("AgentStorm cancellation flush failed", file=sys.stderr)


def main() -> None:
    args = build_parser().parse_args()
    try:
        exit_code = asyncio.run(execute(args))
    except KeyboardInterrupt:
        exit_code = 130
    except Exception:
        print("AgentStorm worker harness failure", file=sys.stderr)
        exit_code = 3
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
