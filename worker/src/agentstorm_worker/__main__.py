from __future__ import annotations

import argparse
import asyncio
import json
import os

from .adapters import create_adapter
from .config import load_dataset, load_run_config
from .results import ResultClient
from .runner import WorkloadRunner, write_results


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
    result_client = ResultClient.from_environment()
    if result_client is not None:
        await asyncio.to_thread(result_client.register_run, config, shard_count)
    adapter = create_adapter(
        config.target.provider,
        model=config.target.model,
        base_url=config.target.base_url,
    )
    runner = WorkloadRunner(config, adapter, shard_index=shard_index, shard_count=shard_count)
    results, summary = await runner.execute(cases)
    write_results(args.output, results, summary)
    if result_client is not None:
        await asyncio.to_thread(
            result_client.upload_shard,
            config.run_id,
            shard_index,
            results,
            summary,
        )
    print(json.dumps(summary.to_dict(), ensure_ascii=False))
    return 0 if summary.thresholds_passed else 2


def main() -> None:
    args = build_parser().parse_args()
    raise SystemExit(asyncio.run(execute(args)))


if __name__ == "__main__":
    main()
