from __future__ import annotations

from ..models import AdapterResponse, TestCase


class OpenAIAgentsAdapter:
    """Thin adapter around the optional OpenAI Agents SDK."""

    def __init__(self, model: str, base_url: str = "") -> None:
        if not model:
            raise ValueError("target.model is required for provider openai-agents")
        try:
            from agents import Agent, OpenAIProvider, RunConfig
        except ImportError as exc:
            raise RuntimeError(
                "openai-agents is not installed; install agentstorm-worker[openai]"
            ) from exc
        self._agent = Agent(
            name="AgentStorm target",
            instructions=(
                "Complete the supplied task accurately. Use concise answers and do not invent "
                "tool results."
            ),
            model=model,
        )
        self._run_config = (
            RunConfig(model_provider=OpenAIProvider(base_url=base_url)) if base_url else None
        )

    async def run(self, case: TestCase) -> AdapterResponse:
        from agents import Runner

        run_options = {"run_config": self._run_config} if self._run_config is not None else {}
        result = await Runner.run(self._agent, case.prompt, **run_options)
        usage = result.context_wrapper.usage
        return AdapterResponse(
            output=str(result.final_output),
            input_tokens=int(usage.input_tokens),
            output_tokens=int(usage.output_tokens),
        )
