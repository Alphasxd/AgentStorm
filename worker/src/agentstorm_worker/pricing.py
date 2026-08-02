from __future__ import annotations

from decimal import Decimal, localcontext

from .config import PricingConfig

_SCALE = Decimal("0.000000000001")
_MILLION = Decimal(1_000_000)


def token_cost_usd(tokens: int, price_per_million: str) -> str:
    with localcontext() as context:
        context.prec = 64
        cost = Decimal(tokens) * Decimal(price_per_million) / _MILLION
        return format(cost.quantize(_SCALE), "f")


def case_costs(
    pricing: PricingConfig | None, input_tokens: int, output_tokens: int
) -> tuple[str | None, str | None, str | None]:
    if pricing is None:
        return None, None, None
    input_cost = token_cost_usd(input_tokens, pricing.input_usd_per_million_tokens)
    output_cost = token_cost_usd(output_tokens, pricing.output_usd_per_million_tokens)
    total = format((Decimal(input_cost) + Decimal(output_cost)).quantize(_SCALE), "f")
    return input_cost, output_cost, total


def sum_costs(values: list[str | None]) -> str | None:
    if any(value is None for value in values):
        return None
    return format(sum((Decimal(value) for value in values if value is not None), Decimal(0)), "f")
