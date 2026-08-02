from __future__ import annotations

import unittest

from agentstorm_worker.config import PricingConfig
from agentstorm_worker.pricing import case_costs, sum_costs, token_cost_usd


class PricingTest(unittest.TestCase):
    def test_calculates_fixed_precision_cost(self) -> None:
        pricing = PricingConfig(
            input_usd_per_million_tokens="2.5",
            output_usd_per_million_tokens="10",
        )
        input_cost, output_cost, total = case_costs(pricing, 1000, 500)

        self.assertEqual(input_cost, "0.002500000000")
        self.assertEqual(output_cost, "0.005000000000")
        self.assertEqual(total, "0.007500000000")
        self.assertEqual(token_cost_usd(1, "0.001"), "0.000000001000")
        self.assertEqual(
            sum_costs(["0.002500000000", "0.005000000000"]),
            "0.007500000000",
        )

    def test_unpriced_cost_remains_unknown(self) -> None:
        self.assertEqual(case_costs(None, 1000, 500), (None, None, None))
        self.assertIsNone(sum_costs([None]))


if __name__ == "__main__":
    unittest.main()
