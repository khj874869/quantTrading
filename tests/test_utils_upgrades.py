from __future__ import annotations

import unittest

from quant_research.utils import resolve_backtest_costs, resolve_order_blotter_settings


class UtilsUpgradeTest(unittest.TestCase):
    def test_backtest_costs_default_slippage_from_transaction_minus_commission(self) -> None:
        costs = resolve_backtest_costs(
            {
                "transaction_cost_bps": 11.0,
                "commission_cost_bps": 3.0,
            }
        )

        self.assertEqual(costs["transaction_cost_bps"], 11.0)
        self.assertEqual(costs["commission_cost_bps"], 3.0)
        self.assertEqual(costs["slippage_cost_bps"], 8.0)

    def test_order_blotter_settings_fall_back_to_capacity_baseline(self) -> None:
        settings = resolve_order_blotter_settings(
            {
                "capacity_baseline_aum": 2500000.0,
            }
        )

        self.assertEqual(settings["blotter_notional"], 2500000.0)
        self.assertEqual(settings["order_type"], "MOC")


if __name__ == "__main__":
    unittest.main()
