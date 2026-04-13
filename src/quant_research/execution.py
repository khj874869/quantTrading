from __future__ import annotations

import json
from pathlib import Path

from .config import Config
from .exports import export_order_blotter
from .pipeline import PreparedData
from .strategy import MultiSignalStrategy
from .utils import read_csv_dicts, write_csv_dicts


class ExecutionReconciler:
    def __init__(self, config: Config, prepared_data: PreparedData, output_dir: Path) -> None:
        self.config = config
        self.prepared_data = prepared_data
        self.output_dir = output_dir

    def run(self) -> tuple[list[Path], dict[str, object]]:
        strategy = MultiSignalStrategy(self.config.strategy)
        blotter_outputs = export_order_blotter(
            self.prepared_data,
            strategy,
            self.output_dir,
            blotter_notional=float(
                self.config.strategy.get(
                    "order_blotter_notional",
                    self.config.strategy.get("slippage_notional", self.config.strategy.get("capacity_baseline_aum", 1_000_000.0)),
                )
            ),
            order_type=str(self.config.strategy.get("order_blotter_order_type", "MOC")),
        )
        fills_path = self._fills_path()
        if not fills_path.exists():
            raise FileNotFoundError(f"missing fills input: {fills_path}")

        expected_orders = read_csv_dicts(self.output_dir / "order_blotter.csv")
        fill_rows = read_csv_dicts(fills_path)
        fill_groups = self._group_fills(fill_rows)

        reconciliation_rows = []
        matched_fill_keys: set[tuple[str, str, str]] = set()
        for order in expected_orders:
            key = (order["rebalance_date"], order["permno"], order["side"])
            fills = fill_groups.get(key, [])
            if fills:
                matched_fill_keys.add(key)
            filled_shares = sum(float(fill.get("filled_shares", 0.0) or 0.0) for fill in fills)
            filled_notional = sum(
                float(fill.get("filled_notional", 0.0) or 0.0)
                if str(fill.get("filled_notional", "")).strip()
                else float(fill.get("filled_shares", 0.0) or 0.0) * float(fill.get("fill_price", 0.0) or 0.0)
                for fill in fills
            )
            explicit_fee_cost = sum(self._explicit_fee_cost(fill) for fill in fills)
            expected_shares = float(order.get("estimated_shares", 0.0) or 0.0)
            expected_notional = float(order.get("estimated_notional", 0.0) or 0.0)
            expected_price = float(order.get("price", 0.0) or 0.0)
            average_fill_price = filled_notional / filled_shares if filled_shares > 0.0 else 0.0
            fill_ratio = filled_shares / expected_shares if expected_shares > 0.0 else 0.0
            residual_shares = expected_shares - filled_shares
            implementation_shortfall_bps = self._implementation_shortfall_bps(
                side=order["side"],
                expected_price=expected_price,
                average_fill_price=average_fill_price,
            )
            implementation_shortfall_dollars = self._implementation_shortfall_dollars(
                side=order["side"],
                expected_price=expected_price,
                average_fill_price=average_fill_price,
                filled_shares=filled_shares,
            )
            total_execution_cost = implementation_shortfall_dollars + explicit_fee_cost
            reconciliation_rows.append(
                {
                    "rebalance_date": order["rebalance_date"],
                    "permno": order["permno"],
                    "side": order["side"],
                    "order_type": order["order_type"],
                    "position_transition": order["position_transition"],
                    "expected_shares": expected_shares,
                    "filled_shares": filled_shares,
                    "residual_shares": residual_shares,
                    "fill_ratio": fill_ratio,
                    "expected_notional": expected_notional,
                    "filled_notional": filled_notional,
                    "expected_price": expected_price,
                    "average_fill_price": average_fill_price,
                    "implementation_shortfall_bps": implementation_shortfall_bps,
                    "implementation_shortfall_dollars": implementation_shortfall_dollars,
                    "explicit_fee_cost": explicit_fee_cost,
                    "total_execution_cost": total_execution_cost,
                    "fill_count": len(fills),
                    "fill_status": self._fill_status(expected_shares, filled_shares),
                }
            )

        unmatched_fills = [
            {
                "rebalance_date": fill["rebalance_date"],
                "permno": fill["permno"],
                "side": fill["side"],
                "filled_shares": float(fill.get("filled_shares", 0.0) or 0.0),
                "fill_price": float(fill.get("fill_price", 0.0) or 0.0),
                "filled_notional": (
                    float(fill.get("filled_notional", 0.0) or 0.0)
                    if str(fill.get("filled_notional", "")).strip()
                    else float(fill.get("filled_shares", 0.0) or 0.0) * float(fill.get("fill_price", 0.0) or 0.0)
                ),
                "fill_timestamp": fill.get("fill_timestamp", ""),
            }
            for fill in fill_rows
            if (fill["rebalance_date"], fill["permno"], fill["side"]) not in matched_fill_keys
        ]

        reconciliation_path = self.output_dir / "execution_reconciliation.csv"
        unmatched_path = self.output_dir / "execution_unmatched_fills.csv"
        costs_by_rebalance_path = self.output_dir / "execution_costs_by_rebalance.csv"
        costs_by_side_path = self.output_dir / "execution_costs_by_side.csv"
        top_orders_path = self.output_dir / "execution_costs_top_orders.csv"
        summary_path = self.output_dir / "execution_summary.json"
        self._write_rows(
            reconciliation_path,
            reconciliation_rows,
            [
                "rebalance_date",
                "permno",
                "side",
                "order_type",
                "position_transition",
                "expected_shares",
                "filled_shares",
                "residual_shares",
                "fill_ratio",
                "expected_notional",
                "filled_notional",
                "expected_price",
                "average_fill_price",
                "implementation_shortfall_bps",
                "implementation_shortfall_dollars",
                "explicit_fee_cost",
                "total_execution_cost",
                "fill_count",
                "fill_status",
            ],
        )
        self._write_rows(
            unmatched_path,
            unmatched_fills,
            [
                "rebalance_date",
                "permno",
                "side",
                "filled_shares",
                "fill_price",
                "filled_notional",
                "fill_timestamp",
            ],
        )
        costs_by_rebalance = self._aggregate_costs_by_field(reconciliation_rows, "rebalance_date")
        costs_by_side = self._aggregate_costs_by_field(reconciliation_rows, "side")
        top_orders = self._top_cost_orders(reconciliation_rows)
        self._write_rows(
            costs_by_rebalance_path,
            costs_by_rebalance,
            [
                "rebalance_date",
                "order_count",
                "matched_order_count",
                "fill_rate",
                "expected_notional",
                "filled_notional",
                "implementation_shortfall_dollars",
                "explicit_fee_cost",
                "total_execution_cost",
                "average_implementation_shortfall_bps",
            ],
        )
        self._write_rows(
            costs_by_side_path,
            costs_by_side,
            [
                "side",
                "order_count",
                "matched_order_count",
                "fill_rate",
                "expected_notional",
                "filled_notional",
                "implementation_shortfall_dollars",
                "explicit_fee_cost",
                "total_execution_cost",
                "average_implementation_shortfall_bps",
            ],
        )
        self._write_rows(
            top_orders_path,
            top_orders,
            [
                "rebalance_date",
                "permno",
                "side",
                "fill_status",
                "expected_notional",
                "filled_notional",
                "implementation_shortfall_bps",
                "implementation_shortfall_dollars",
                "explicit_fee_cost",
                "total_execution_cost",
            ],
        )
        summary = self._summary_payload(reconciliation_rows, unmatched_fills, fills_path)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return [
            *blotter_outputs,
            reconciliation_path,
            unmatched_path,
            costs_by_rebalance_path,
            costs_by_side_path,
            top_orders_path,
            summary_path,
        ], summary

    def _fills_path(self) -> Path:
        fills_value = self.config.paths.get("broker_fills", self.config.paths.get("fills", "data/broker_fills.csv"))
        return self.config.resolve_path(fills_value)

    def _group_fills(self, rows: list[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, str]]]:
        grouped: dict[tuple[str, str, str], list[dict[str, str]]] = {}
        for row in rows:
            key = (row["rebalance_date"], row["permno"], str(row["side"]).strip().upper())
            normalized = dict(row)
            normalized["side"] = str(normalized["side"]).strip().upper()
            grouped.setdefault(key, []).append(normalized)
        return grouped

    def _implementation_shortfall_bps(self, side: str, expected_price: float, average_fill_price: float) -> float:
        if expected_price <= 0.0 or average_fill_price <= 0.0:
            return 0.0
        if side == "BUY":
            return ((average_fill_price / expected_price) - 1.0) * 10000.0
        return ((expected_price / average_fill_price) - 1.0) * 10000.0

    def _implementation_shortfall_dollars(
        self,
        side: str,
        expected_price: float,
        average_fill_price: float,
        filled_shares: float,
    ) -> float:
        if expected_price <= 0.0 or average_fill_price <= 0.0 or filled_shares <= 0.0:
            return 0.0
        if side == "BUY":
            return max(average_fill_price - expected_price, 0.0) * filled_shares
        return max(expected_price - average_fill_price, 0.0) * filled_shares

    def _explicit_fee_cost(self, fill: dict[str, str]) -> float:
        fee_fields = (
            "commission",
            "broker_commission",
            "exchange_fee",
            "regulatory_fee",
            "tax",
            "other_fee",
        )
        return sum(float(fill.get(field, 0.0) or 0.0) for field in fee_fields)

    def _fill_status(self, expected_shares: float, filled_shares: float) -> str:
        if expected_shares <= 0.0:
            return "no_expected"
        if filled_shares <= 1e-12:
            return "unfilled"
        if filled_shares >= expected_shares - 1e-9:
            return "filled"
        return "partial"

    def _summary_payload(
        self,
        reconciliation_rows: list[dict[str, object]],
        unmatched_fills: list[dict[str, object]],
        fills_path: Path,
    ) -> dict[str, object]:
        expected_shares = sum(float(row.get("expected_shares", 0.0) or 0.0) for row in reconciliation_rows)
        filled_shares = sum(float(row.get("filled_shares", 0.0) or 0.0) for row in reconciliation_rows)
        expected_notional = sum(float(row.get("expected_notional", 0.0) or 0.0) for row in reconciliation_rows)
        filled_notional = sum(float(row.get("filled_notional", 0.0) or 0.0) for row in reconciliation_rows)
        shortfall_rows = [
            float(row.get("implementation_shortfall_bps", 0.0) or 0.0)
            for row in reconciliation_rows
            if float(row.get("filled_shares", 0.0) or 0.0) > 0.0
        ]
        total_implementation_shortfall_dollars = sum(
            float(row.get("implementation_shortfall_dollars", 0.0) or 0.0) for row in reconciliation_rows
        )
        total_explicit_fee_cost = sum(float(row.get("explicit_fee_cost", 0.0) or 0.0) for row in reconciliation_rows)
        total_execution_cost = sum(float(row.get("total_execution_cost", 0.0) or 0.0) for row in reconciliation_rows)
        worst_row = max(
            reconciliation_rows,
            key=lambda row: float(row.get("total_execution_cost", 0.0) or 0.0),
            default=None,
        )
        return {
            "fills_path": str(fills_path),
            "expected_order_count": len(reconciliation_rows),
            "matched_order_count": sum(1 for row in reconciliation_rows if int(row.get("fill_count", 0) or 0) > 0),
            "unmatched_fill_count": len(unmatched_fills),
            "total_expected_shares": expected_shares,
            "total_filled_shares": filled_shares,
            "share_fill_rate": (filled_shares / expected_shares) if expected_shares > 0.0 else 0.0,
            "total_expected_notional": expected_notional,
            "total_filled_notional": filled_notional,
            "average_fill_ratio": (
                sum(float(row.get("fill_ratio", 0.0) or 0.0) for row in reconciliation_rows) / len(reconciliation_rows)
                if reconciliation_rows
                else 0.0
            ),
            "average_implementation_shortfall_bps": (
                sum(shortfall_rows) / len(shortfall_rows)
                if shortfall_rows
                else 0.0
            ),
            "total_implementation_shortfall_dollars": total_implementation_shortfall_dollars,
            "total_explicit_fee_cost": total_explicit_fee_cost,
            "total_execution_cost": total_execution_cost,
            "execution_cost_bps_vs_filled_notional": (
                (total_execution_cost / filled_notional) * 10000.0
                if filled_notional > 0.0
                else 0.0
            ),
            "worst_order_by_execution_cost": (
                {
                    "rebalance_date": str(worst_row["rebalance_date"]),
                    "permno": str(worst_row["permno"]),
                    "side": str(worst_row["side"]),
                    "fill_status": str(worst_row["fill_status"]),
                    "total_execution_cost": float(worst_row.get("total_execution_cost", 0.0) or 0.0),
                }
                if worst_row is not None
                else None
            ),
            "latest_rebalance_date": max((str(row["rebalance_date"]) for row in reconciliation_rows), default=None),
        }

    def _aggregate_costs_by_field(
        self,
        reconciliation_rows: list[dict[str, object]],
        field: str,
    ) -> list[dict[str, object]]:
        grouped: dict[str, list[dict[str, object]]] = {}
        for row in reconciliation_rows:
            grouped.setdefault(str(row.get(field, "")), []).append(row)
        aggregated_rows = []
        for key in sorted(grouped):
            rows = grouped[key]
            expected_notional = sum(float(row.get("expected_notional", 0.0) or 0.0) for row in rows)
            filled_notional = sum(float(row.get("filled_notional", 0.0) or 0.0) for row in rows)
            shortfall_bps_values = [
                float(row.get("implementation_shortfall_bps", 0.0) or 0.0)
                for row in rows
                if float(row.get("filled_shares", 0.0) or 0.0) > 0.0
            ]
            aggregated_rows.append(
                {
                    field: key,
                    "order_count": len(rows),
                    "matched_order_count": sum(1 for row in rows if int(row.get("fill_count", 0) or 0) > 0),
                    "fill_rate": (
                        sum(float(row.get("filled_shares", 0.0) or 0.0) for row in rows)
                        / sum(float(row.get("expected_shares", 0.0) or 0.0) for row in rows)
                        if sum(float(row.get("expected_shares", 0.0) or 0.0) for row in rows) > 0.0
                        else 0.0
                    ),
                    "expected_notional": expected_notional,
                    "filled_notional": filled_notional,
                    "implementation_shortfall_dollars": sum(
                        float(row.get("implementation_shortfall_dollars", 0.0) or 0.0) for row in rows
                    ),
                    "explicit_fee_cost": sum(float(row.get("explicit_fee_cost", 0.0) or 0.0) for row in rows),
                    "total_execution_cost": sum(float(row.get("total_execution_cost", 0.0) or 0.0) for row in rows),
                    "average_implementation_shortfall_bps": (
                        sum(shortfall_bps_values) / len(shortfall_bps_values)
                        if shortfall_bps_values
                        else 0.0
                    ),
                }
            )
        return aggregated_rows

    def _top_cost_orders(self, reconciliation_rows: list[dict[str, object]]) -> list[dict[str, object]]:
        ranked = sorted(
            reconciliation_rows,
            key=lambda row: float(row.get("total_execution_cost", 0.0) or 0.0),
            reverse=True,
        )
        top_n = max(int(self.config.strategy.get("execution_reconcile_top_n", 10)), 1)
        return [
            {
                "rebalance_date": row["rebalance_date"],
                "permno": row["permno"],
                "side": row["side"],
                "fill_status": row["fill_status"],
                "expected_notional": row["expected_notional"],
                "filled_notional": row["filled_notional"],
                "implementation_shortfall_bps": row["implementation_shortfall_bps"],
                "implementation_shortfall_dollars": row["implementation_shortfall_dollars"],
                "explicit_fee_cost": row["explicit_fee_cost"],
                "total_execution_cost": row["total_execution_cost"],
            }
            for row in ranked[:top_n]
        ]

    def _write_rows(self, path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
        write_csv_dicts(
            path,
            rows if rows else [{field: "" for field in fieldnames}],
        )
        if not rows:
            path.write_text(",".join(fieldnames) + "\n", encoding="utf-8")
