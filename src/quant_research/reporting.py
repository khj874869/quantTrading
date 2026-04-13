from __future__ import annotations

import html
import json
from collections import defaultdict
from datetime import date
from pathlib import Path

from .config import Config
from .pipeline import PreparedData
from .utils import ensure_directory, parse_date, read_csv_dicts, write_csv_dicts


DIAGNOSTIC_FACTORS = {
    "book_to_market_z": 1.0,
    "roa_z": 1.0,
    "asset_growth_z": -1.0,
    "cash_flow_ratio_z": 1.0,
    "revision_z": 1.0,
    "dispersion_z": 1.0,
    "surprise_z": 1.0,
    "patent_intensity_z": 1.0,
    "citation_intensity_z": 1.0,
    "net_upgrades_z": 1.0,
    "composite_score": 1.0,
    "risk_adjusted_score": 1.0,
    "downside_beta_z": -1.0,
    "idio_vol_z": -1.0,
}


class PerformanceReporter:
    def __init__(self, config: Config, prepared_data: PreparedData, output_dir: Path) -> None:
        self.config = config
        self.prepared_data = prepared_data
        self.output_dir = output_dir

    def run(self) -> tuple[list[Path], dict[str, object]]:
        ensure_directory(self.output_dir)
        daily_rows = self._load_daily_rows()
        rebalance_rows = self._load_rebalance_rows()
        rebalance_weights = self._extract_rebalance_weights(rebalance_rows)
        monthly_rows = self._build_monthly_rows(daily_rows)
        security_rows, sector_rows, benchmark_hedge_contribution = self._build_contribution_rows(daily_rows, rebalance_weights)
        factor_rows, factor_regime_rows = self._build_factor_diagnostics()
        capacity_rows, capacity_breach_rows = self._build_capacity_diagnostics(rebalance_rows)
        stress_rows = self._build_stress_scenarios(daily_rows)
        summary = self._build_summary(
            monthly_rows,
            security_rows,
            sector_rows,
            benchmark_hedge_contribution,
            factor_rows,
            factor_regime_rows,
            capacity_rows,
            stress_rows,
        )

        monthly_path = self.output_dir / "report_monthly_returns.csv"
        sector_path = self.output_dir / "report_sector_contributions.csv"
        security_path = self.output_dir / "report_security_contributions.csv"
        factor_path = self.output_dir / "report_factor_diagnostics.csv"
        factor_regime_path = self.output_dir / "report_factor_regime_diagnostics.csv"
        capacity_path = self.output_dir / "report_capacity_curve.csv"
        capacity_breach_path = self.output_dir / "report_capacity_breaches.csv"
        stress_path = self.output_dir / "report_stress_scenarios.csv"
        summary_path = self.output_dir / "report_summary.json"
        html_path = self.output_dir / "report_dashboard.html"

        write_csv_dicts(monthly_path, monthly_rows)
        write_csv_dicts(sector_path, sector_rows)
        write_csv_dicts(security_path, security_rows)
        write_csv_dicts(factor_path, factor_rows)
        write_csv_dicts(factor_regime_path, factor_regime_rows)
        write_csv_dicts(capacity_path, capacity_rows)
        write_csv_dicts(capacity_breach_path, capacity_breach_rows)
        write_csv_dicts(stress_path, stress_rows)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        html_path.write_text(
            self._build_html_report(
                summary=summary,
                monthly_rows=monthly_rows,
                sector_rows=sector_rows,
                security_rows=security_rows,
                factor_rows=factor_rows,
                capacity_rows=capacity_rows,
                stress_rows=stress_rows,
            ),
            encoding="utf-8",
        )
        return [
            monthly_path,
            sector_path,
            security_path,
            factor_path,
            factor_regime_path,
            capacity_path,
            capacity_breach_path,
            stress_path,
            summary_path,
            html_path,
        ], summary

    def _load_daily_rows(self) -> list[dict[str, object]]:
        path = self.output_dir / "portfolio_daily_returns.csv"
        if not path.exists():
            raise FileNotFoundError(f"missing backtest output: {path}")
        rows = []
        for row in read_csv_dicts(path):
            rows.append(
                {
                    "date": parse_date(row["date"]),
                    "gross_return": float(row["gross_return"]),
                    "net_return": float(row["net_return"]),
                    "benchmark_return": float(row["benchmark_return"]),
                    "active_return": float(row["active_return"]),
                    "exposure": float(row["exposure"]),
                    "cash_weight": float(row.get("cash_weight", 0.0)),
                    "holdings": float(row.get("holdings", 0.0)),
                    "turnover": float(row["turnover"]),
                    "cash_drag": float(row.get("cash_drag", 0.0)),
                    "commission_cost": float(row.get("commission_cost", 0.0)),
                    "slippage_cost": float(row.get("slippage_cost", 0.0)),
                    "transaction_cost": float(row["transaction_cost"]),
                    "short_borrow_cost": float(row.get("short_borrow_cost", 0.0)),
                }
            )
        return sorted(rows, key=lambda item: item["date"])

    def _load_rebalance_rows(self) -> dict[date, list[dict[str, object]]]:
        path = self.output_dir / "portfolio_rebalances.csv"
        if not path.exists():
            raise FileNotFoundError(f"missing backtest output: {path}")
        by_date: dict[date, list[dict[str, object]]] = defaultdict(list)
        for row in read_csv_dicts(path):
            rebalance_date = parse_date(row["rebalance_date"])
            by_date[rebalance_date].append(
                {
                    "permno": row["permno"],
                    "weight": float(row["weight"]),
                }
            )
        return dict(sorted(by_date.items()))

    def _extract_rebalance_weights(self, rebalance_rows: dict[date, list[dict[str, object]]]) -> dict[date, dict[str, float]]:
        return {
            rebalance_date: {
                str(row["permno"]): float(row["weight"])
                for row in rows
            }
            for rebalance_date, rows in rebalance_rows.items()
        }

    def _build_monthly_rows(self, daily_rows: list[dict[str, object]]) -> list[dict[str, object]]:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in daily_rows:
            month_key = str(row["date"])[:7]
            grouped[month_key].append(row)

        monthly_rows = []
        for month_key, rows in sorted(grouped.items()):
            gross_equity = 1.0
            net_equity = 1.0
            benchmark_equity = 1.0
            for row in rows:
                gross_equity *= 1.0 + float(row["gross_return"])
                net_equity *= 1.0 + float(row["net_return"])
                benchmark_equity *= 1.0 + float(row["benchmark_return"])
            monthly_rows.append(
                {
                    "month": month_key,
                    "days": len(rows),
                    "gross_total_return": gross_equity - 1.0,
                    "net_total_return": net_equity - 1.0,
                    "benchmark_total_return": benchmark_equity - 1.0,
                    "active_total_return": (net_equity - 1.0) - (benchmark_equity - 1.0),
                    "transaction_cost_drag": (gross_equity - 1.0) - (net_equity - 1.0),
                    "total_transaction_cost": sum(float(row["transaction_cost"]) for row in rows),
                    "total_commission_cost": sum(float(row["commission_cost"]) for row in rows),
                    "total_slippage_cost": sum(float(row["slippage_cost"]) for row in rows),
                    "total_short_borrow_cost": sum(float(row.get("short_borrow_cost", 0.0)) for row in rows),
                    "average_turnover": sum(float(row["turnover"]) for row in rows) / len(rows),
                    "average_exposure": sum(float(row["exposure"]) for row in rows) / len(rows),
                    "average_cash_weight": sum(float(row["cash_weight"]) for row in rows) / len(rows),
                    "average_holdings": sum(float(row["holdings"]) for row in rows) / len(rows),
                }
            )
        return monthly_rows

    def _build_contribution_rows(
        self,
        daily_rows: list[dict[str, object]],
        rebalance_weights: dict[date, dict[str, float]],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]], float]:
        security_totals: dict[str, dict[str, object]] = {}
        sector_totals: dict[str, dict[str, object]] = {}
        benchmark_hedge_contribution = 0.0
        rebalance_dates = sorted(rebalance_weights)
        if not rebalance_dates:
            return [], [], benchmark_hedge_contribution

        sector_lookup_by_rebalance = {
            rebalance_date: {
                row["permno"]: str(row.get("sector", "UNKNOWN"))
                for row in rows
            }
            for rebalance_date, rows in self.prepared_data.features_by_rebalance.items()
        }

        rebalance_index = 0
        current_weights: dict[str, float] = {}
        current_sector_lookup: dict[str, str] = {}
        for daily_row in daily_rows:
            current_date = daily_row["date"]
            while rebalance_index < len(rebalance_dates) and rebalance_dates[rebalance_index] <= current_date:
                rebalance_date = rebalance_dates[rebalance_index]
                current_weights = rebalance_weights[rebalance_date]
                current_sector_lookup = sector_lookup_by_rebalance.get(rebalance_date, {})
                rebalance_index += 1
            if not current_weights:
                continue

            security_returns = self.prepared_data.returns_by_date.get(current_date, {})
            benchmark_return = self.prepared_data.benchmark_by_date.get(current_date, 0.0)
            for permno, weight in current_weights.items():
                asset_return = benchmark_return if permno == "__BENCH__" else security_returns.get(permno, 0.0)
                contribution = weight * asset_return
                if permno == "__BENCH__":
                    benchmark_hedge_contribution += contribution
                    continue
                sector = current_sector_lookup.get(permno, "UNKNOWN")
                security_entry = security_totals.setdefault(
                    permno,
                    {
                        "permno": permno,
                        "sector": sector,
                        "days_held": 0.0,
                        "total_weight": 0.0,
                        "total_abs_weight": 0.0,
                        "total_contribution": 0.0,
                    },
                )
                security_entry["sector"] = sector
                security_entry["days_held"] = float(security_entry["days_held"]) + 1.0
                security_entry["total_weight"] = float(security_entry["total_weight"]) + weight
                security_entry["total_abs_weight"] = float(security_entry["total_abs_weight"]) + abs(weight)
                security_entry["total_contribution"] = float(security_entry["total_contribution"]) + contribution

                sector_entry = sector_totals.setdefault(
                    sector,
                    {
                        "sector": sector,
                        "days_held": 0.0,
                        "total_weight": 0.0,
                        "total_abs_weight": 0.0,
                        "total_contribution": 0.0,
                    },
                )
                sector_entry["days_held"] = float(sector_entry["days_held"]) + 1.0
                sector_entry["total_weight"] = float(sector_entry["total_weight"]) + weight
                sector_entry["total_abs_weight"] = float(sector_entry["total_abs_weight"]) + abs(weight)
                sector_entry["total_contribution"] = float(sector_entry["total_contribution"]) + contribution

        security_rows = self._finalize_contribution_rows(security_totals, label_key="permno")
        sector_rows = self._finalize_contribution_rows(sector_totals, label_key="sector")
        return security_rows, sector_rows, benchmark_hedge_contribution

    def _finalize_contribution_rows(
        self,
        rows: dict[str, dict[str, object]],
        label_key: str,
    ) -> list[dict[str, object]]:
        finalized_rows = []
        for row in rows.values():
            days_held = float(row["days_held"])
            total_weight = float(row["total_weight"])
            total_abs_weight = float(row["total_abs_weight"])
            finalized = dict(row)
            finalized["days_held"] = int(days_held)
            finalized["average_weight"] = total_weight / days_held if days_held > 0.0 else 0.0
            finalized["average_abs_weight"] = total_abs_weight / days_held if days_held > 0.0 else 0.0
            finalized_rows.append(finalized)
        return sorted(
            finalized_rows,
            key=lambda item: (
                float(item["total_contribution"]),
                float(item["average_abs_weight"]),
                str(item[label_key]),
            ),
            reverse=True,
        )

    def _build_summary(
        self,
        monthly_rows: list[dict[str, object]],
        security_rows: list[dict[str, object]],
        sector_rows: list[dict[str, object]],
        benchmark_hedge_contribution: float,
        factor_rows: list[dict[str, object]],
        factor_regime_rows: list[dict[str, object]],
        capacity_rows: list[dict[str, object]],
        stress_rows: list[dict[str, object]],
    ) -> dict[str, object]:
        summary = self._load_backtest_summary()
        positive_months = sum(1 for row in monthly_rows if float(row["net_total_return"]) > 0.0)
        best_month = max(monthly_rows, key=lambda row: float(row["net_total_return"]), default=None)
        worst_month = min(monthly_rows, key=lambda row: float(row["net_total_return"]), default=None)
        feasible_capacity_rows = [row for row in capacity_rows if int(row.get("breached_trade_count", 0)) == 0]
        breached_capacity_rows = [row for row in capacity_rows if int(row.get("breached_trade_count", 0)) > 0]
        baseline_aum = max(float(self.config.strategy.get("capacity_baseline_aum", self.config.strategy.get("slippage_notional", 1_000_000.0))), 1.0)
        return {
            "backtest_summary": summary,
            "benchmark_mode": str(self.config.strategy.get("benchmark_mode", "ff_total_return")),
            "universe_rules": {
                "min_price": float(self.config.strategy.get("min_price", 5.0)),
                "min_market_cap": float(self.config.strategy.get("min_market_cap", 100_000_000.0)),
                "min_avg_dollar_volume": float(self.config.strategy.get("min_avg_dollar_volume", 0.0)),
                "universe_include_sectors": list(self.config.strategy.get("universe_include_sectors", [])),
                "universe_exclude_sectors": list(self.config.strategy.get("universe_exclude_sectors", [])),
                "universe_top_n_by_market_cap": int(self.config.strategy.get("universe_top_n_by_market_cap", 0)),
            },
            "monthly_positive_rate": (positive_months / len(monthly_rows)) if monthly_rows else 0.0,
            "best_month": best_month,
            "worst_month": worst_month,
            "benchmark_hedge_contribution": benchmark_hedge_contribution,
            "top_sectors": sector_rows[:5],
            "bottom_sectors": list(reversed(sector_rows[-5:])),
            "top_securities": security_rows[:10],
            "bottom_securities": list(reversed(security_rows[-10:])),
            "top_factors_by_ic": factor_rows[:5],
            "top_factors_by_spread": sorted(
                factor_rows,
                key=lambda row: (float(row.get("average_quintile_spread", 0.0)), float(row.get("spread_hit_rate", 0.0))),
                reverse=True,
            )[:5],
            "factor_regimes": factor_regime_rows,
            "capacity_model": {
                "baseline_aum": baseline_aum,
                "slippage_model": str(self.config.strategy.get("slippage_model", "fixed")).strip().lower() or "fixed",
                "max_trade_participation_ratio": float(self.config.strategy.get("max_trade_participation_ratio", 0.0)),
                "slippage_adv_floor": float(self.config.strategy.get("slippage_adv_floor", 100_000.0)),
                "slippage_impact_bps_per_adv": float(self.config.strategy.get("slippage_impact_bps_per_adv", 50.0)),
                "slippage_impact_exponent": self._slippage_impact_exponent(),
            },
            "capacity_curve": capacity_rows,
            "largest_aum_without_breach": feasible_capacity_rows[-1]["aum"] if feasible_capacity_rows else None,
            "first_breached_aum": breached_capacity_rows[0]["aum"] if breached_capacity_rows else None,
            "stress_scenarios": stress_rows,
            "worst_stress_scenarios": sorted(
                stress_rows,
                key=lambda row: (float(row.get("net_total_return", 0.0)), str(row.get("scenario_type", "")), str(row.get("scenario_bucket", ""))),
            )[:5],
        }

    def _load_backtest_summary(self) -> dict[str, object]:
        path = self.output_dir / "summary.json"
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
        return {}

    def _build_factor_diagnostics(self) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        rebalance_dates = sorted(self.prepared_data.features_by_rebalance)
        if not rebalance_dates:
            return [], []
        observations: list[dict[str, object]] = []
        for index, rebalance_date in enumerate(rebalance_dates):
            rows = self.prepared_data.features_by_rebalance[rebalance_date]
            next_rebalance_date = rebalance_dates[index + 1] if index + 1 < len(rebalance_dates) else None
            forward_returns = self._forward_returns(rebalance_date, next_rebalance_date)
            if not forward_returns:
                continue
            regime_vix_bucket, regime_macro_bucket = self._regime_buckets(rows)
            for factor_name, direction in DIAGNOSTIC_FACTORS.items():
                factor_pairs = [
                    (
                        direction * float(row[factor_name]),
                        forward_returns[row["permno"]],
                    )
                    for row in rows
                    if factor_name in row and row["permno"] in forward_returns
                ]
                if len(factor_pairs) < 2:
                    continue
                scores = [score for score, _ in factor_pairs]
                realized = [forward_return for _, forward_return in factor_pairs]
                top_returns, bottom_returns = self._top_bottom_bucket_returns(scores, realized)
                quintile_spread = (sum(top_returns) / len(top_returns)) - (sum(bottom_returns) / len(bottom_returns))
                observations.append(
                    {
                        "rebalance_date": rebalance_date.isoformat(),
                        "factor": factor_name,
                        "ic": self._pearson(scores, realized),
                        "quintile_spread": quintile_spread,
                        "spread_hit": 1.0 if quintile_spread > 0.0 else 0.0,
                        "top_bucket_return": sum(top_returns) / len(top_returns),
                        "bottom_bucket_return": sum(bottom_returns) / len(bottom_returns),
                        "observation_count": float(len(factor_pairs)),
                        "regime_vix_bucket": regime_vix_bucket,
                        "regime_macro_bucket": regime_macro_bucket,
                    }
                )
        return self._aggregate_factor_diagnostics(observations)

    def _forward_returns(self, rebalance_date: date, next_rebalance_date: date | None) -> dict[str, float]:
        period_dates = [
            day
            for day in sorted(self.prepared_data.returns_by_date)
            if day >= rebalance_date and (next_rebalance_date is None or day < next_rebalance_date)
        ]
        if not period_dates:
            return {}
        returns: dict[str, float] = {}
        for day in period_dates:
            for permno, daily_return in self.prepared_data.returns_by_date[day].items():
                returns[permno] = (1.0 + returns.get(permno, 0.0)) * (1.0 + daily_return) - 1.0
        return returns

    def _regime_buckets(self, rows: list[dict]) -> tuple[str, str]:
        if not rows:
            return "unknown", "unknown"
        average_vix = sum(float(row.get("vix", 0.0)) for row in rows) / len(rows)
        average_macro_score = sum(float(row.get("macro_score", 0.0)) for row in rows) / len(rows)
        vix_threshold = float(self.config.strategy.get("regime_vix_threshold", self.config.strategy.get("vix_de_risk_level", 30.0)))
        macro_threshold = float(self.config.strategy.get("regime_macro_threshold", 0.5))
        regime_vix_bucket = "high_vix" if average_vix >= vix_threshold else "low_vix"
        regime_macro_bucket = "weak_macro" if average_macro_score < macro_threshold else "strong_macro"
        return regime_vix_bucket, regime_macro_bucket

    def _top_bottom_bucket_returns(self, scores: list[float], realized: list[float]) -> tuple[list[float], list[float]]:
        ordered = sorted(zip(scores, realized, strict=True), key=lambda item: item[0])
        bucket_size = max(len(ordered) // 5, 1)
        bottom = [forward_return for _, forward_return in ordered[:bucket_size]]
        top = [forward_return for _, forward_return in ordered[-bucket_size:]]
        return top, bottom

    def _aggregate_factor_diagnostics(
        self,
        observations: list[dict[str, object]],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        regime_grouped: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
        for observation in observations:
            factor = str(observation["factor"])
            grouped[factor].append(observation)
            regime_grouped[
                (
                    factor,
                    str(observation["regime_vix_bucket"]),
                    str(observation["regime_macro_bucket"]),
                )
            ].append(observation)

        factor_rows = []
        for factor, rows in grouped.items():
            factor_rows.append(
                {
                    "factor": factor,
                    "windows": len(rows),
                    "average_ic": self._average(rows, "ic"),
                    "positive_ic_rate": self._positive_rate(rows, "ic"),
                    "average_quintile_spread": self._average(rows, "quintile_spread"),
                    "spread_hit_rate": self._average(rows, "spread_hit"),
                    "average_top_bucket_return": self._average(rows, "top_bucket_return"),
                    "average_bottom_bucket_return": self._average(rows, "bottom_bucket_return"),
                    "average_cross_section_size": self._average(rows, "observation_count"),
                }
            )
        factor_rows = sorted(
            factor_rows,
            key=lambda row: (
                float(row["average_ic"]),
                float(row["average_quintile_spread"]),
                float(row["spread_hit_rate"]),
            ),
            reverse=True,
        )

        factor_regime_rows = []
        for (factor, regime_vix_bucket, regime_macro_bucket), rows in sorted(regime_grouped.items()):
            factor_regime_rows.append(
                {
                    "factor": factor,
                    "regime_vix_bucket": regime_vix_bucket,
                    "regime_macro_bucket": regime_macro_bucket,
                    "windows": len(rows),
                    "average_ic": self._average(rows, "ic"),
                    "average_quintile_spread": self._average(rows, "quintile_spread"),
                    "spread_hit_rate": self._average(rows, "spread_hit"),
                }
            )
        factor_regime_rows = sorted(
            factor_regime_rows,
            key=lambda row: (
                str(row["factor"]),
                str(row["regime_vix_bucket"]),
                str(row["regime_macro_bucket"]),
            ),
        )
        return factor_rows, factor_regime_rows

    def _build_capacity_diagnostics(
        self,
        rebalance_rows: dict[date, list[dict[str, object]]],
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        ordered_rebalances = sorted(rebalance_rows)
        if not ordered_rebalances:
            return [], []

        backtest_summary = self._load_backtest_summary()
        benchmark_total_return = float(backtest_summary.get("benchmark_total_return", 0.0))
        gross_total_return = float(backtest_summary.get("gross_total_return", 0.0))
        total_short_borrow_cost = float(backtest_summary.get("total_short_borrow_cost", 0.0))
        baseline_aum = max(float(self.config.strategy.get("capacity_baseline_aum", self.config.strategy.get("slippage_notional", 1_000_000.0))), 1.0)
        aum_levels = self._capacity_aum_levels(baseline_aum)
        slippage_model = str(self.config.strategy.get("slippage_model", "fixed")).strip().lower() or "fixed"
        commission_bps = max(float(self.config.strategy.get("commission_cost_bps", 0.0)), 0.0)
        slippage_cost_bps = max(
            float(
                self.config.strategy.get(
                    "slippage_cost_bps",
                    max(float(self.config.strategy.get("transaction_cost_bps", 10.0)) - commission_bps, 0.0),
                )
            ),
            0.0,
        )
        slippage_impact_bps_per_adv = max(float(self.config.strategy.get("slippage_impact_bps_per_adv", 50.0)), 0.0)
        slippage_adv_floor = max(float(self.config.strategy.get("slippage_adv_floor", 100_000.0)), 1.0)
        max_trade_participation_ratio = max(float(self.config.strategy.get("max_trade_participation_ratio", 0.0)), 0.0)

        previous_weights: dict[str, float] = {}
        rebalance_trade_events: list[dict[str, object]] = []
        for rebalance_date in ordered_rebalances:
            current_weights = {
                str(row["permno"]): float(row["weight"])
                for row in rebalance_rows[rebalance_date]
            }
            feature_lookup = {
                str(row["permno"]): row
                for row in self.prepared_data.features_by_rebalance.get(rebalance_date, [])
            }
            trades = []
            for permno in sorted(set(previous_weights) | set(current_weights)):
                if permno == "__BENCH__":
                    continue
                trade_weight = abs(current_weights.get(permno, 0.0) - previous_weights.get(permno, 0.0))
                if trade_weight <= 1e-12:
                    continue
                trades.append(
                    {
                        "permno": permno,
                        "trade_weight": trade_weight,
                        "avg_dollar_volume": max(float(feature_lookup.get(permno, {}).get("avg_dollar_volume", 0.0)), slippage_adv_floor),
                    }
                )
            if trades:
                rebalance_trade_events.append(
                    {
                        "rebalance_date": rebalance_date.isoformat(),
                        "trades": trades,
                    }
                )
            previous_weights = current_weights

        if not rebalance_trade_events:
            return [], []

        curve_rows = []
        breach_rows = []
        for aum in aum_levels:
            total_trade_count = 0
            breached_trade_count = 0
            breached_rebalance_count = 0
            total_commission_cost = 0.0
            total_slippage_cost = 0.0
            total_trade_weight = 0.0
            weighted_participation = 0.0
            max_participation_ratio = 0.0
            for event in rebalance_trade_events:
                trades = list(event["trades"])
                event_trade_count = len(trades)
                event_breached_trade_count = 0
                event_trade_weight = 0.0
                event_weighted_participation = 0.0
                event_max_participation = 0.0
                event_commission_cost = 0.0
                event_slippage_cost = 0.0
                for trade in trades:
                    trade_weight = float(trade["trade_weight"])
                    avg_dollar_volume = max(float(trade["avg_dollar_volume"]), 1.0)
                    participation_ratio = (trade_weight * aum) / avg_dollar_volume
                    slippage_bps = (
                        slippage_cost_bps + self._impact_bps(participation_ratio, slippage_model, slippage_impact_bps_per_adv)
                        if slippage_model != "fixed"
                        else slippage_cost_bps
                    )
                    event_trade_weight += trade_weight
                    event_weighted_participation += trade_weight * participation_ratio
                    event_max_participation = max(event_max_participation, participation_ratio)
                    event_commission_cost += 0.5 * trade_weight * (commission_bps / 10000.0)
                    event_slippage_cost += 0.5 * trade_weight * (slippage_bps / 10000.0)
                    if max_trade_participation_ratio > 0.0 and participation_ratio > max_trade_participation_ratio:
                        event_breached_trade_count += 1

                total_trade_count += event_trade_count
                breached_trade_count += event_breached_trade_count
                total_trade_weight += event_trade_weight
                weighted_participation += event_weighted_participation
                max_participation_ratio = max(max_participation_ratio, event_max_participation)
                total_commission_cost += event_commission_cost
                total_slippage_cost += event_slippage_cost
                if event_breached_trade_count > 0:
                    breached_rebalance_count += 1
                breach_rows.append(
                    {
                        "aum": aum,
                        "aum_multiple": aum / baseline_aum,
                        "rebalance_date": str(event["rebalance_date"]),
                        "trade_count": event_trade_count,
                        "breached_trade_count": event_breached_trade_count,
                        "breach_rate": (event_breached_trade_count / event_trade_count) if event_trade_count else 0.0,
                        "average_participation_ratio": (event_weighted_participation / event_trade_weight) if event_trade_weight > 0.0 else 0.0,
                        "max_participation_ratio": event_max_participation,
                        "estimated_commission_cost": event_commission_cost,
                        "estimated_slippage_cost": event_slippage_cost,
                        "estimated_transaction_cost": event_commission_cost + event_slippage_cost,
                    }
                )

            total_transaction_cost = total_commission_cost + total_slippage_cost
            estimated_net_total_return = gross_total_return - total_transaction_cost - total_short_borrow_cost
            curve_rows.append(
                {
                    "aum": aum,
                    "aum_multiple": aum / baseline_aum,
                    "trade_count": total_trade_count,
                    "breached_trade_count": breached_trade_count,
                    "participation_breach_rate": (breached_trade_count / total_trade_count) if total_trade_count else 0.0,
                    "breached_rebalance_count": breached_rebalance_count,
                    "rebalance_breach_rate": (breached_rebalance_count / len(rebalance_trade_events)) if rebalance_trade_events else 0.0,
                    "average_participation_ratio": (weighted_participation / total_trade_weight) if total_trade_weight > 0.0 else 0.0,
                    "max_participation_ratio": max_participation_ratio,
                    "estimated_commission_cost": total_commission_cost,
                    "estimated_slippage_cost": total_slippage_cost,
                    "estimated_transaction_cost": total_transaction_cost,
                    "estimated_net_total_return": estimated_net_total_return,
                    "estimated_active_total_return": estimated_net_total_return - benchmark_total_return,
                    "capacity_status": "breached" if breached_trade_count > 0 else "within_limits",
                }
            )
        return curve_rows, breach_rows

    def _build_stress_scenarios(self, daily_rows: list[dict[str, object]]) -> list[dict[str, object]]:
        contextual_rows = self._contextual_daily_rows(daily_rows)
        if not contextual_rows:
            return []
        turnover_threshold = max(
            float(self.config.strategy.get("stress_turnover_threshold", 0.0)),
            max(float(self.config.strategy.get("max_turnover_per_rebalance", 0.0)) * 0.5, 0.10),
        )
        cash_weight_threshold = max(float(self.config.strategy.get("stress_cash_weight_threshold", 0.10)), 0.0)
        grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
        for row in contextual_rows:
            grouped[("vix_regime", str(row["regime_vix_bucket"]))].append(row)
            grouped[("macro_regime", str(row["regime_macro_bucket"]))].append(row)
            grouped[(
                "turnover_regime",
                "high_turnover" if float(row["turnover"]) >= turnover_threshold else "low_turnover",
            )].append(row)
            grouped[(
                "cash_regime",
                "elevated_cash" if float(row["cash_weight"]) >= cash_weight_threshold else "fully_invested",
            )].append(row)

        rows = []
        for (scenario_type, scenario_bucket), scenario_rows in sorted(grouped.items()):
            gross_equity = 1.0
            net_equity = 1.0
            benchmark_equity = 1.0
            for row in scenario_rows:
                gross_equity *= 1.0 + float(row["gross_return"])
                net_equity *= 1.0 + float(row["net_return"])
                benchmark_equity *= 1.0 + float(row["benchmark_return"])
            rows.append(
                {
                    "scenario_type": scenario_type,
                    "scenario_bucket": scenario_bucket,
                    "days": len(scenario_rows),
                    "gross_total_return": gross_equity - 1.0,
                    "net_total_return": net_equity - 1.0,
                    "benchmark_total_return": benchmark_equity - 1.0,
                    "active_total_return": (net_equity - 1.0) - (benchmark_equity - 1.0),
                    "average_turnover": self._average(scenario_rows, "turnover"),
                    "average_cash_weight": self._average(scenario_rows, "cash_weight"),
                    "average_exposure": self._average(scenario_rows, "exposure"),
                    "average_transaction_cost": self._average(scenario_rows, "transaction_cost"),
                    "average_vix": self._average(scenario_rows, "average_vix"),
                    "average_macro_score": self._average(scenario_rows, "average_macro_score"),
                }
            )
        return rows

    def _contextual_daily_rows(self, daily_rows: list[dict[str, object]]) -> list[dict[str, object]]:
        rebalance_dates = sorted(self.prepared_data.features_by_rebalance)
        if not rebalance_dates:
            return []
        rebalance_context = {}
        for rebalance_date in rebalance_dates:
            rows = self.prepared_data.features_by_rebalance[rebalance_date]
            if not rows:
                rebalance_context[rebalance_date] = {
                    "average_vix": 0.0,
                    "average_macro_score": 0.0,
                    "regime_vix_bucket": "unknown",
                    "regime_macro_bucket": "unknown",
                }
                continue
            regime_vix_bucket, regime_macro_bucket = self._regime_buckets(rows)
            rebalance_context[rebalance_date] = {
                "average_vix": sum(float(row.get("vix", 0.0)) for row in rows) / len(rows),
                "average_macro_score": sum(float(row.get("macro_score", 0.0)) for row in rows) / len(rows),
                "regime_vix_bucket": regime_vix_bucket,
                "regime_macro_bucket": regime_macro_bucket,
            }

        contextual_rows = []
        rebalance_index = 0
        current_context = {
            "average_vix": 0.0,
            "average_macro_score": 0.0,
            "regime_vix_bucket": "unknown",
            "regime_macro_bucket": "unknown",
        }
        for daily_row in daily_rows:
            current_date = daily_row["date"]
            while rebalance_index < len(rebalance_dates) and rebalance_dates[rebalance_index] <= current_date:
                current_context = dict(rebalance_context[rebalance_dates[rebalance_index]])
                rebalance_index += 1
            contextual_rows.append(
                {
                    **daily_row,
                    **current_context,
                }
            )
        return contextual_rows

    def _capacity_aum_levels(self, baseline_aum: float) -> list[float]:
        configured = self.config.strategy.get("capacity_aum_levels", [])
        if isinstance(configured, list):
            levels = sorted({max(float(value), 1.0) for value in configured if float(value) > 0.0})
            if levels:
                return levels
        return [baseline_aum * multiple for multiple in (0.25, 0.5, 1.0, 2.0, 5.0)]

    def _slippage_impact_exponent(self) -> float:
        configured = self.config.strategy.get("slippage_impact_exponent")
        slippage_model = str(self.config.strategy.get("slippage_model", "fixed")).strip().lower() or "fixed"
        if configured is None and slippage_model in {"square_root", "sqrt_liquidity_aware"}:
            configured = 0.5
        if configured is None:
            configured = 1.0
        return min(max(float(configured), 0.0), 2.0)

    def _impact_bps(self, participation_ratio: float, slippage_model: str, impact_bps_per_adv: float) -> float:
        if participation_ratio <= 0.0 or slippage_model == "fixed":
            return 0.0
        return impact_bps_per_adv * (participation_ratio ** self._slippage_impact_exponent())

    def _average(self, rows: list[dict[str, object]], key: str) -> float:
        if not rows:
            return 0.0
        return sum(float(row.get(key, 0.0)) for row in rows) / len(rows)

    def _positive_rate(self, rows: list[dict[str, object]], key: str) -> float:
        if not rows:
            return 0.0
        return sum(1.0 for row in rows if float(row.get(key, 0.0)) > 0.0) / len(rows)

    def _pearson(self, left: list[float], right: list[float]) -> float:
        if len(left) != len(right) or len(left) < 2:
            return 0.0
        left_mean = sum(left) / len(left)
        right_mean = sum(right) / len(right)
        covariance = sum((l - left_mean) * (r - right_mean) for l, r in zip(left, right, strict=True))
        left_variance = sum((l - left_mean) ** 2 for l in left)
        right_variance = sum((r - right_mean) ** 2 for r in right)
        if left_variance <= 0.0 or right_variance <= 0.0:
            return 0.0
        return covariance / ((left_variance ** 0.5) * (right_variance ** 0.5))

    def _build_html_report(
        self,
        summary: dict[str, object],
        monthly_rows: list[dict[str, object]],
        sector_rows: list[dict[str, object]],
        security_rows: list[dict[str, object]],
        factor_rows: list[dict[str, object]],
        capacity_rows: list[dict[str, object]],
        stress_rows: list[dict[str, object]],
    ) -> str:
        backtest_summary = summary.get("backtest_summary", {})
        cards = [
            ("Net Total Return", self._format_pct(float(backtest_summary.get("net_total_return", 0.0)))),
            ("Sharpe", self._format_number(float(backtest_summary.get("sharpe", 0.0)))),
            ("Max Drawdown", self._format_pct(float(backtest_summary.get("max_drawdown", 0.0)))),
            ("Positive Months", self._format_pct(float(summary.get("monthly_positive_rate", 0.0)))),
            ("Capacity Without Breach", self._format_currency(summary.get("largest_aum_without_breach"))),
            ("Execution Cost", self._format_pct(float(backtest_summary.get("total_transaction_cost", 0.0)))),
        ]
        hero_cards = "".join(
            f"""
            <article class="metric-card">
              <div class="metric-label">{html.escape(label)}</div>
              <div class="metric-value">{html.escape(value)}</div>
            </article>
            """
            for label, value in cards
        )
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Quant Research Dashboard</title>
  <style>
    :root {{
      --bg: #f4efe6;
      --panel: rgba(255, 252, 246, 0.9);
      --panel-strong: #fffaf0;
      --ink: #1f1b16;
      --muted: #695f55;
      --line: rgba(74, 60, 45, 0.12);
      --accent: #b84d1b;
      --accent-soft: #f0b48f;
      --cool: #214f6b;
      --danger: #8d2f2f;
      --shadow: 0 18px 50px rgba(72, 46, 19, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(184, 77, 27, 0.16), transparent 32%),
        radial-gradient(circle at top right, rgba(33, 79, 107, 0.14), transparent 28%),
        linear-gradient(180deg, #f7f1e8 0%, var(--bg) 100%);
    }}
    .shell {{
      width: min(1180px, calc(100% - 32px));
      margin: 0 auto;
      padding: 32px 0 48px;
    }}
    .hero {{
      padding: 28px;
      border: 1px solid var(--line);
      border-radius: 28px;
      background: linear-gradient(145deg, rgba(255,255,255,0.85), rgba(247, 239, 228, 0.92));
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }}
    .hero::after {{
      content: "";
      position: absolute;
      inset: auto -80px -120px auto;
      width: 260px;
      height: 260px;
      background: radial-gradient(circle, rgba(184, 77, 27, 0.18), transparent 70%);
      pointer-events: none;
    }}
    .eyebrow {{
      color: var(--accent);
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(32px, 5vw, 56px);
      line-height: 0.95;
      max-width: 720px;
    }}
    .lede {{
      margin: 14px 0 0;
      max-width: 760px;
      color: var(--muted);
      font-size: 17px;
      line-height: 1.6;
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      margin-top: 24px;
    }}
    .metric-card {{
      background: rgba(255,255,255,0.68);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px 16px;
      min-height: 104px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}
    .metric-label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .metric-value {{
      font-size: 28px;
      line-height: 1;
    }}
    .section {{
      margin-top: 22px;
      padding: 22px;
      border: 1px solid var(--line);
      border-radius: 24px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }}
    .section-head {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 16px;
    }}
    h2 {{
      margin: 0;
      font-size: 24px;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 14px;
    }}
    .split {{
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 16px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
      overflow: hidden;
      border-radius: 16px;
      background: var(--panel-strong);
    }}
    th, td {{
      text-align: left;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    tr:last-child td {{ border-bottom: none; }}
    .capacity-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }}
    .capacity-row {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: linear-gradient(180deg, rgba(255,255,255,0.7), rgba(248, 242, 232, 0.92));
    }}
    .capacity-top {{
      display: flex;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 10px;
      font-size: 15px;
    }}
    .bar {{
      height: 10px;
      border-radius: 999px;
      background: rgba(33, 79, 107, 0.1);
      overflow: hidden;
    }}
    .bar > span {{
      display: block;
      height: 100%;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--cool), var(--accent));
    }}
    .tag {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      background: rgba(184, 77, 27, 0.1);
      color: var(--accent);
    }}
    .tag.bad {{
      background: rgba(141, 47, 47, 0.12);
      color: var(--danger);
    }}
    @media (max-width: 980px) {{
      .metric-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
      .split {{ grid-template-columns: 1fr; }}
      .capacity-grid {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 640px) {{
      .shell {{ width: min(100% - 20px, 1180px); padding-top: 20px; }}
      .hero, .section {{ padding: 18px; border-radius: 20px; }}
      .metric-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      h2 {{ font-size: 20px; }}
      .metric-value {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="eyebrow">Quant Research Stack</div>
      <h1>Report Dashboard</h1>
      <p class="lede">Backtest, capacity, factor edge, and stress outcomes are condensed into a single static report so the project can be judged in one pass instead of by opening five CSV files.</p>
      <div class="metric-grid">{hero_cards}</div>
    </section>
    <section class="section">
      <div class="section-head">
        <div>
          <h2>Monthly Path</h2>
          <div class="subtle">Monthly gross, net, active return and cost drag.</div>
        </div>
      </div>
      {self._render_table(monthly_rows[:12], ["month", "gross_total_return", "net_total_return", "active_total_return", "total_transaction_cost", "average_turnover"])}
    </section>
    <section class="section">
      <div class="section-head">
        <div>
          <h2>Capacity Curve</h2>
          <div class="subtle">Participation pressure and breach state by AUM.</div>
        </div>
      </div>
      <div class="capacity-grid">{self._render_capacity_cards(capacity_rows[:6])}</div>
    </section>
    <section class="section split">
      <div>
        <div class="section-head">
          <div>
            <h2>Top Securities</h2>
            <div class="subtle">Names with the largest total contribution.</div>
          </div>
        </div>
        {self._render_table(security_rows[:8], ["permno", "sector", "total_contribution", "average_weight", "days_held"])}
      </div>
      <div>
        <div class="section-head">
          <div>
            <h2>Top Sectors</h2>
            <div class="subtle">Sector contribution and average deployed weight.</div>
          </div>
        </div>
        {self._render_table(sector_rows[:8], ["sector", "total_contribution", "average_weight", "days_held"])}
      </div>
    </section>
    <section class="section split">
      <div>
        <div class="section-head">
          <div>
            <h2>Factor Edge</h2>
            <div class="subtle">Highest ranked factors by IC and quintile spread.</div>
          </div>
        </div>
        {self._render_table(factor_rows[:8], ["factor", "average_ic", "positive_ic_rate", "average_quintile_spread", "spread_hit_rate"])}
      </div>
      <div>
        <div class="section-head">
          <div>
            <h2>Stress Map</h2>
            <div class="subtle">Worst scenario buckets from the current backtest.</div>
          </div>
        </div>
        {self._render_table(
            sorted(stress_rows, key=lambda row: float(row.get("net_total_return", 0.0)))[:8],
            ["scenario_type", "scenario_bucket", "net_total_return", "active_total_return", "average_turnover", "average_cash_weight"],
        )}
      </div>
    </section>
  </main>
</body>
</html>
"""

    def _render_table(self, rows: list[dict[str, object]], columns: list[str]) -> str:
        if not rows:
            return "<div class=\"subtle\">No rows available.</div>"
        header = "".join(f"<th>{html.escape(self._labelize(column))}</th>" for column in columns)
        body = "".join(
            "<tr>"
            + "".join(f"<td>{html.escape(self._format_cell(row.get(column)))}</td>" for column in columns)
            + "</tr>"
            for row in rows
        )
        return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"

    def _render_capacity_cards(self, rows: list[dict[str, object]]) -> str:
        if not rows:
            return "<div class=\"subtle\">No capacity diagnostics available.</div>"
        cards = []
        for row in rows:
            participation_ratio = min(max(float(row.get("max_participation_ratio", 0.0)), 0.0), 1.0)
            status = str(row.get("capacity_status", "unknown"))
            cards.append(
                f"""
                <article class="capacity-row">
                  <div class="capacity-top">
                    <strong>{html.escape(self._format_currency(row.get("aum")))}</strong>
                    <span class="tag{' bad' if status == 'breached' else ''}">{html.escape(status)}</span>
                  </div>
                  <div class="subtle">Max participation {html.escape(self._format_pct(participation_ratio))}</div>
                  <div class="bar"><span style="width:{participation_ratio * 100:.2f}%"></span></div>
                  <div class="subtle" style="margin-top:10px;">Estimated net return {html.escape(self._format_pct(float(row.get("estimated_net_total_return", 0.0))))}</div>
                </article>
                """
            )
        return "".join(cards)

    def _format_cell(self, value: object) -> str:
        if isinstance(value, float):
            magnitude = abs(value)
            if magnitude >= 100000:
                return self._format_currency(value)
            if 0.0 < magnitude <= 1.0:
                return self._format_pct(value)
            return self._format_number(value)
        if isinstance(value, int):
            return f"{value:,}"
        if value is None:
            return "-"
        return str(value)

    def _format_pct(self, value: object) -> str:
        numeric = float(value or 0.0)
        return f"{numeric * 100.0:.2f}%"

    def _format_currency(self, value: object) -> str:
        if value is None:
            return "-"
        return f"${float(value):,.0f}"

    def _format_number(self, value: object) -> str:
        return f"{float(value or 0.0):.2f}"

    def _labelize(self, key: str) -> str:
        return key.replace("_", " ").strip()
