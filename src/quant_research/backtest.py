from __future__ import annotations

import csv
import json
import math
from pathlib import Path

from .data_models import PortfolioDay
from .pipeline import PreparedData
from .strategy import MultiSignalStrategy
from .utils import ensure_directory


class Backtester:
    def __init__(
        self,
        prepared_data: PreparedData,
        strategy: MultiSignalStrategy,
        output_dir: Path,
        transaction_cost_bps: float = 10.0,
        commission_cost_bps: float | None = None,
        slippage_cost_bps: float | None = None,
    ) -> None:
        self.prepared_data = prepared_data
        self.strategy = strategy
        self.output_dir = output_dir
        self.transaction_cost_bps = transaction_cost_bps
        self.slippage_model = str(strategy.config.get("slippage_model", "fixed")).strip().lower() or "fixed"
        self.slippage_notional = max(float(strategy.config.get("slippage_notional", 1_000_000.0)), 0.0)
        self.slippage_adv_floor = max(float(strategy.config.get("slippage_adv_floor", 100_000.0)), 1.0)
        self.slippage_impact_bps_per_adv = max(float(strategy.config.get("slippage_impact_bps_per_adv", 50.0)), 0.0)
        configured_impact_exponent = strategy.config.get("slippage_impact_exponent")
        if configured_impact_exponent is None and self.slippage_model in {"square_root", "sqrt_liquidity_aware"}:
            configured_impact_exponent = 0.5
        if configured_impact_exponent is None:
            configured_impact_exponent = 1.0
        self.slippage_impact_exponent = min(max(float(configured_impact_exponent), 0.0), 2.0)
        self.max_trade_participation_ratio = max(float(strategy.config.get("max_trade_participation_ratio", 0.0)), 0.0)
        self.execution_backlog_carry_forward_enabled = bool(strategy.config.get("execution_backlog_carry_forward_enabled", False))
        self.execution_backlog_decay = min(max(float(strategy.config.get("execution_backlog_decay", 1.0)), 0.0), 1.0)
        self.execution_priority_backlog_buy = max(float(strategy.config.get("execution_priority_backlog_buy", 1.0)), 0.0)
        self.execution_priority_existing_buy = max(float(strategy.config.get("execution_priority_existing_buy", 1.0)), 0.0)
        self.execution_priority_new_buy = max(float(strategy.config.get("execution_priority_new_buy", 1.0)), 0.0)
        self.execution_priority_backlog_sell = max(float(strategy.config.get("execution_priority_backlog_sell", 1.0)), 0.0)
        self.execution_priority_existing_sell = max(float(strategy.config.get("execution_priority_existing_sell", 1.0)), 0.0)
        self.execution_priority_new_sell = max(float(strategy.config.get("execution_priority_new_sell", 1.0)), 0.0)
        self.execution_backlog_age_half_life_rebalances = max(float(strategy.config.get("execution_backlog_age_half_life_rebalances", 0.0)), 0.0)
        self.execution_days_per_rebalance = max(int(strategy.config.get("execution_days_per_rebalance", 1)), 1)
        self.cash_carry_enabled = bool(strategy.config.get("cash_carry_enabled", True))
        self.short_borrow_cost_bps_annual = max(float(strategy.config.get("short_borrow_cost_bps_annual", 0.0)), 0.0)
        self.short_borrow_cost_field = str(strategy.config.get("short_borrow_cost_field", "short_borrow_cost_bps_annual")).strip() or "short_borrow_cost_bps_annual"
        if commission_cost_bps is None and slippage_cost_bps is None:
            self.commission_cost_bps = 0.0
            self.slippage_cost_bps = transaction_cost_bps
        else:
            self.commission_cost_bps = max(float(commission_cost_bps or 0.0), 0.0)
            remaining_slippage = transaction_cost_bps - self.commission_cost_bps if slippage_cost_bps is None else slippage_cost_bps
            self.slippage_cost_bps = max(float(remaining_slippage), 0.0)
        self.total_cost_bps = self.commission_cost_bps + self.slippage_cost_bps

    def run(self) -> dict[str, float]:
        ensure_directory(self.output_dir)
        rebalances = sorted(self.prepared_data.features_by_rebalance.keys())
        daily_dates = sorted(self.prepared_data.returns_by_date.keys())
        risk_free_by_date = getattr(self.prepared_data, "risk_free_by_date", {})
        implemented_portfolios = {}
        execution_records: list[dict[str, float | str]] = []

        rows: list[PortfolioDay] = []
        previous_weights: dict[str, float] = {}
        current_target_portfolio = None
        current_rebalance_rows: list[dict] = []
        execution_days_remaining = 0
        previous_target_weights: dict[str, float] = {}
        previous_backlog_ages: dict[str, float] = {}
        realized_rebalance_turnovers: list[float] = []
        rebalance_index = 0
        for current_date in daily_dates:
            while rebalance_index < len(rebalances) and rebalances[rebalance_index] <= current_date:
                rebalance_date = rebalances[rebalance_index]
                had_previous_weights = bool(previous_weights)
                rebalance_rows = self.prepared_data.features_by_rebalance[rebalance_date]
                incoming_backlog_ages = dict(previous_backlog_ages)
                target_portfolio = self.strategy.build_weights(
                    rebalance_date,
                    rebalance_rows,
                    previous_weights=previous_weights,
                )
                current_portfolio, previous_target_weights, previous_backlog_ages, execution_components = self._implement_portfolio(
                    target_portfolio,
                    previous_weights,
                    previous_target_weights,
                    previous_backlog_ages,
                    realized_rebalance_turnovers,
                    rebalance_rows,
                )
                execution_records.extend(
                    self._execution_diagnostic_records(
                        rebalance_date,
                        previous_weights,
                        current_portfolio.target_weights or {},
                        current_portfolio.weights,
                        incoming_backlog_ages,
                        execution_components,
                        rebalance_rows,
                    )
                )
                implemented_portfolios[current_portfolio.rebalance_date] = current_portfolio
                planned_turnover = self._compute_turnover(previous_weights, current_portfolio.weights)
                current_target_portfolio = current_portfolio
                current_rebalance_rows = rebalance_rows
                execution_days_remaining = self.execution_days_per_rebalance
                if had_previous_weights:
                    realized_rebalance_turnovers.append(planned_turnover)
                rebalance_index += 1
            if current_target_portfolio is None or (not current_target_portfolio.weights and not previous_weights):
                continue
            turnover = 0.0
            commission_cost = 0.0
            slippage_cost = 0.0
            if execution_days_remaining > 0:
                next_weights = self._execution_step_weights(
                    previous_weights,
                    current_target_portfolio.weights,
                    execution_days_remaining,
                )
                turnover = self._compute_turnover(previous_weights, next_weights)
                commission_cost = turnover * (self.commission_cost_bps / 10000.0)
                slippage_cost = float(
                    self._slippage_metadata(previous_weights, next_weights, current_rebalance_rows).get(
                        "slippage_cost",
                        turnover * (self.slippage_cost_bps / 10000.0),
                    )
                )
                previous_weights = dict(next_weights)
                execution_days_remaining -= 1
            security_returns = self.prepared_data.returns_by_date[current_date]
            benchmark_return = self.prepared_data.benchmark_by_date.get(current_date, 0.0)
            invested_return = sum(
                weight * (benchmark_return if permno == "__BENCH__" else security_returns.get(permno, 0.0))
                for permno, weight in previous_weights.items()
            )
            cash_rate = risk_free_by_date.get(current_date, 0.0) if self.cash_carry_enabled else 0.0
            cash_weight, cash_carry, cash_drag = self._cash_metrics(
                current_target_portfolio.target_weights or current_target_portfolio.weights,
                previous_weights,
                security_returns,
                benchmark_return,
                cash_rate,
            )
            gross_return = invested_return + cash_carry
            transaction_cost = commission_cost + slippage_cost
            short_borrow_cost = self._short_borrow_cost(previous_weights, current_rebalance_rows)
            net_return = gross_return - transaction_cost - short_borrow_cost
            rows.append(
                PortfolioDay(
                    date=current_date,
                    gross_return=gross_return,
                    net_return=net_return,
                    benchmark_return=benchmark_return,
                    active_return=net_return - benchmark_return,
                    exposure=current_target_portfolio.exposure,
                    cash_weight=cash_weight,
                    cash_carry=cash_carry,
                    holdings=sum(1 for permno in previous_weights if permno != "__BENCH__"),
                    turnover=turnover,
                    cash_drag=cash_drag,
                    commission_cost=commission_cost,
                    slippage_cost=slippage_cost,
                    transaction_cost=transaction_cost,
                    short_borrow_cost=short_borrow_cost,
                )
            )
        self._write_rebalance_weights(implemented_portfolios)
        self._write_daily_returns(rows)
        self._write_execution_diagnostics(execution_records)
        self._write_execution_diagnostics_by_bucket(execution_records)
        self._write_execution_diagnostics_by_bucket_timeseries(execution_records)
        self._write_execution_backlog_aging(execution_records)
        self._write_execution_backlog_dropoff(execution_records)
        metadata_by_rebalance = {
            rebalance_date.isoformat(): dict(portfolio.metadata or {})
            for rebalance_date, portfolio in implemented_portfolios.items()
        }
        self._write_execution_backlog_dropoff_timeseries(execution_records, metadata_by_rebalance)
        self._write_execution_backlog_dropoff_by_regime(execution_records, metadata_by_rebalance)
        return self._summarize(rows)

    def _write_daily_returns(self, rows: list[PortfolioDay]) -> None:
        path = self.output_dir / "portfolio_daily_returns.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "date",
                    "gross_return",
                    "net_return",
                    "benchmark_return",
                    "active_return",
                    "exposure",
                    "cash_weight",
                    "cash_carry",
                    "holdings",
                    "turnover",
                    "cash_drag",
                    "commission_cost",
                    "slippage_cost",
                    "transaction_cost",
                    "short_borrow_cost",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(
                    {
                        "date": row.date.isoformat(),
                        "gross_return": f"{row.gross_return:.8f}",
                        "net_return": f"{row.net_return:.8f}",
                        "benchmark_return": f"{row.benchmark_return:.8f}",
                        "active_return": f"{row.active_return:.8f}",
                        "exposure": f"{row.exposure:.4f}",
                        "cash_weight": f"{row.cash_weight:.8f}",
                        "cash_carry": f"{row.cash_carry:.8f}",
                        "holdings": row.holdings,
                        "turnover": f"{row.turnover:.8f}",
                        "cash_drag": f"{row.cash_drag:.8f}",
                        "commission_cost": f"{row.commission_cost:.8f}",
                        "slippage_cost": f"{row.slippage_cost:.8f}",
                        "transaction_cost": f"{row.transaction_cost:.8f}",
                        "short_borrow_cost": f"{row.short_borrow_cost:.8f}",
                    }
                )

    def _write_rebalance_weights(self, portfolios: dict) -> None:
        path = self.output_dir / "portfolio_rebalances.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "rebalance_date",
                    "permno",
                    "weight",
                    "exposure",
                    "is_benchmark_hedge",
                    "beta_exposure",
                    "downside_beta_exposure",
                    "idio_vol_exposure",
                    "size_exposure",
                    "net_weight",
                    "gross_weight",
                    "effective_turnover_budget",
                    "budget_floor",
                    "budget_ceiling",
                    "budget_score_dispersion",
                    "budget_recent_turnover",
                    "budget_score_multiplier",
                    "budget_turnover_multiplier",
                    "average_slippage_bps",
                    "target_max_participation_ratio",
                    "max_participation_ratio",
                    "liquidity_scale",
                    "backlog_turnover",
                    "carried_forward_turnover",
                    "backlog_decay",
                    "buy_priority_backlog_weight",
                    "buy_priority_existing_weight",
                    "buy_priority_new_weight",
                    "sell_priority_backlog_weight",
                    "sell_priority_existing_weight",
                    "sell_priority_new_weight",
                    "average_backlog_age",
                    "average_backlog_age_multiplier",
                    "average_sell_backlog_age",
                    "average_sell_backlog_age_multiplier",
                    "target_turnover",
                    "implemented_turnover",
                    "turnover_scale",
                ],
            )
            writer.writeheader()
            for rebalance_date, portfolio in sorted(portfolios.items()):
                diagnostics = portfolio.diagnostics or {}
                metadata = portfolio.metadata or {}
                for permno, weight in sorted(portfolio.weights.items()):
                    writer.writerow(
                        {
                            "rebalance_date": rebalance_date.isoformat(),
                            "permno": permno,
                            "weight": f"{weight:.8f}",
                            "exposure": f"{portfolio.exposure:.4f}",
                            "is_benchmark_hedge": int(permno == "__BENCH__"),
                            "beta_exposure": f"{diagnostics.get('beta_exposure', 0.0):.8f}",
                            "downside_beta_exposure": f"{diagnostics.get('downside_beta_exposure', 0.0):.8f}",
                            "idio_vol_exposure": f"{diagnostics.get('idio_vol_exposure', 0.0):.8f}",
                            "size_exposure": f"{diagnostics.get('size_exposure', 0.0):.8f}",
                            "net_weight": f"{diagnostics.get('net_weight', 0.0):.8f}",
                            "gross_weight": f"{diagnostics.get('gross_weight', 0.0):.8f}",
                            "effective_turnover_budget": f"{metadata.get('effective_turnover_budget', 0.0):.8f}",
                            "budget_floor": f"{metadata.get('budget_floor', 0.0):.8f}",
                            "budget_ceiling": f"{metadata.get('budget_ceiling', 0.0):.8f}",
                            "budget_score_dispersion": f"{metadata.get('budget_score_dispersion', 0.0):.8f}",
                            "budget_recent_turnover": f"{metadata.get('budget_recent_turnover', 0.0):.8f}",
                            "budget_score_multiplier": f"{metadata.get('budget_score_multiplier', 1.0):.8f}",
                            "budget_turnover_multiplier": f"{metadata.get('budget_turnover_multiplier', 1.0):.8f}",
                            "average_slippage_bps": f"{metadata.get('average_slippage_bps', self.slippage_cost_bps):.8f}",
                            "target_max_participation_ratio": f"{metadata.get('target_max_participation_ratio', 0.0):.8f}",
                            "max_participation_ratio": f"{metadata.get('max_participation_ratio', 0.0):.8f}",
                            "liquidity_scale": f"{metadata.get('liquidity_scale', 1.0):.8f}",
                            "backlog_turnover": f"{metadata.get('backlog_turnover', 0.0):.8f}",
                            "carried_forward_turnover": f"{metadata.get('carried_forward_turnover', 0.0):.8f}",
                            "backlog_decay": f"{metadata.get('backlog_decay', 0.0):.8f}",
                            "buy_priority_backlog_weight": f"{metadata.get('buy_priority_backlog_weight', 1.0):.8f}",
                            "buy_priority_existing_weight": f"{metadata.get('buy_priority_existing_weight', 1.0):.8f}",
                            "buy_priority_new_weight": f"{metadata.get('buy_priority_new_weight', 1.0):.8f}",
                            "sell_priority_backlog_weight": f"{metadata.get('sell_priority_backlog_weight', 1.0):.8f}",
                            "sell_priority_existing_weight": f"{metadata.get('sell_priority_existing_weight', 1.0):.8f}",
                            "sell_priority_new_weight": f"{metadata.get('sell_priority_new_weight', 1.0):.8f}",
                            "average_backlog_age": f"{metadata.get('average_backlog_age', 0.0):.8f}",
                            "average_backlog_age_multiplier": f"{metadata.get('average_backlog_age_multiplier', 1.0):.8f}",
                            "average_sell_backlog_age": f"{metadata.get('average_sell_backlog_age', 0.0):.8f}",
                            "average_sell_backlog_age_multiplier": f"{metadata.get('average_sell_backlog_age_multiplier', 1.0):.8f}",
                            "target_turnover": f"{metadata.get('target_turnover', 0.0):.8f}",
                            "implemented_turnover": f"{metadata.get('implemented_turnover', 0.0):.8f}",
                            "turnover_scale": f"{metadata.get('turnover_scale', 1.0):.8f}",
                        }
                    )

    def _write_execution_diagnostics(self, records: list[dict[str, float | str]]) -> None:
        path = self.output_dir / "execution_diagnostics.csv"
        fieldnames = [
            "permno",
            "event_count",
            "trade_event_count",
            "partial_fill_count",
            "full_fill_rate",
            "average_fill_ratio",
            "average_target_trade",
            "average_implemented_trade",
            "average_residual_trade",
            "average_backlog_age",
            "max_backlog_age",
            "average_target_participation_ratio",
            "average_implemented_participation_ratio",
            "max_implemented_participation_ratio",
            "average_target_weight",
            "average_implemented_weight",
        ]
        grouped: dict[str, list[dict[str, float | str]]] = {}
        for record in records:
            permno = str(record["permno"])
            grouped.setdefault(permno, []).append(record)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for permno in sorted(grouped):
                permno_records = grouped[permno]
                trade_records = [record for record in permno_records if float(record["target_trade"]) > 1e-12]
                partial_fill_count = sum(1 for record in trade_records if float(record["fill_ratio"]) < 0.999999)
                full_fill_rate = (
                    sum(1 for record in trade_records if float(record["fill_ratio"]) >= 0.999999) / len(trade_records)
                    if trade_records
                    else 1.0
                )
                writer.writerow(
                    {
                        "permno": permno,
                        "event_count": len(permno_records),
                        "trade_event_count": len(trade_records),
                        "partial_fill_count": partial_fill_count,
                        "full_fill_rate": f"{full_fill_rate:.8f}",
                        "average_fill_ratio": f"{self._average_record_value(trade_records, 'fill_ratio', default=1.0):.8f}",
                        "average_target_trade": f"{self._average_record_value(trade_records, 'target_trade'):.8f}",
                        "average_implemented_trade": f"{self._average_record_value(trade_records, 'implemented_trade'):.8f}",
                        "average_residual_trade": f"{self._average_record_value(trade_records, 'residual_trade'):.8f}",
                        "average_backlog_age": f"{self._average_record_value(trade_records, 'backlog_age'):.8f}",
                        "max_backlog_age": f"{self._max_record_value(trade_records, 'backlog_age'):.8f}",
                        "average_target_participation_ratio": f"{self._average_record_value(trade_records, 'target_participation_ratio'):.8f}",
                        "average_implemented_participation_ratio": f"{self._average_record_value(trade_records, 'implemented_participation_ratio'):.8f}",
                        "max_implemented_participation_ratio": f"{self._max_record_value(trade_records, 'implemented_participation_ratio'):.8f}",
                        "average_target_weight": f"{self._average_record_value(permno_records, 'target_weight'):.8f}",
                        "average_implemented_weight": f"{self._average_record_value(permno_records, 'implemented_weight'):.8f}",
                    }
                )

    def _write_execution_diagnostics_by_bucket(self, records: list[dict[str, float | str]]) -> None:
        path = self.output_dir / "execution_diagnostics_by_bucket.csv"
        fieldnames = [
            "trade_side",
            "bucket",
            "trade_event_count",
            "distinct_permno_count",
            "partial_fill_count",
            "full_fill_rate",
            "average_fill_ratio",
            "total_target_trade",
            "total_implemented_trade",
            "total_residual_trade",
            "average_target_trade",
            "average_implemented_trade",
            "average_residual_trade",
            "average_backlog_age",
            "max_backlog_age",
            "average_target_participation_ratio",
            "average_implemented_participation_ratio",
            "max_implemented_participation_ratio",
            "total_commission_cost",
            "total_slippage_cost",
            "total_transaction_cost",
            "average_commission_cost",
            "average_slippage_cost",
            "average_transaction_cost",
            "average_transaction_cost_bps",
        ]
        bucket_records = self._execution_bucket_records(records)
        grouped: dict[tuple[str, str], list[dict[str, float | str]]] = {}
        for record in bucket_records:
            key = (str(record["trade_side"]), str(record["bucket"]))
            grouped.setdefault(key, []).append(record)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for trade_side, bucket in sorted(grouped):
                group_records = grouped[(trade_side, bucket)]
                writer.writerow(
                    {
                        "trade_side": trade_side,
                        "bucket": bucket,
                        **self._execution_bucket_summary(group_records),
                    }
                )

    def _write_execution_diagnostics_by_bucket_timeseries(self, records: list[dict[str, float | str]]) -> None:
        path = self.output_dir / "execution_diagnostics_by_bucket_timeseries.csv"
        fieldnames = [
            "rebalance_date",
            "trade_side",
            "bucket",
            "trade_event_count",
            "distinct_permno_count",
            "partial_fill_count",
            "full_fill_rate",
            "average_fill_ratio",
            "total_target_trade",
            "total_implemented_trade",
            "total_residual_trade",
            "average_target_trade",
            "average_implemented_trade",
            "average_residual_trade",
            "average_backlog_age",
            "max_backlog_age",
            "average_target_participation_ratio",
            "average_implemented_participation_ratio",
            "max_implemented_participation_ratio",
            "total_commission_cost",
            "total_slippage_cost",
            "total_transaction_cost",
            "average_commission_cost",
            "average_slippage_cost",
            "average_transaction_cost",
            "average_transaction_cost_bps",
        ]
        bucket_records = self._execution_bucket_records(records)
        grouped: dict[tuple[str, str, str], list[dict[str, float | str]]] = {}
        for record in bucket_records:
            key = (str(record["rebalance_date"]), str(record["trade_side"]), str(record["bucket"]))
            grouped.setdefault(key, []).append(record)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for rebalance_date, trade_side, bucket in sorted(grouped):
                writer.writerow(
                    {
                        "rebalance_date": rebalance_date,
                        "trade_side": trade_side,
                        "bucket": bucket,
                        **self._execution_bucket_summary(grouped[(rebalance_date, trade_side, bucket)]),
                    }
                )

    def _write_execution_backlog_aging(self, records: list[dict[str, float | str]]) -> None:
        episodes = self._execution_backlog_episodes(records)
        self._write_execution_backlog_aging_events(episodes)
        path = self.output_dir / "execution_backlog_aging.csv"
        fieldnames = [
            "backlog_side",
            "episode_count",
            "resolved_episode_count",
            "open_episode_count",
            "resolution_rate",
            "average_resolution_rebalances",
            "median_resolution_rebalances",
            "p90_resolution_rebalances",
            "max_resolution_rebalances",
            "average_initial_backlog_trade",
            "average_resolved_backlog_trade",
            "average_open_backlog_trade",
            "average_open_age_rebalances",
            "resolved_within_1_rebalance_rate",
            "resolved_within_2_rebalances_rate",
            "resolved_within_3_rebalances_rate",
        ]
        grouped: dict[str, list[dict[str, float | str]]] = {"all": list(episodes)}
        for episode in episodes:
            grouped.setdefault(str(episode["backlog_side"]), []).append(episode)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for backlog_side in sorted(grouped):
                writer.writerow(
                    {
                        "backlog_side": backlog_side,
                        **self._execution_backlog_aging_summary(grouped[backlog_side]),
                    }
                )

    def _write_execution_backlog_aging_events(self, episodes: list[dict[str, float | str]]) -> None:
        path = self.output_dir / "execution_backlog_aging_events.csv"
        fieldnames = [
            "permno",
            "backlog_side",
            "status",
            "start_rebalance_date",
            "resolved_rebalance_date",
            "event_count",
            "initial_backlog_trade",
            "final_backlog_trade",
            "resolution_rebalances",
            "max_backlog_age",
            "open_age_rebalances",
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for episode in sorted(episodes, key=lambda item: (str(item["permno"]), str(item["backlog_side"]), str(item["start_rebalance_date"]))):
                writer.writerow(
                    {
                        "permno": episode["permno"],
                        "backlog_side": episode["backlog_side"],
                        "status": episode["status"],
                        "start_rebalance_date": episode["start_rebalance_date"],
                        "resolved_rebalance_date": episode["resolved_rebalance_date"],
                        "event_count": int(episode["event_count"]),
                        "initial_backlog_trade": f"{float(episode['initial_backlog_trade']):.8f}",
                        "final_backlog_trade": f"{float(episode['final_backlog_trade']):.8f}",
                        "resolution_rebalances": f"{float(episode['resolution_rebalances']):.8f}",
                        "max_backlog_age": f"{float(episode['max_backlog_age']):.8f}",
                        "open_age_rebalances": f"{float(episode['open_age_rebalances']):.8f}",
                    }
                )

    def _write_execution_backlog_dropoff(self, records: list[dict[str, float | str]]) -> None:
        episodes = self._execution_backlog_episodes(records)
        self._write_execution_backlog_dropoff_events(episodes)
        path = self.output_dir / "execution_backlog_dropoff.csv"
        fieldnames = [
            "backlog_side",
            "episode_count",
            "executed_episode_count",
            "dropped_episode_count",
            "mixed_episode_count",
            "open_episode_count",
            "executed_episode_rate",
            "dropped_episode_rate",
            "mixed_episode_rate",
            "total_executed_backlog_trade",
            "total_dropped_backlog_trade",
            "average_executed_backlog_trade",
            "average_dropped_backlog_trade",
            "average_executed_ratio",
            "average_dropped_ratio",
        ]
        grouped: dict[str, list[dict[str, float | str]]] = {"all": list(episodes)}
        for episode in episodes:
            grouped.setdefault(str(episode["backlog_side"]), []).append(episode)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for backlog_side in sorted(grouped):
                writer.writerow(
                    {
                        "backlog_side": backlog_side,
                        **self._execution_backlog_dropoff_summary(grouped[backlog_side]),
                    }
                )

    def _write_execution_backlog_dropoff_timeseries(
        self,
        records: list[dict[str, float | str]],
        metadata_by_rebalance: dict[str, dict[str, float]],
    ) -> None:
        episodes = self._execution_backlog_episodes(records)
        path = self.output_dir / "execution_backlog_dropoff_timeseries.csv"
        fieldnames = [
            "terminal_rebalance_date",
            "backlog_side",
            "terminal_status",
            "episode_count",
            "total_initial_backlog_trade",
            "total_executed_backlog_trade",
            "total_dropped_backlog_trade",
            "average_executed_ratio",
            "average_dropped_ratio",
            "average_resolution_rebalances",
            "terminal_score_dispersion",
            "terminal_effective_turnover_budget",
            "terminal_implemented_turnover",
            "terminal_turnover_scale",
            "terminal_target_max_participation_ratio",
            "terminal_max_participation_ratio",
            "terminal_liquidity_scale",
            "terminal_average_slippage_bps",
        ]
        grouped: dict[tuple[str, str, str], list[dict[str, float | str]]] = {}
        for episode in episodes:
            terminal_date = str(episode["resolved_rebalance_date"] or episode["start_rebalance_date"])
            key = (terminal_date, str(episode["backlog_side"]), str(episode["terminal_status"]))
            grouped.setdefault(key, []).append(episode)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for terminal_date, backlog_side, terminal_status in sorted(grouped):
                grouped_episodes = grouped[(terminal_date, backlog_side, terminal_status)]
                regime_overlay = self._terminal_regime_overlay(metadata_by_rebalance.get(terminal_date, {}))
                writer.writerow(
                    {
                        "terminal_rebalance_date": terminal_date,
                        "backlog_side": backlog_side,
                        "terminal_status": terminal_status,
                        "episode_count": len(grouped_episodes),
                        "total_initial_backlog_trade": f"{sum(float(episode['initial_backlog_trade']) for episode in grouped_episodes):.8f}",
                        "total_executed_backlog_trade": f"{sum(float(episode['executed_backlog_trade']) for episode in grouped_episodes):.8f}",
                        "total_dropped_backlog_trade": f"{sum(float(episode['dropped_backlog_trade']) for episode in grouped_episodes):.8f}",
                        "average_executed_ratio": f"{self._average_episode_value(grouped_episodes, 'executed_ratio'):.8f}",
                        "average_dropped_ratio": f"{self._average_episode_value(grouped_episodes, 'dropped_ratio'):.8f}",
                        "average_resolution_rebalances": f"{self._average_episode_value(grouped_episodes, 'resolution_rebalances'):.8f}",
                        **regime_overlay,
                    }
                )

    def _write_execution_backlog_dropoff_by_regime(
        self,
        records: list[dict[str, float | str]],
        metadata_by_rebalance: dict[str, dict[str, float]],
    ) -> None:
        episodes = self._execution_backlog_episodes(records)
        path = self.output_dir / "execution_backlog_dropoff_by_regime.csv"
        fieldnames = [
            "backlog_side",
            "terminal_status",
            "score_dispersion_bucket",
            "turnover_bucket",
            "liquidity_bucket",
            "episode_count",
            "total_initial_backlog_trade",
            "total_executed_backlog_trade",
            "total_dropped_backlog_trade",
            "average_executed_ratio",
            "average_dropped_ratio",
            "average_resolution_rebalances",
            "average_terminal_score_dispersion",
            "average_terminal_effective_turnover_budget",
            "average_terminal_implemented_turnover",
            "average_terminal_turnover_scale",
            "average_terminal_target_max_participation_ratio",
            "average_terminal_max_participation_ratio",
            "average_terminal_liquidity_scale",
            "average_terminal_average_slippage_bps",
        ]
        grouped: dict[tuple[str, str, str, str, str], list[tuple[dict[str, float | str], dict[str, float]]]] = {}
        for episode in episodes:
            terminal_date = str(episode["resolved_rebalance_date"] or episode["start_rebalance_date"])
            regime_metrics = self._terminal_regime_metrics(metadata_by_rebalance.get(terminal_date, {}))
            key = (
                str(episode["backlog_side"]),
                str(episode["terminal_status"]),
                self._score_dispersion_bucket(regime_metrics["score_dispersion"]),
                self._turnover_bucket(regime_metrics["effective_turnover_budget"], regime_metrics["turnover_scale"]),
                self._liquidity_bucket(regime_metrics["liquidity_scale"]),
            )
            grouped.setdefault(key, []).append((episode, regime_metrics))
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for backlog_side, terminal_status, dispersion_bucket, turnover_bucket, liquidity_bucket in sorted(grouped):
                grouped_items = grouped[(backlog_side, terminal_status, dispersion_bucket, turnover_bucket, liquidity_bucket)]
                grouped_episodes = [episode for episode, _ in grouped_items]
                regime_metrics = [metrics for _, metrics in grouped_items]
                writer.writerow(
                    {
                        "backlog_side": backlog_side,
                        "terminal_status": terminal_status,
                        "score_dispersion_bucket": dispersion_bucket,
                        "turnover_bucket": turnover_bucket,
                        "liquidity_bucket": liquidity_bucket,
                        "episode_count": len(grouped_items),
                        "total_initial_backlog_trade": f"{sum(float(episode['initial_backlog_trade']) for episode in grouped_episodes):.8f}",
                        "total_executed_backlog_trade": f"{sum(float(episode['executed_backlog_trade']) for episode in grouped_episodes):.8f}",
                        "total_dropped_backlog_trade": f"{sum(float(episode['dropped_backlog_trade']) for episode in grouped_episodes):.8f}",
                        "average_executed_ratio": f"{self._average_episode_value(grouped_episodes, 'executed_ratio'):.8f}",
                        "average_dropped_ratio": f"{self._average_episode_value(grouped_episodes, 'dropped_ratio'):.8f}",
                        "average_resolution_rebalances": f"{self._average_episode_value(grouped_episodes, 'resolution_rebalances'):.8f}",
                        "average_terminal_score_dispersion": f"{sum(item['score_dispersion'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_effective_turnover_budget": f"{sum(item['effective_turnover_budget'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_implemented_turnover": f"{sum(item['implemented_turnover'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_turnover_scale": f"{sum(item['turnover_scale'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_target_max_participation_ratio": f"{sum(item['target_max_participation_ratio'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_max_participation_ratio": f"{sum(item['max_participation_ratio'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_liquidity_scale": f"{sum(item['liquidity_scale'] for item in regime_metrics) / len(regime_metrics):.8f}",
                        "average_terminal_average_slippage_bps": f"{sum(item['average_slippage_bps'] for item in regime_metrics) / len(regime_metrics):.8f}",
                    }
                )

    def _terminal_regime_metrics(self, metadata: dict[str, float]) -> dict[str, float]:
        return {
            "score_dispersion": float(metadata.get("selection_score_dispersion", 0.0)),
            "effective_turnover_budget": float(metadata.get("effective_turnover_budget", 0.0)),
            "implemented_turnover": float(metadata.get("implemented_turnover", 0.0)),
            "turnover_scale": float(metadata.get("turnover_scale", 1.0)),
            "target_max_participation_ratio": float(metadata.get("target_max_participation_ratio", 0.0)),
            "max_participation_ratio": float(metadata.get("max_participation_ratio", 0.0)),
            "liquidity_scale": float(metadata.get("liquidity_scale", 1.0)),
            "average_slippage_bps": float(metadata.get("average_slippage_bps", self.slippage_cost_bps)),
        }

    def _terminal_regime_overlay(self, metadata: dict[str, float]) -> dict[str, str]:
        regime_metrics = self._terminal_regime_metrics(metadata)
        return {
            "terminal_score_dispersion": f"{regime_metrics['score_dispersion']:.8f}",
            "terminal_effective_turnover_budget": f"{regime_metrics['effective_turnover_budget']:.8f}",
            "terminal_implemented_turnover": f"{regime_metrics['implemented_turnover']:.8f}",
            "terminal_turnover_scale": f"{regime_metrics['turnover_scale']:.8f}",
            "terminal_target_max_participation_ratio": f"{regime_metrics['target_max_participation_ratio']:.8f}",
            "terminal_max_participation_ratio": f"{regime_metrics['max_participation_ratio']:.8f}",
            "terminal_liquidity_scale": f"{regime_metrics['liquidity_scale']:.8f}",
            "terminal_average_slippage_bps": f"{regime_metrics['average_slippage_bps']:.8f}",
        }

    def _score_dispersion_bucket(self, score_dispersion: float) -> str:
        if score_dispersion < 0.25:
            return "flat"
        if score_dispersion < 1.0:
            return "medium"
        return "wide"

    def _turnover_bucket(self, effective_turnover_budget: float, turnover_scale: float) -> str:
        if turnover_scale < 0.999999:
            return "turnover_capped"
        if effective_turnover_budget > 0.0:
            return "turnover_budget_full"
        return "turnover_uncapped"

    def _liquidity_bucket(self, liquidity_scale: float) -> str:
        if liquidity_scale < 0.999999:
            return "liquidity_scaled"
        return "liquidity_full"

    def _write_execution_backlog_dropoff_events(self, episodes: list[dict[str, float | str]]) -> None:
        path = self.output_dir / "execution_backlog_dropoff_events.csv"
        fieldnames = [
            "permno",
            "backlog_side",
            "terminal_status",
            "start_rebalance_date",
            "resolved_rebalance_date",
            "event_count",
            "initial_backlog_trade",
            "executed_backlog_trade",
            "dropped_backlog_trade",
            "final_backlog_trade",
            "executed_ratio",
            "dropped_ratio",
            "resolution_rebalances",
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for episode in sorted(episodes, key=lambda item: (str(item["permno"]), str(item["backlog_side"]), str(item["start_rebalance_date"]))):
                writer.writerow(
                    {
                        "permno": episode["permno"],
                        "backlog_side": episode["backlog_side"],
                        "terminal_status": episode["terminal_status"],
                        "start_rebalance_date": episode["start_rebalance_date"],
                        "resolved_rebalance_date": episode["resolved_rebalance_date"],
                        "event_count": int(episode["event_count"]),
                        "initial_backlog_trade": f"{float(episode['initial_backlog_trade']):.8f}",
                        "executed_backlog_trade": f"{float(episode['executed_backlog_trade']):.8f}",
                        "dropped_backlog_trade": f"{float(episode['dropped_backlog_trade']):.8f}",
                        "final_backlog_trade": f"{float(episode['final_backlog_trade']):.8f}",
                        "executed_ratio": f"{float(episode['executed_ratio']):.8f}",
                        "dropped_ratio": f"{float(episode['dropped_ratio']):.8f}",
                        "resolution_rebalances": f"{float(episode['resolution_rebalances']):.8f}",
                    }
                )

    def _summarize(self, rows: list[PortfolioDay]) -> dict[str, float]:
        if not rows:
            return {
                "days": 0.0,
                "total_return": 0.0,
                "benchmark_total_return": 0.0,
                "active_total_return": 0.0,
                "gross_total_return": 0.0,
                "cagr": 0.0,
                "sharpe": 0.0,
                "information_ratio": 0.0,
                "max_drawdown": 0.0,
                "average_turnover": 0.0,
                "average_cash_weight": 0.0,
                "total_cash_carry": 0.0,
                "average_cash_carry": 0.0,
                "total_cash_drag": 0.0,
                "average_cash_drag": 0.0,
                "total_transaction_cost": 0.0,
                "total_short_borrow_cost": 0.0,
                "total_commission_cost": 0.0,
                "total_slippage_cost": 0.0,
                "average_transaction_cost": 0.0,
                "average_short_borrow_cost": 0.0,
                "transaction_cost_drag": 0.0,
            }
        equity = 1.0
        gross_equity = 1.0
        benchmark_equity = 1.0
        peak = 1.0
        returns = []
        active_returns = []
        turnovers = []
        cash_weights = []
        cash_carries = []
        cash_drags = []
        transaction_costs = []
        short_borrow_costs = []
        commission_costs = []
        slippage_costs = []
        max_drawdown = 0.0
        for row in rows:
            gross_equity *= 1.0 + row.gross_return
            equity *= 1.0 + row.net_return
            benchmark_equity *= 1.0 + row.benchmark_return
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity / peak - 1.0)
            returns.append(row.net_return)
            active_returns.append(row.active_return)
            turnovers.append(row.turnover)
            cash_weights.append(row.cash_weight)
            cash_carries.append(row.cash_carry)
            cash_drags.append(row.cash_drag)
            transaction_costs.append(row.transaction_cost)
            short_borrow_costs.append(row.short_borrow_cost)
            commission_costs.append(row.commission_cost)
            slippage_costs.append(row.slippage_cost)
        mean_return = sum(returns) / len(returns)
        variance = sum((value - mean_return) ** 2 for value in returns) / len(returns)
        active_mean = sum(active_returns) / len(active_returns)
        active_variance = sum((value - active_mean) ** 2 for value in active_returns) / len(active_returns)
        sharpe = 0.0
        information_ratio = 0.0
        if variance > 0:
            sharpe = (mean_return / math.sqrt(variance)) * math.sqrt(252)
        if active_variance > 0:
            information_ratio = (active_mean / math.sqrt(active_variance)) * math.sqrt(252)
        years = max(len(rows) / 252.0, 1 / 252.0)
        cagr = equity ** (1.0 / years) - 1.0
        summary = {
            "days": float(len(rows)),
            "total_return": equity - 1.0,
            "benchmark_total_return": benchmark_equity - 1.0,
            "active_total_return": equity - benchmark_equity,
            "gross_total_return": gross_equity - 1.0,
            "cagr": cagr,
            "sharpe": sharpe,
            "information_ratio": information_ratio,
            "max_drawdown": max_drawdown,
            "average_turnover": sum(turnovers) / len(turnovers),
            "average_cash_weight": sum(cash_weights) / len(cash_weights),
            "total_cash_carry": sum(cash_carries),
            "average_cash_carry": sum(cash_carries) / len(cash_carries),
            "total_cash_drag": sum(cash_drags),
            "average_cash_drag": sum(cash_drags) / len(cash_drags),
            "total_transaction_cost": sum(transaction_costs),
            "total_short_borrow_cost": sum(short_borrow_costs),
            "total_commission_cost": sum(commission_costs),
            "total_slippage_cost": sum(slippage_costs),
            "average_transaction_cost": sum(transaction_costs) / len(transaction_costs),
            "average_short_borrow_cost": sum(short_borrow_costs) / len(short_borrow_costs),
            "transaction_cost_drag": (gross_equity - 1.0) - (equity - 1.0),
        }
        summary_path = self.output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary

    def _short_borrow_cost(self, weights: dict[str, float], rebalance_rows: list[dict] | None = None) -> float:
        if not weights:
            return 0.0
        lookup = {
            row["permno"]: row
            for row in (rebalance_rows or [])
        }
        total_cost = 0.0
        for permno, weight in weights.items():
            if permno == "__BENCH__" or weight >= 0.0:
                continue
            annual_bps = float(lookup.get(permno, {}).get(self.short_borrow_cost_field, self.short_borrow_cost_bps_annual) or self.short_borrow_cost_bps_annual)
            if annual_bps <= 0.0:
                continue
            total_cost += abs(weight) * ((annual_bps / 252.0) / 10000.0)
        return total_cost

    def _compute_turnover(self, old_weights: dict[str, float], new_weights: dict[str, float]) -> float:
        securities = set(old_weights) | set(new_weights)
        return 0.5 * sum(abs(new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0)) for permno in securities)

    def _average_record_value(
        self,
        records: list[dict[str, float | str]],
        key: str,
        default: float = 0.0,
    ) -> float:
        if not records:
            return default
        return sum(float(record[key]) for record in records) / len(records)

    def _max_record_value(self, records: list[dict[str, float | str]], key: str) -> float:
        if not records:
            return 0.0
        return max(float(record[key]) for record in records)

    def _average_transaction_cost_bps(self, records: list[dict[str, float | str]]) -> float:
        total_implemented_trade = sum(float(record["implemented_trade"]) for record in records)
        if total_implemented_trade <= 1e-12:
            return 0.0
        total_cost = sum(float(record["commission_cost"]) + float(record["slippage_cost"]) for record in records)
        return (total_cost / (0.5 * total_implemented_trade)) * 10000.0

    def _backlog_progress_trade(self, backlog_side: str, record: dict[str, float | str]) -> float:
        old_weight = float(record["old_weight"])
        implemented_weight = float(record["implemented_weight"])
        if backlog_side == "buy":
            return max(implemented_weight - old_weight, 0.0)
        return max(old_weight - implemented_weight, 0.0)

    def _execution_backlog_episodes(self, records: list[dict[str, float | str]]) -> list[dict[str, float | str]]:
        grouped: dict[str, list[dict[str, float | str]]] = {}
        for record in records:
            grouped.setdefault(str(record["permno"]), []).append(record)
        episodes: list[dict[str, float | str]] = []
        for permno in sorted(grouped):
            permno_records = sorted(grouped[permno], key=lambda item: str(item["rebalance_date"]))
            active: dict[str, dict[str, float | str]] = {}
            for record in permno_records:
                rebalance_date = str(record["rebalance_date"])
                backlog_age = float(record["backlog_age"])
                residual_by_side = {
                    "buy": max(float(record["target_weight"]) - float(record["implemented_weight"]), 0.0),
                    "sell": max(float(record["implemented_weight"]) - float(record["target_weight"]), 0.0),
                }
                for backlog_side, residual_trade in residual_by_side.items():
                    active_episode = active.get(backlog_side)
                    if residual_trade > 1e-12:
                        if active_episode is None:
                            active_episode = {
                                "permno": permno,
                                "backlog_side": backlog_side,
                                "status": "open",
                                "terminal_status": "open",
                                "start_rebalance_date": rebalance_date,
                                "resolved_rebalance_date": "",
                                "event_count": 1.0,
                                "initial_backlog_trade": residual_trade,
                                "executed_backlog_trade": 0.0,
                                "dropped_backlog_trade": 0.0,
                                "final_backlog_trade": residual_trade,
                                "executed_ratio": 0.0,
                                "dropped_ratio": 0.0,
                                "resolution_rebalances": 0.0,
                                "max_backlog_age": backlog_age,
                                "open_age_rebalances": backlog_age + 1.0,
                            }
                            active[backlog_side] = active_episode
                        else:
                            previous_residual = float(active_episode["final_backlog_trade"])
                            progress_trade = self._backlog_progress_trade(backlog_side, record)
                            executed_step = min(progress_trade, previous_residual)
                            dropped_step = max(previous_residual - executed_step - residual_trade, 0.0)
                            active_episode["event_count"] = float(active_episode["event_count"]) + 1.0
                            active_episode["executed_backlog_trade"] = float(active_episode["executed_backlog_trade"]) + executed_step
                            active_episode["dropped_backlog_trade"] = float(active_episode["dropped_backlog_trade"]) + dropped_step
                            active_episode["final_backlog_trade"] = residual_trade
                            active_episode["max_backlog_age"] = max(float(active_episode["max_backlog_age"]), backlog_age)
                            active_episode["open_age_rebalances"] = max(float(active_episode["open_age_rebalances"]), backlog_age + 1.0)
                    elif active_episode is not None:
                        previous_residual = float(active_episode["final_backlog_trade"])
                        progress_trade = self._backlog_progress_trade(backlog_side, record)
                        executed_step = min(progress_trade, previous_residual)
                        dropped_step = max(previous_residual - executed_step, 0.0)
                        executed_total = float(active_episode["executed_backlog_trade"]) + executed_step
                        dropped_total = float(active_episode["dropped_backlog_trade"]) + dropped_step
                        initial_backlog = max(float(active_episode["initial_backlog_trade"]), 1e-12)
                        active_episode["status"] = "resolved"
                        if dropped_total <= 1e-12 and executed_total > 1e-12:
                            active_episode["terminal_status"] = "executed"
                        elif executed_total <= 1e-12 and dropped_total > 1e-12:
                            active_episode["terminal_status"] = "dropped"
                        elif executed_total > 1e-12 and dropped_total > 1e-12:
                            active_episode["terminal_status"] = "mixed"
                        else:
                            active_episode["terminal_status"] = "open"
                        active_episode["resolved_rebalance_date"] = rebalance_date
                        active_episode["resolution_rebalances"] = backlog_age
                        active_episode["executed_backlog_trade"] = executed_total
                        active_episode["dropped_backlog_trade"] = dropped_total
                        active_episode["final_backlog_trade"] = 0.0
                        active_episode["executed_ratio"] = executed_total / initial_backlog
                        active_episode["dropped_ratio"] = dropped_total / initial_backlog
                        active_episode["max_backlog_age"] = max(float(active_episode["max_backlog_age"]), backlog_age)
                        active_episode["open_age_rebalances"] = 0.0
                        episodes.append(active_episode)
                        del active[backlog_side]
            for active_episode in active.values():
                initial_backlog = max(float(active_episode["initial_backlog_trade"]), 1e-12)
                active_episode["terminal_status"] = "open"
                active_episode["executed_ratio"] = float(active_episode["executed_backlog_trade"]) / initial_backlog
                active_episode["dropped_ratio"] = float(active_episode["dropped_backlog_trade"]) / initial_backlog
                episodes.append(active_episode)
        return episodes

    def _execution_backlog_aging_summary(self, episodes: list[dict[str, float | str]]) -> dict[str, str | int]:
        resolved = [episode for episode in episodes if str(episode["status"]) == "resolved"]
        open_episodes = [episode for episode in episodes if str(episode["status"]) != "resolved"]
        resolution_rebalances = sorted(float(episode["resolution_rebalances"]) for episode in resolved)
        return {
            "episode_count": len(episodes),
            "resolved_episode_count": len(resolved),
            "open_episode_count": len(open_episodes),
            "resolution_rate": f"{(len(resolved) / len(episodes)) if episodes else 0.0:.8f}",
            "average_resolution_rebalances": f"{self._average_episode_value(resolved, 'resolution_rebalances'):.8f}",
            "median_resolution_rebalances": f"{self._percentile(resolution_rebalances, 0.5):.8f}",
            "p90_resolution_rebalances": f"{self._percentile(resolution_rebalances, 0.9):.8f}",
            "max_resolution_rebalances": f"{max(resolution_rebalances) if resolution_rebalances else 0.0:.8f}",
            "average_initial_backlog_trade": f"{self._average_episode_value(episodes, 'initial_backlog_trade'):.8f}",
            "average_resolved_backlog_trade": f"{self._average_episode_value(resolved, 'initial_backlog_trade'):.8f}",
            "average_open_backlog_trade": f"{self._average_episode_value(open_episodes, 'final_backlog_trade'):.8f}",
            "average_open_age_rebalances": f"{self._average_episode_value(open_episodes, 'open_age_rebalances'):.8f}",
            "resolved_within_1_rebalance_rate": f"{self._resolution_within_rate(resolved, 1.0):.8f}",
            "resolved_within_2_rebalances_rate": f"{self._resolution_within_rate(resolved, 2.0):.8f}",
            "resolved_within_3_rebalances_rate": f"{self._resolution_within_rate(resolved, 3.0):.8f}",
        }

    def _execution_backlog_dropoff_summary(self, episodes: list[dict[str, float | str]]) -> dict[str, str | int]:
        executed = [episode for episode in episodes if str(episode["terminal_status"]) == "executed"]
        dropped = [episode for episode in episodes if str(episode["terminal_status"]) == "dropped"]
        mixed = [episode for episode in episodes if str(episode["terminal_status"]) == "mixed"]
        open_episodes = [episode for episode in episodes if str(episode["terminal_status"]) == "open"]
        return {
            "episode_count": len(episodes),
            "executed_episode_count": len(executed),
            "dropped_episode_count": len(dropped),
            "mixed_episode_count": len(mixed),
            "open_episode_count": len(open_episodes),
            "executed_episode_rate": f"{(len(executed) / len(episodes)) if episodes else 0.0:.8f}",
            "dropped_episode_rate": f"{(len(dropped) / len(episodes)) if episodes else 0.0:.8f}",
            "mixed_episode_rate": f"{(len(mixed) / len(episodes)) if episodes else 0.0:.8f}",
            "total_executed_backlog_trade": f"{sum(float(episode['executed_backlog_trade']) for episode in episodes):.8f}",
            "total_dropped_backlog_trade": f"{sum(float(episode['dropped_backlog_trade']) for episode in episodes):.8f}",
            "average_executed_backlog_trade": f"{self._average_episode_value(episodes, 'executed_backlog_trade'):.8f}",
            "average_dropped_backlog_trade": f"{self._average_episode_value(episodes, 'dropped_backlog_trade'):.8f}",
            "average_executed_ratio": f"{self._average_episode_value(episodes, 'executed_ratio'):.8f}",
            "average_dropped_ratio": f"{self._average_episode_value(episodes, 'dropped_ratio'):.8f}",
        }

    def _average_episode_value(self, episodes: list[dict[str, float | str]], key: str) -> float:
        if not episodes:
            return 0.0
        return sum(float(episode[key]) for episode in episodes) / len(episodes)

    def _resolution_within_rate(self, resolved: list[dict[str, float | str]], threshold: float) -> float:
        if not resolved:
            return 0.0
        return sum(1 for episode in resolved if float(episode["resolution_rebalances"]) <= threshold) / len(resolved)

    def _percentile(self, values: list[float], quantile: float) -> float:
        if not values:
            return 0.0
        if len(values) == 1:
            return values[0]
        rank = (len(values) - 1) * min(max(quantile, 0.0), 1.0)
        lower = math.floor(rank)
        upper = math.ceil(rank)
        if lower == upper:
            return values[lower]
        weight = rank - lower
        return values[lower] * (1.0 - weight) + values[upper] * weight

    def _execution_bucket_records(self, records: list[dict[str, float | str]]) -> list[dict[str, float | str]]:
        bucket_records: list[dict[str, float | str]] = []
        bucket_mappings = [
            ("buy", "backlog", "buy_backlog_target_trade"),
            ("buy", "existing", "buy_existing_target_trade"),
            ("buy", "new", "buy_new_target_trade"),
            ("sell", "backlog", "sell_backlog_target_trade"),
            ("sell", "existing", "sell_existing_target_trade"),
            ("sell", "new", "sell_new_target_trade"),
        ]
        for record in records:
            total_target_trade = float(record["target_trade"])
            if total_target_trade <= 1e-12:
                continue
            fill_ratio = float(record["fill_ratio"])
            total_target_participation_ratio = float(record["target_participation_ratio"])
            total_implemented_participation_ratio = float(record["implemented_participation_ratio"])
            backlog_age = float(record["backlog_age"])
            permno = str(record["permno"])
            rebalance_date = str(record["rebalance_date"])
            for trade_side, bucket, key in bucket_mappings:
                target_trade = float(record[key])
                if target_trade <= 1e-12:
                    continue
                trade_share = target_trade / total_target_trade
                implemented_trade = target_trade * fill_ratio
                bucket_records.append(
                    {
                        "rebalance_date": rebalance_date,
                        "trade_side": trade_side,
                        "bucket": bucket,
                        "permno": permno,
                        "fill_ratio": fill_ratio,
                        "target_trade": target_trade,
                        "implemented_trade": implemented_trade,
                        "residual_trade": max(target_trade - implemented_trade, 0.0),
                        "backlog_age": backlog_age if bucket == "backlog" else 0.0,
                        "target_participation_ratio": total_target_participation_ratio * trade_share,
                        "implemented_participation_ratio": total_implemented_participation_ratio * trade_share,
                        "commission_cost": 0.5 * implemented_trade * (float(record["implemented_commission_bps"]) / 10000.0),
                        "slippage_cost": 0.5 * implemented_trade * (float(record["implemented_slippage_bps"]) / 10000.0),
                    }
                )
        return bucket_records

    def _execution_bucket_summary(self, records: list[dict[str, float | str]]) -> dict[str, int | str]:
        partial_fill_count = sum(1 for record in records if float(record["fill_ratio"]) < 0.999999)
        full_fill_rate = (
            sum(1 for record in records if float(record["fill_ratio"]) >= 0.999999) / len(records)
            if records
            else 1.0
        )
        total_target_trade = sum(float(record["target_trade"]) for record in records)
        total_implemented_trade = sum(float(record["implemented_trade"]) for record in records)
        total_residual_trade = sum(float(record["residual_trade"]) for record in records)
        total_commission_cost = sum(float(record["commission_cost"]) for record in records)
        total_slippage_cost = sum(float(record["slippage_cost"]) for record in records)
        total_transaction_cost = total_commission_cost + total_slippage_cost
        return {
            "trade_event_count": len(records),
            "distinct_permno_count": len({str(record["permno"]) for record in records}),
            "partial_fill_count": partial_fill_count,
            "full_fill_rate": f"{full_fill_rate:.8f}",
            "average_fill_ratio": f"{self._average_record_value(records, 'fill_ratio', default=1.0):.8f}",
            "total_target_trade": f"{total_target_trade:.8f}",
            "total_implemented_trade": f"{total_implemented_trade:.8f}",
            "total_residual_trade": f"{total_residual_trade:.8f}",
            "average_target_trade": f"{self._average_record_value(records, 'target_trade'):.8f}",
            "average_implemented_trade": f"{self._average_record_value(records, 'implemented_trade'):.8f}",
            "average_residual_trade": f"{self._average_record_value(records, 'residual_trade'):.8f}",
            "average_backlog_age": f"{self._average_record_value(records, 'backlog_age'):.8f}",
            "max_backlog_age": f"{self._max_record_value(records, 'backlog_age'):.8f}",
            "average_target_participation_ratio": f"{self._average_record_value(records, 'target_participation_ratio'):.8f}",
            "average_implemented_participation_ratio": f"{self._average_record_value(records, 'implemented_participation_ratio'):.8f}",
            "max_implemented_participation_ratio": f"{self._max_record_value(records, 'implemented_participation_ratio'):.8f}",
            "total_commission_cost": f"{total_commission_cost:.8f}",
            "total_slippage_cost": f"{total_slippage_cost:.8f}",
            "total_transaction_cost": f"{total_transaction_cost:.8f}",
            "average_commission_cost": f"{self._average_record_value(records, 'commission_cost'):.8f}",
            "average_slippage_cost": f"{self._average_record_value(records, 'slippage_cost'):.8f}",
            "average_transaction_cost": f"{total_transaction_cost / len(records):.8f}",
            "average_transaction_cost_bps": f"{self._average_transaction_cost_bps(records):.8f}",
        }

    def _implement_portfolio(
        self,
        target_portfolio,
        previous_weights: dict[str, float],
        previous_target_weights: dict[str, float],
        previous_backlog_ages: dict[str, float],
        realized_rebalance_turnovers: list[float],
        rebalance_rows: list[dict],
    ):
        desired_weights, backlog_metadata, execution_components = self._apply_execution_backlog(
            previous_weights,
            previous_target_weights,
            previous_backlog_ages,
            target_portfolio.weights,
        )
        implemented_weights, metadata = self._apply_turnover_cap(previous_weights, desired_weights, target_portfolio, realized_rebalance_turnovers, rebalance_rows)
        next_backlog_ages = self._next_backlog_ages(previous_backlog_ages, desired_weights, implemented_weights)
        diagnostics = self.strategy.recompute_diagnostics(implemented_weights, target_portfolio.selected_rows)
        combined_metadata = dict(target_portfolio.metadata or {})
        combined_metadata.update(backlog_metadata)
        combined_metadata.update(metadata)
        implemented_portfolio = type(target_portfolio)(
            rebalance_date=target_portfolio.rebalance_date,
            weights=implemented_weights,
            exposure=target_portfolio.exposure,
            diagnostics=diagnostics,
            selected_rows=target_portfolio.selected_rows,
            metadata=combined_metadata,
            target_weights=desired_weights,
        )
        return implemented_portfolio, desired_weights, next_backlog_ages, execution_components

    def _execution_diagnostic_records(
        self,
        rebalance_date,
        old_weights: dict[str, float],
        target_weights: dict[str, float],
        implemented_weights: dict[str, float],
        backlog_ages: dict[str, float],
        execution_components: dict[str, dict[str, float]],
        rebalance_rows: list[dict],
    ) -> list[dict[str, float | str]]:
        lookup = {row["permno"]: row for row in rebalance_rows}
        records: list[dict[str, float | str]] = []
        securities = set(old_weights) | set(target_weights) | set(implemented_weights) | set(backlog_ages)
        for permno in sorted(securities):
            if permno == "__BENCH__":
                continue
            target_weight = target_weights.get(permno, 0.0)
            implemented_weight = implemented_weights.get(permno, 0.0)
            target_trade = abs(target_weight - old_weights.get(permno, 0.0))
            implemented_trade = abs(implemented_weight - old_weights.get(permno, 0.0))
            residual_trade = abs(target_weight - implemented_weight)
            if (
                target_trade <= 1e-12
                and implemented_trade <= 1e-12
                and residual_trade <= 1e-12
                and backlog_ages.get(permno, 0.0) <= 0.0
            ):
                continue
            fill_ratio = implemented_trade / target_trade if target_trade > 1e-12 else 1.0
            components = execution_components.get(permno, {})
            records.append(
                {
                    "rebalance_date": rebalance_date.isoformat(),
                    "permno": permno,
                    "old_weight": old_weights.get(permno, 0.0),
                    "target_weight": target_weight,
                    "implemented_weight": implemented_weight,
                    "target_trade": target_trade,
                    "implemented_trade": implemented_trade,
                    "residual_trade": residual_trade,
                    "fill_ratio": fill_ratio,
                    "backlog_age": backlog_ages.get(permno, 0.0),
                    "target_participation_ratio": self._participation_ratio(permno, target_trade, lookup),
                    "implemented_participation_ratio": self._participation_ratio(permno, implemented_trade, lookup),
                    "buy_backlog_target_trade": components.get("buy_backlog", 0.0),
                    "buy_existing_target_trade": components.get("buy_existing", 0.0),
                    "buy_new_target_trade": components.get("buy_new", 0.0),
                    "sell_backlog_target_trade": components.get("sell_backlog", 0.0),
                    "sell_existing_target_trade": components.get("sell_existing", 0.0),
                    "sell_new_target_trade": components.get("sell_new", 0.0),
                    "implemented_commission_bps": self.commission_cost_bps,
                    "implemented_slippage_bps": self._implemented_slippage_bps(permno, implemented_trade, lookup),
                }
            )
        return records

    def _cash_metrics(
        self,
        target_weights: dict[str, float],
        implemented_weights: dict[str, float],
        security_returns: dict[str, float],
        benchmark_return: float,
        cash_rate: float,
    ) -> tuple[float, float, float]:
        cash_weight = 0.0
        cash_carry = 0.0
        cash_drag = 0.0
        securities = set(target_weights) | set(implemented_weights)
        for permno in securities:
            target_long = max(target_weights.get(permno, 0.0), 0.0)
            implemented_long = max(implemented_weights.get(permno, 0.0), 0.0)
            long_shortfall = max(target_long - implemented_long, 0.0)
            if long_shortfall <= 0.0:
                continue
            asset_return = benchmark_return if permno == "__BENCH__" else security_returns.get(permno, 0.0)
            cash_weight += long_shortfall
            cash_carry += long_shortfall * cash_rate
            cash_drag += long_shortfall * (asset_return - cash_rate)
        return cash_weight, cash_carry, cash_drag

    def _apply_execution_backlog(
        self,
        old_weights: dict[str, float],
        previous_target_weights: dict[str, float],
        previous_backlog_ages: dict[str, float],
        new_weights: dict[str, float],
    ) -> tuple[dict[str, float], dict[str, float], dict[str, dict[str, float]]]:
        backlog_turnover = self._compute_turnover(old_weights, previous_target_weights)
        buy_priority_enabled = any(
            abs(value - 1.0) > 1e-12
            for value in (
                self.execution_priority_backlog_buy,
                self.execution_priority_existing_buy,
                self.execution_priority_new_buy,
                self.execution_priority_backlog_sell,
                self.execution_priority_existing_sell,
                self.execution_priority_new_sell,
            )
        )
        securities = set(old_weights) | set(previous_target_weights) | set(new_weights)
        desired_buys: dict[str, float] = {}
        desired_sells: dict[str, float] = {}
        buy_priority: dict[str, float] = {}
        sell_priority: dict[str, float] = {}
        buy_priority_components: dict[str, dict[str, float]] = {}
        sell_priority_components: dict[str, dict[str, float]] = {}
        execution_components: dict[str, dict[str, float]] = {}
        backlog_ages_used: list[float] = []
        backlog_age_multipliers: list[float] = []
        sell_backlog_ages_used: list[float] = []
        sell_backlog_age_multipliers: list[float] = []
        total_buy = 0.0
        total_sell = 0.0
        for permno in securities:
            desired_delta = new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0)
            backlog_delta = 0.0
            if self.execution_backlog_carry_forward_enabled and self.execution_backlog_decay > 0.0 and previous_target_weights:
                backlog_delta = (previous_target_weights.get(permno, 0.0) - old_weights.get(permno, 0.0)) * self.execution_backlog_decay
            desired_buy = max(desired_delta, 0.0)
            desired_sell = max(-desired_delta, 0.0)
            backlog_buy = max(backlog_delta, 0.0)
            backlog_sell = max(-backlog_delta, 0.0)
            if desired_buy > 0.0:
                desired_buys[permno] = desired_buy
                category_weight = self.execution_priority_existing_buy if old_weights.get(permno, 0.0) > 0.0 else self.execution_priority_new_buy
                backlog_age = previous_backlog_ages.get(permno, 0.0)
                backlog_age_multiplier = self._backlog_age_multiplier(backlog_age)
                buy_backlog_component = self.execution_priority_backlog_buy * backlog_buy * backlog_age_multiplier
                buy_current_component = category_weight * desired_buy
                buy_priority_components[permno] = {
                    "backlog": buy_backlog_component,
                    "current": buy_current_component,
                }
                buy_priority[permno] = buy_backlog_component + buy_current_component
                if backlog_buy > 0.0:
                    backlog_ages_used.append(backlog_age)
                    backlog_age_multipliers.append(backlog_age_multiplier)
                total_buy += desired_buy
            if desired_sell > 0.0:
                desired_sells[permno] = desired_sell
                sell_category_weight = self.execution_priority_existing_sell if abs(old_weights.get(permno, 0.0)) > 1e-12 else self.execution_priority_new_sell
                sell_backlog_age = previous_backlog_ages.get(permno, 0.0)
                sell_backlog_age_multiplier = self._backlog_age_multiplier(sell_backlog_age)
                sell_backlog_component = self.execution_priority_backlog_sell * backlog_sell * sell_backlog_age_multiplier
                sell_current_component = sell_category_weight * desired_sell
                sell_priority_components[permno] = {
                    "backlog": sell_backlog_component,
                    "current": sell_current_component,
                }
                sell_priority[permno] = sell_backlog_component + sell_current_component
                if backlog_sell > 0.0:
                    sell_backlog_ages_used.append(sell_backlog_age)
                    sell_backlog_age_multipliers.append(sell_backlog_age_multiplier)
                total_sell += desired_sell

        total_buy_priority = sum(buy_priority.values())
        total_sell_priority = sum(sell_priority.values())
        effective_trades: dict[str, float] = {}
        for permno, desired_buy in desired_buys.items():
            if total_buy_priority > 0.0:
                effective_buy = total_buy * buy_priority[permno] / total_buy_priority
            else:
                effective_buy = desired_buy
            effective_trades[permno] = effective_buy
            components = buy_priority_components.get(permno, {})
            asset_total_priority = components.get("backlog", 0.0) + components.get("current", 0.0)
            bucket_allocations = execution_components.setdefault(
                permno,
                {
                    "buy_backlog": 0.0,
                    "buy_existing": 0.0,
                    "buy_new": 0.0,
                    "sell_backlog": 0.0,
                    "sell_existing": 0.0,
                    "sell_new": 0.0,
                },
            )
            if asset_total_priority > 0.0:
                bucket_allocations["buy_backlog"] = effective_buy * components.get("backlog", 0.0) / asset_total_priority
                current_buy = effective_buy * components.get("current", 0.0) / asset_total_priority
            else:
                current_buy = effective_buy
            if old_weights.get(permno, 0.0) > 0.0:
                bucket_allocations["buy_existing"] = current_buy
            else:
                bucket_allocations["buy_new"] = current_buy
        for permno, desired_sell in desired_sells.items():
            if total_sell_priority > 0.0:
                effective_sell = total_sell * sell_priority[permno] / total_sell_priority
            else:
                effective_sell = desired_sell
            effective_trades[permno] = effective_trades.get(permno, 0.0) - effective_sell
            components = sell_priority_components.get(permno, {})
            asset_total_priority = components.get("backlog", 0.0) + components.get("current", 0.0)
            bucket_allocations = execution_components.setdefault(
                permno,
                {
                    "buy_backlog": 0.0,
                    "buy_existing": 0.0,
                    "buy_new": 0.0,
                    "sell_backlog": 0.0,
                    "sell_existing": 0.0,
                    "sell_new": 0.0,
                },
            )
            if asset_total_priority > 0.0:
                bucket_allocations["sell_backlog"] = effective_sell * components.get("backlog", 0.0) / asset_total_priority
                current_sell = effective_sell * components.get("current", 0.0) / asset_total_priority
            else:
                current_sell = effective_sell
            if abs(old_weights.get(permno, 0.0)) > 1e-12:
                bucket_allocations["sell_existing"] = current_sell
            else:
                bucket_allocations["sell_new"] = current_sell

        adjusted = {
            permno: old_weights.get(permno, 0.0) + effective_trades.get(permno, 0.0)
            for permno in securities
        }
        adjusted = self._clean_weights(adjusted)
        return adjusted, {
            "backlog_turnover": backlog_turnover,
            "carried_forward_turnover": self._compute_turnover(new_weights, adjusted),
            "backlog_decay": self.execution_backlog_decay if self.execution_backlog_carry_forward_enabled else 0.0,
            "buy_priority_backlog_weight": self.execution_priority_backlog_buy,
            "buy_priority_existing_weight": self.execution_priority_existing_buy,
            "buy_priority_new_weight": self.execution_priority_new_buy,
            "sell_priority_backlog_weight": self.execution_priority_backlog_sell,
            "sell_priority_existing_weight": self.execution_priority_existing_sell,
            "sell_priority_new_weight": self.execution_priority_new_sell,
            "average_backlog_age": sum(backlog_ages_used) / len(backlog_ages_used) if backlog_ages_used else 0.0,
            "average_backlog_age_multiplier": sum(backlog_age_multipliers) / len(backlog_age_multipliers) if backlog_age_multipliers else 1.0,
            "average_sell_backlog_age": sum(sell_backlog_ages_used) / len(sell_backlog_ages_used) if sell_backlog_ages_used else 0.0,
            "average_sell_backlog_age_multiplier": sum(sell_backlog_age_multipliers) / len(sell_backlog_age_multipliers) if sell_backlog_age_multipliers else 1.0,
        }, execution_components

    def _next_backlog_ages(
        self,
        previous_backlog_ages: dict[str, float],
        target_weights: dict[str, float],
        implemented_weights: dict[str, float],
    ) -> dict[str, float]:
        next_ages: dict[str, float] = {}
        securities = set(target_weights) | set(implemented_weights)
        for permno in securities:
            residual_target_gap = target_weights.get(permno, 0.0) - implemented_weights.get(permno, 0.0)
            if abs(residual_target_gap) > 1e-12:
                next_ages[permno] = previous_backlog_ages.get(permno, 0.0) + 1.0
        return next_ages

    def _backlog_age_multiplier(self, backlog_age: float) -> float:
        if self.execution_backlog_age_half_life_rebalances <= 0.0 or backlog_age <= 0.0:
            return 1.0
        return math.exp(-math.log(2.0) * backlog_age / self.execution_backlog_age_half_life_rebalances)

    def _apply_turnover_cap(
        self,
        old_weights: dict[str, float],
        new_weights: dict[str, float],
        target_portfolio,
        realized_rebalance_turnovers: list[float],
        rebalance_rows: list[dict],
    ) -> tuple[dict[str, float], dict[str, float]]:
        target_turnover = self._compute_turnover(old_weights, new_weights)
        budget_metadata = self._turnover_budget_metadata(target_portfolio, realized_rebalance_turnovers)
        max_turnover = budget_metadata["effective_turnover_budget"]
        adjusted = dict(new_weights)
        if old_weights and target_turnover > 0.0 and max_turnover > 0.0 and target_turnover > max_turnover:
            scale = max_turnover / target_turnover
            securities = set(old_weights) | set(new_weights)
            adjusted = {
                permno: old_weights.get(permno, 0.0) + scale * (new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0))
                for permno in securities
            }
        adjusted = self._clean_weights(adjusted)
        adjusted, liquidity_metadata = self._apply_participation_cap(old_weights, adjusted, rebalance_rows)
        slippage_metadata = self._slippage_metadata(old_weights, adjusted, rebalance_rows)
        implemented_turnover = self._compute_turnover(old_weights, adjusted)
        turnover_scale = implemented_turnover / target_turnover if target_turnover > 0.0 else 1.0
        return adjusted, {
            **budget_metadata,
            **liquidity_metadata,
            **slippage_metadata,
            "target_turnover": target_turnover,
            "implemented_turnover": implemented_turnover,
            "turnover_scale": turnover_scale,
        }

    def _clean_weights(self, weights: dict[str, float]) -> dict[str, float]:
        return {
            permno: weight
            for permno, weight in weights.items()
            if abs(weight) > 1e-12
        }

    def _execution_step_weights(
        self,
        current_weights: dict[str, float],
        target_weights: dict[str, float],
        days_remaining: int,
    ) -> dict[str, float]:
        if days_remaining <= 1:
            return dict(target_weights)
        securities = set(current_weights) | set(target_weights)
        stepped = {
            permno: current_weights.get(permno, 0.0) + (target_weights.get(permno, 0.0) - current_weights.get(permno, 0.0)) / days_remaining
            for permno in securities
        }
        return self._clean_weights(stepped)

    def _turnover_budget_metadata(self, target_portfolio, realized_rebalance_turnovers: list[float]) -> dict[str, float]:
        base_budget = max(float(self.strategy.config.get("max_turnover_per_rebalance", 0.0)), 0.0)
        adaptive_enabled = bool(self.strategy.config.get("adaptive_turnover_budget_enabled", False))
        floor = max(float(self.strategy.config.get("adaptive_turnover_budget_floor", base_budget)), 0.0)
        ceiling = max(float(self.strategy.config.get("adaptive_turnover_budget_ceiling", base_budget)), floor)
        score_dispersion = float((target_portfolio.metadata or {}).get("selection_score_dispersion", 0.0))
        score_scale = max(float(self.strategy.config.get("adaptive_turnover_budget_score_scale", 0.0)), 0.0)
        recent_scale = max(float(self.strategy.config.get("adaptive_turnover_budget_recent_turnover_scale", 0.0)), 0.0)
        lookback = max(int(self.strategy.config.get("adaptive_turnover_budget_lookback_rebalances", 3)), 1)
        recent_window = realized_rebalance_turnovers[-lookback:]
        recent_turnover = sum(recent_window) / len(recent_window) if recent_window else 0.0
        score_multiplier = 1.0 + score_scale * score_dispersion if adaptive_enabled else 1.0
        turnover_multiplier = 1.0 / (1.0 + recent_scale * recent_turnover) if adaptive_enabled else 1.0
        effective_budget = base_budget
        if adaptive_enabled and base_budget > 0.0:
            effective_budget = min(max(base_budget * score_multiplier * turnover_multiplier, floor), ceiling)
        return {
            "effective_turnover_budget": effective_budget,
            "budget_floor": floor,
            "budget_ceiling": ceiling,
            "budget_score_dispersion": score_dispersion,
            "budget_recent_turnover": recent_turnover,
            "budget_score_multiplier": score_multiplier,
            "budget_turnover_multiplier": turnover_multiplier,
        }

    def _slippage_metadata(self, old_weights: dict[str, float], new_weights: dict[str, float], rebalance_rows: list[dict]) -> dict[str, float]:
        lookup = {row["permno"]: row for row in rebalance_rows}
        max_participation_ratio = self._portfolio_max_participation_ratio(old_weights, new_weights, lookup)
        if self.slippage_model == "fixed":
            turnover = self._compute_turnover(old_weights, new_weights)
            return {
                "slippage_cost": turnover * (self.slippage_cost_bps / 10000.0),
                "average_slippage_bps": self.slippage_cost_bps,
                "max_participation_ratio": max_participation_ratio,
            }
        securities = set(old_weights) | set(new_weights)
        total_slippage_cost = 0.0
        total_traded_weight = 0.0
        weighted_bps = 0.0
        for permno in securities:
            traded_weight = abs(new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0))
            if traded_weight <= 0.0:
                continue
            asset_slippage_bps, participation_ratio = self._asset_slippage_bps(permno, traded_weight, lookup)
            total_slippage_cost += 0.5 * traded_weight * (asset_slippage_bps / 10000.0)
            total_traded_weight += traded_weight
            weighted_bps += traded_weight * asset_slippage_bps
            max_participation_ratio = max(max_participation_ratio, participation_ratio)
        average_slippage_bps = weighted_bps / total_traded_weight if total_traded_weight > 0.0 else self.slippage_cost_bps
        return {
            "slippage_cost": total_slippage_cost,
            "average_slippage_bps": average_slippage_bps,
            "max_participation_ratio": max_participation_ratio,
        }

    def _apply_participation_cap(
        self,
        old_weights: dict[str, float],
        new_weights: dict[str, float],
        rebalance_rows: list[dict],
    ) -> tuple[dict[str, float], dict[str, float]]:
        lookup = {row["permno"]: row for row in rebalance_rows}
        target_max_participation_ratio = self._portfolio_max_participation_ratio(old_weights, new_weights, lookup)
        if (
            self.max_trade_participation_ratio <= 0.0
            or self.slippage_notional <= 0.0
            or target_max_participation_ratio <= 0.0
            or target_max_participation_ratio <= self.max_trade_participation_ratio
        ):
            return new_weights, {
                "target_max_participation_ratio": target_max_participation_ratio,
                "liquidity_scale": 1.0,
            }
        scale = self.max_trade_participation_ratio / target_max_participation_ratio
        securities = set(old_weights) | set(new_weights)
        adjusted = {
            permno: old_weights.get(permno, 0.0) + scale * (new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0))
            for permno in securities
        }
        return self._clean_weights(adjusted), {
            "target_max_participation_ratio": target_max_participation_ratio,
            "liquidity_scale": scale,
        }

    def _portfolio_max_participation_ratio(
        self,
        old_weights: dict[str, float],
        new_weights: dict[str, float],
        lookup: dict[str, dict],
    ) -> float:
        securities = set(old_weights) | set(new_weights)
        max_participation_ratio = 0.0
        for permno in securities:
            traded_weight = abs(new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0))
            if traded_weight <= 0.0:
                continue
            max_participation_ratio = max(
                max_participation_ratio,
                self._participation_ratio(permno, traded_weight, lookup),
            )
        return max_participation_ratio

    def _asset_slippage_bps(self, permno: str, traded_weight: float, lookup: dict[str, dict]) -> tuple[float, float]:
        base_bps = self.slippage_cost_bps
        participation_ratio = self._participation_ratio(permno, traded_weight, lookup)
        return base_bps + self._impact_bps(participation_ratio), participation_ratio

    def _implemented_slippage_bps(self, permno: str, traded_weight: float, lookup: dict[str, dict]) -> float:
        if traded_weight <= 0.0:
            return 0.0
        if self.slippage_model == "fixed":
            return self.slippage_cost_bps
        asset_slippage_bps, _ = self._asset_slippage_bps(permno, traded_weight, lookup)
        return asset_slippage_bps

    def _impact_bps(self, participation_ratio: float) -> float:
        if participation_ratio <= 0.0 or self.slippage_model == "fixed":
            return 0.0
        return self.slippage_impact_bps_per_adv * (participation_ratio ** self.slippage_impact_exponent)

    def _participation_ratio(self, permno: str, traded_weight: float, lookup: dict[str, dict]) -> float:
        if permno == "__BENCH__" or self.slippage_notional <= 0.0:
            return 0.0
        adv = max(float(lookup.get(permno, {}).get("avg_dollar_volume", 0.0)), self.slippage_adv_floor)
        return (traded_weight * self.slippage_notional) / adv
