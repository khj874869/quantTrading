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
    def __init__(self, prepared_data: PreparedData, strategy: MultiSignalStrategy, output_dir: Path, transaction_cost_bps: float = 10.0) -> None:
        self.prepared_data = prepared_data
        self.strategy = strategy
        self.output_dir = output_dir
        self.transaction_cost_bps = transaction_cost_bps

    def run(self) -> dict[str, float]:
        ensure_directory(self.output_dir)
        rebalances = sorted(self.prepared_data.features_by_rebalance.keys())
        daily_dates = sorted(self.prepared_data.returns_by_date.keys())
        portfolios = {
            rebalance_date: self.strategy.build_weights(
                rebalance_date,
                self.prepared_data.features_by_rebalance[rebalance_date],
            )
            for rebalance_date in rebalances
        }
        self._write_rebalance_weights(portfolios)

        rows: list[PortfolioDay] = []
        current_portfolio = None
        previous_weights: dict[str, float] = {}
        pending_turnover = 0.0
        pending_cost = 0.0
        rebalance_index = 0
        for current_date in daily_dates:
            while rebalance_index < len(rebalances) and rebalances[rebalance_index] <= current_date:
                current_portfolio = portfolios[rebalances[rebalance_index]]
                pending_turnover = self._compute_turnover(previous_weights, current_portfolio.weights)
                pending_cost = pending_turnover * (self.transaction_cost_bps / 10000.0)
                previous_weights = dict(current_portfolio.weights)
                rebalance_index += 1
            if current_portfolio is None or not current_portfolio.weights:
                continue
            security_returns = self.prepared_data.returns_by_date[current_date]
            benchmark_return = self.prepared_data.benchmark_by_date.get(current_date, 0.0)
            gross_return = sum(
                weight * (benchmark_return if permno == "__BENCH__" else security_returns.get(permno, 0.0))
                for permno, weight in current_portfolio.weights.items()
            )
            transaction_cost = pending_cost
            turnover = pending_turnover
            pending_cost = 0.0
            pending_turnover = 0.0
            net_return = gross_return - transaction_cost
            rows.append(
                PortfolioDay(
                    date=current_date,
                    gross_return=gross_return,
                    net_return=net_return,
                    benchmark_return=benchmark_return,
                    active_return=net_return - benchmark_return,
                    exposure=current_portfolio.exposure,
                    holdings=sum(1 for permno in current_portfolio.weights if permno != "__BENCH__"),
                    turnover=turnover,
                    transaction_cost=transaction_cost,
                )
            )
        self._write_daily_returns(rows)
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
                    "holdings",
                    "turnover",
                    "transaction_cost",
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
                        "holdings": row.holdings,
                        "turnover": f"{row.turnover:.8f}",
                        "transaction_cost": f"{row.transaction_cost:.8f}",
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
                ],
            )
            writer.writeheader()
            for rebalance_date, portfolio in sorted(portfolios.items()):
                diagnostics = portfolio.diagnostics or {}
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
                        }
                    )

    def _summarize(self, rows: list[PortfolioDay]) -> dict[str, float]:
        if not rows:
            return {
                "days": 0.0,
                "total_return": 0.0,
                "benchmark_total_return": 0.0,
                "active_total_return": 0.0,
                "cagr": 0.0,
                "sharpe": 0.0,
                "information_ratio": 0.0,
                "max_drawdown": 0.0,
                "average_turnover": 0.0,
            }
        equity = 1.0
        benchmark_equity = 1.0
        peak = 1.0
        returns = []
        active_returns = []
        turnovers = []
        max_drawdown = 0.0
        for row in rows:
            equity *= 1.0 + row.net_return
            benchmark_equity *= 1.0 + row.benchmark_return
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity / peak - 1.0)
            returns.append(row.net_return)
            active_returns.append(row.active_return)
            turnovers.append(row.turnover)
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
            "cagr": cagr,
            "sharpe": sharpe,
            "information_ratio": information_ratio,
            "max_drawdown": max_drawdown,
            "average_turnover": sum(turnovers) / len(turnovers),
        }
        summary_path = self.output_dir / "summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return summary

    def _compute_turnover(self, old_weights: dict[str, float], new_weights: dict[str, float]) -> float:
        securities = set(old_weights) | set(new_weights)
        return 0.5 * sum(abs(new_weights.get(permno, 0.0) - old_weights.get(permno, 0.0)) for permno in securities)
