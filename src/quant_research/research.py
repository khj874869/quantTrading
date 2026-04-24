from __future__ import annotations

import csv
import itertools
import json
import math
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

from .backtest import Backtester
from .cache import PreparedDataCache
from .config import Config
from .strategy import MultiSignalStrategy
from .utils import ensure_directory, parse_date, resolve_backtest_costs


def run_parameter_sweep(config: Config) -> Path:
    sweep_config = config.sweep
    strategy_grid = sweep_config.get("strategy_grid", {})
    combinations = _expand_strategy_grid(strategy_grid)
    output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
    sweep_dir = output_dir / "sweeps"
    ensure_directory(sweep_dir)
    if not combinations:
        raise ValueError("sweep.strategy_grid must contain at least one strategy parameter list")

    results: list[dict] = []
    for run_index, overrides in enumerate(combinations, start=1):
        run_config = _with_strategy_overrides(config, overrides)
        prepared = PreparedDataCache(run_config).load_or_build().prepared_data
        run_dir = sweep_dir / f"run_{run_index:03d}"
        costs = resolve_backtest_costs(run_config.strategy)
        summary = Backtester(
            prepared,
            MultiSignalStrategy(run_config.strategy),
            output_dir=run_dir,
            **costs,
        ).run()
        results.append(
            {
                "run_id": f"run_{run_index:03d}",
                **overrides,
                **summary,
            }
        )

    ranked = sorted(
        results,
        key=lambda row: (
            row.get("information_ratio", 0.0),
            row.get("active_total_return", 0.0),
            row.get("sharpe", 0.0),
        ),
        reverse=True,
    )
    summary_path = sweep_dir / "summary.csv"
    _write_sweep_summary(summary_path, ranked)
    (sweep_dir / "summary.json").write_text(json.dumps(ranked, indent=2), encoding="utf-8")
    return summary_path


def run_walk_forward_optimization(config: Config) -> Path:
    sweep_config = config.sweep
    walk_forward_config = config.walk_forward
    strategy_grid = sweep_config.get("strategy_grid", {})
    combinations = _expand_strategy_grid(strategy_grid)
    windows = _resolve_walk_forward_windows(config)
    output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
    walk_forward_dir = output_dir / "walk_forward"
    ensure_directory(walk_forward_dir)
    if not combinations:
        raise ValueError("sweep.strategy_grid must contain at least one strategy parameter list")
    if not windows:
        raise ValueError("walk_forward configuration did not produce any train/test windows")

    selection_metric = str(walk_forward_config.get("selection_metric", "information_ratio")).strip() or "information_ratio"
    results: list[dict] = []
    selected_params_rows: list[dict] = []
    oos_daily_rows: list[dict] = []
    for window_index, window in enumerate(windows, start=1):
        window_dir = walk_forward_dir / f"window_{window_index:03d}"
        train_results: list[dict] = []
        for run_index, overrides in enumerate(combinations, start=1):
            train_dir = window_dir / "train" / f"run_{run_index:03d}"
            train_config = _with_strategy_overrides(
                _with_strategy_period(config, window["train_start"], window["train_end"]),
                overrides,
            )
            train_summary = _run_backtest(train_config, train_dir)
            train_results.append(
                {
                    "run_id": f"run_{run_index:03d}",
                    **overrides,
                    **train_summary,
                }
            )

        best_train = _select_best_result(train_results, selection_metric)
        best_overrides = {
            key: value
            for key, value in best_train.items()
            if key not in _SUMMARY_KEYS and key != "run_id"
        }
        test_dir = window_dir / "test"
        test_config = _with_strategy_overrides(
            _with_strategy_period(config, window["test_start"], window["test_end"]),
            best_overrides,
        )
        test_summary = _run_backtest(test_config, test_dir)
        oos_daily_rows.extend(_load_daily_return_rows(test_dir / "portfolio_daily_returns.csv", f"window_{window_index:03d}"))
        row = {
            "window_id": f"window_{window_index:03d}",
            "train_start": window["train_start"].isoformat(),
            "train_end": window["train_end"].isoformat(),
            "test_start": window["test_start"].isoformat(),
            "test_end": window["test_end"].isoformat(),
            "selection_metric": selection_metric,
            "selected_run_id": best_train["run_id"],
            "selected_metric_value": best_train.get(selection_metric, 0.0),
            **{f"param_{key}": value for key, value in best_overrides.items()},
            **{f"train_{key}": value for key, value in best_train.items() if key in _SUMMARY_KEYS},
            **{f"test_{key}": value for key, value in test_summary.items() if key in _SUMMARY_KEYS},
        }
        results.append(row)
        selected_params_rows.append(
            {
                "window_id": f"window_{window_index:03d}",
                "train_start": window["train_start"].isoformat(),
                "train_end": window["train_end"].isoformat(),
                "test_start": window["test_start"].isoformat(),
                "test_end": window["test_end"].isoformat(),
                "selected_run_id": best_train["run_id"],
                "selection_metric": selection_metric,
                "selected_metric_value": best_train.get(selection_metric, 0.0),
                **best_overrides,
            }
        )
        (window_dir / "train_summary.json").write_text(json.dumps(train_results, indent=2), encoding="utf-8")
        (window_dir / "selected_params.json").write_text(json.dumps(best_overrides, indent=2), encoding="utf-8")
        (window_dir / "test_summary.json").write_text(json.dumps(test_summary, indent=2), encoding="utf-8")

    summary_path = walk_forward_dir / "summary.csv"
    _write_sweep_summary(summary_path, results)
    (walk_forward_dir / "summary.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    selected_params_path = walk_forward_dir / "selected_params_by_window.csv"
    _write_sweep_summary(selected_params_path, selected_params_rows)
    (walk_forward_dir / "selected_params_by_window.json").write_text(json.dumps(selected_params_rows, indent=2), encoding="utf-8")
    stability_rows = _aggregate_selected_config_stability(results)
    _write_sweep_summary(walk_forward_dir / "selected_config_stability.csv", stability_rows)
    (walk_forward_dir / "selected_config_stability.json").write_text(json.dumps(stability_rows, indent=2), encoding="utf-8")
    leaderboard_rows = _build_walk_forward_leaderboard(stability_rows, walk_forward_config)
    _write_sweep_summary(walk_forward_dir / "leaderboard.csv", leaderboard_rows)
    (walk_forward_dir / "leaderboard.json").write_text(json.dumps(leaderboard_rows, indent=2), encoding="utf-8")
    recommended_config = _extract_recommended_config(leaderboard_rows)
    (walk_forward_dir / "recommended_config.json").write_text(json.dumps(recommended_config, indent=2), encoding="utf-8")
    oos_summary = _summarize_daily_return_rows(oos_daily_rows)
    oos_summary["windows"] = float(len(results))
    oos_summary["test_start"] = results[0]["test_start"]
    oos_summary["test_end"] = results[-1]["test_end"]
    _write_sweep_summary(walk_forward_dir / "oos_summary.csv", [oos_summary])
    (walk_forward_dir / "oos_summary.json").write_text(json.dumps(oos_summary, indent=2), encoding="utf-8")
    _write_walk_forward_daily_returns(walk_forward_dir / "oos_portfolio_daily_returns.csv", oos_daily_rows)
    return summary_path


def load_recommended_strategy_overrides(config: Config, recommendation_path: str | Path | None = None) -> dict:
    resolved_path = _resolve_recommended_config_path(config, recommendation_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"recommended config not found: {resolved_path}")
    with resolved_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    strategy_overrides = payload.get("strategy_overrides", {})
    if not isinstance(strategy_overrides, dict) or not strategy_overrides:
        raise ValueError(f"recommended config does not contain strategy_overrides: {resolved_path}")
    return strategy_overrides


def apply_recommended_config(config: Config, recommendation_path: str | Path | None = None) -> Config:
    return _with_strategy_overrides(config, load_recommended_strategy_overrides(config, recommendation_path))


def write_applied_recommended_config(
    config: Config,
    applied_config: Config,
    recommendation_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> Path:
    destination = _resolve_applied_config_output_path(config, output_path)
    ensure_directory(destination.parent)
    payload = deepcopy(applied_config.raw)
    payload["recommended_config"] = {
        "source_path": str(_resolve_recommended_config_path(config, recommendation_path)),
        "applied_output_path": str(destination),
    }
    destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return destination


def _expand_strategy_grid(strategy_grid: dict) -> list[dict]:
    keys = list(strategy_grid.keys())
    if not keys:
        return []
    values = []
    for key in keys:
        raw_values = strategy_grid[key]
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError(f"sweep.strategy_grid[{key}] must be a non-empty list")
        values.append(raw_values)
    return [dict(zip(keys, combo, strict=True)) for combo in itertools.product(*values)]


def _with_strategy_overrides(config: Config, overrides: dict) -> Config:
    return config.with_strategy_overrides(**overrides)


def _resolve_recommended_config_path(config: Config, recommendation_path: str | Path | None) -> Path:
    if recommendation_path is not None:
        return config.resolve_path(str(recommendation_path))
    output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
    return output_dir / "walk_forward" / "recommended_config.json"


def _resolve_applied_config_output_path(config: Config, output_path: str | Path | None) -> Path:
    if output_path is not None:
        return config.resolve_path(str(output_path))
    output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
    return output_dir / "applied_recommended_config.json"


def _with_strategy_period(config: Config, start_date: date, end_date: date) -> Config:
    return _with_strategy_overrides(
        config,
        {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )


def _resolve_walk_forward_windows(config: Config) -> list[dict[str, date]]:
    walk_forward_config = config.walk_forward
    explicit_windows = walk_forward_config.get("windows", [])
    if explicit_windows:
        return [
            {
                "train_start": parse_date(window["train_start"]),
                "train_end": parse_date(window["train_end"]),
                "test_start": parse_date(window["test_start"]),
                "test_end": parse_date(window["test_end"]),
            }
            for window in explicit_windows
        ]

    strategy = config.strategy
    strategy_start = parse_date(str(strategy["start_date"]))
    strategy_end = parse_date(str(strategy["end_date"]))
    train_months = int(walk_forward_config.get("train_months", 12))
    test_months = int(walk_forward_config.get("test_months", 3))
    step_months = int(walk_forward_config.get("step_months", test_months))
    if train_months <= 0 or test_months <= 0 or step_months <= 0:
        raise ValueError("walk_forward train_months, test_months, and step_months must be positive")

    windows: list[dict[str, date]] = []
    train_start = strategy_start
    while True:
        train_end = _add_months(train_start, train_months) - timedelta(days=1)
        test_start = train_end + timedelta(days=1)
        test_end = _add_months(test_start, test_months) - timedelta(days=1)
        if test_start > strategy_end:
            break
        windows.append(
            {
                "train_start": train_start,
                "train_end": min(train_end, strategy_end),
                "test_start": test_start,
                "test_end": min(test_end, strategy_end),
            }
        )
        next_train_start = _add_months(train_start, step_months)
        if next_train_start > strategy_end:
            break
        train_start = next_train_start
    return windows


def _add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def _run_backtest(config: Config, output_dir: Path) -> dict[str, float]:
    prepared = PreparedDataCache(config).load_or_build().prepared_data
    costs = resolve_backtest_costs(config.strategy)
    return Backtester(
        prepared,
        MultiSignalStrategy(config.strategy),
        output_dir=output_dir,
        **costs,
    ).run()


def _select_best_result(rows: list[dict], selection_metric: str) -> dict:
    if not rows:
        raise ValueError("walk-forward selection received no training runs")
    return sorted(
        rows,
        key=lambda row: (
            row.get(selection_metric, 0.0),
            row.get("active_total_return", 0.0),
            row.get("sharpe", 0.0),
        ),
        reverse=True,
    )[0]


def _load_daily_return_rows(path: Path, window_id: str) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                {
                    "window_id": window_id,
                    "date": row["date"],
                    "gross_return": float(row["gross_return"]),
                    "net_return": float(row["net_return"]),
                    "benchmark_return": float(row["benchmark_return"]),
                    "active_return": float(row["active_return"]),
                    "exposure": float(row["exposure"]),
                    "cash_weight": float(row.get("cash_weight", 0.0)),
                    "cash_carry": float(row.get("cash_carry", 0.0)),
                    "holdings": float(row["holdings"]),
                    "turnover": float(row["turnover"]),
                    "cash_drag": float(row.get("cash_drag", 0.0)),
                    "commission_cost": float(row.get("commission_cost", 0.0)),
                    "slippage_cost": float(row.get("slippage_cost", 0.0)),
                    "transaction_cost": float(row["transaction_cost"]),
                    "short_borrow_cost": float(row.get("short_borrow_cost", 0.0)),
                }
            )
    return rows


def _summarize_daily_return_rows(rows: list[dict]) -> dict[str, float]:
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
    ordered_rows = sorted(rows, key=lambda row: row["date"])
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
    for row in ordered_rows:
        gross_equity *= 1.0 + row["gross_return"]
        equity *= 1.0 + row["net_return"]
        benchmark_equity *= 1.0 + row["benchmark_return"]
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)
        returns.append(row["net_return"])
        active_returns.append(row["active_return"])
        turnovers.append(row["turnover"])
        cash_weights.append(row.get("cash_weight", 0.0))
        cash_carries.append(row.get("cash_carry", 0.0))
        cash_drags.append(row.get("cash_drag", 0.0))
        transaction_costs.append(row["transaction_cost"])
        short_borrow_costs.append(row.get("short_borrow_cost", 0.0))
        commission_costs.append(row.get("commission_cost", 0.0))
        slippage_costs.append(row.get("slippage_cost", 0.0))
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
    years = max(len(ordered_rows) / 252.0, 1 / 252.0)
    cagr = equity ** (1.0 / years) - 1.0
    return {
        "days": float(len(ordered_rows)),
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


def _write_walk_forward_daily_returns(path: Path, rows: list[dict]) -> None:
    ordered_rows = sorted(rows, key=lambda row: (row["date"], row["window_id"]))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "window_id",
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
        for row in ordered_rows:
            writer.writerow(
                {
                    "window_id": row["window_id"],
                    "date": row["date"],
                    "gross_return": _format_value(row["gross_return"]),
                    "net_return": _format_value(row["net_return"]),
                    "benchmark_return": _format_value(row["benchmark_return"]),
                    "active_return": _format_value(row["active_return"]),
                    "exposure": _format_value(row["exposure"]),
                    "cash_weight": _format_value(row.get("cash_weight", 0.0)),
                    "cash_carry": _format_value(row.get("cash_carry", 0.0)),
                    "holdings": _format_value(row["holdings"]),
                    "turnover": _format_value(row["turnover"]),
                    "cash_drag": _format_value(row.get("cash_drag", 0.0)),
                    "commission_cost": _format_value(row.get("commission_cost", 0.0)),
                    "slippage_cost": _format_value(row.get("slippage_cost", 0.0)),
                    "transaction_cost": _format_value(row["transaction_cost"]),
                    "short_borrow_cost": _format_value(row.get("short_borrow_cost", 0.0)),
                }
            )


def _aggregate_selected_config_stability(rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        signature = _parameter_signature(row)
        grouped.setdefault(signature, []).append(row)
    total_windows = len(rows)
    aggregated_rows: list[dict] = []
    for signature, grouped_rows in grouped.items():
        sample = grouped_rows[0]
        param_fields = {key: value for key, value in sample.items() if key.startswith("param_")}
        aggregated = {
            "config_id": signature,
            "selected_windows": float(len(grouped_rows)),
            "selection_rate": float(len(grouped_rows)) / float(total_windows) if total_windows else 0.0,
            **param_fields,
        }
        metric_prefixes = ("train_", "test_")
        metric_keys = sorted(
            {
                key
                for row in grouped_rows
                for key in row
                if any(key.startswith(prefix) for prefix in metric_prefixes)
            }
        )
        for key in metric_keys:
            values = [float(row[key]) for row in grouped_rows if isinstance(row.get(key), (int, float))]
            if values:
                aggregated[f"avg_{key}"] = sum(values) / len(values)
                aggregated[f"median_{key}"] = _median(values)
                aggregated[f"worst_{key}"] = min(values)
        test_active_values = [
            float(row["test_active_total_return"])
            for row in grouped_rows
            if isinstance(row.get("test_active_total_return"), (int, float))
        ]
        if test_active_values:
            positive_window_rate = sum(1 for value in test_active_values if value > 0.0) / len(test_active_values)
            aggregated["positive_window_rate"] = positive_window_rate
            aggregated["consistency_score"] = aggregated["selection_rate"] * positive_window_rate
        else:
            aggregated["positive_window_rate"] = 0.0
            aggregated["consistency_score"] = 0.0
        aggregated_rows.append(aggregated)
    return sorted(
        aggregated_rows,
        key=lambda row: (
            row.get("selected_windows", 0.0),
            row.get("consistency_score", 0.0),
            row.get("avg_test_information_ratio", 0.0),
            row.get("avg_test_active_total_return", 0.0),
        ),
        reverse=True,
    )


def _build_walk_forward_leaderboard(stability_rows: list[dict], walk_forward_config: dict) -> list[dict]:
    if not stability_rows:
        return []
    top_n = int(walk_forward_config.get("leaderboard_top_n", 5))
    min_selection_rate = float(walk_forward_config.get("leaderboard_min_selection_rate", 0.0))
    min_positive_window_rate = float(walk_forward_config.get("leaderboard_min_positive_window_rate", 0.0))
    sort_by = str(walk_forward_config.get("leaderboard_sort_by", "consistency_score")).strip() or "consistency_score"
    filtered_rows = [
        row
        for row in stability_rows
        if row.get("selection_rate", 0.0) >= min_selection_rate
        and row.get("positive_window_rate", 0.0) >= min_positive_window_rate
    ]
    ranked_rows = sorted(
        filtered_rows,
        key=lambda row: (
            row.get(sort_by, 0.0),
            row.get("avg_test_information_ratio", 0.0),
            row.get("avg_test_active_total_return", 0.0),
            row.get("selected_windows", 0.0),
        ),
        reverse=True,
    )
    return [
        {
            "rank": float(index),
            "leaderboard_sort_by": sort_by,
            **row,
        }
        for index, row in enumerate(ranked_rows[:top_n], start=1)
    ]


def _extract_recommended_config(leaderboard_rows: list[dict]) -> dict:
    if not leaderboard_rows:
        return {}
    best_row = leaderboard_rows[0]
    strategy_overrides = {
        key.removeprefix("param_"): value
        for key, value in best_row.items()
        if key.startswith("param_")
    }
    return {
        "rank": best_row.get("rank", 1.0),
        "config_id": best_row.get("config_id", ""),
        "leaderboard_sort_by": best_row.get("leaderboard_sort_by", "consistency_score"),
        "selection_rate": best_row.get("selection_rate", 0.0),
        "positive_window_rate": best_row.get("positive_window_rate", 0.0),
        "consistency_score": best_row.get("consistency_score", 0.0),
        "avg_test_information_ratio": best_row.get("avg_test_information_ratio", 0.0),
        "avg_test_active_total_return": best_row.get("avg_test_active_total_return", 0.0),
        "strategy_overrides": strategy_overrides,
    }


def _parameter_signature(row: dict) -> str:
    parameter_items = sorted(
        (key, row[key])
        for key in row
        if key.startswith("param_")
    )
    return json.dumps(parameter_items, separators=(",", ":"), sort_keys=False)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def _write_sweep_summary(path: Path, rows: list[dict]) -> None:
    fieldnames = _summary_fieldnames(rows)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: _format_value(row.get(key))
                    for key in fieldnames
                }
            )


def _summary_fieldnames(rows: list[dict]) -> list[str]:
    present_keys = {
        key
        for row in rows
        for key in row
    }
    preferred = [
        "rank",
        "run_id",
        "window_id",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "selection_metric",
        "selected_run_id",
        "selected_metric_value",
        "information_ratio",
        "active_total_return",
        "sharpe",
        "total_return",
        "gross_total_return",
        "benchmark_total_return",
        "cagr",
        "max_drawdown",
        "average_turnover",
        "total_transaction_cost",
        "total_short_borrow_cost",
        "total_commission_cost",
        "total_slippage_cost",
        "average_transaction_cost",
        "average_short_borrow_cost",
        "transaction_cost_drag",
        "days",
    ]
    preferred_fields = [key for key in preferred if key in present_keys]
    strategy_keys = sorted(present_keys - set(preferred_fields))
    return preferred_fields + strategy_keys


def _format_value(value: object) -> object:
    if isinstance(value, float):
        return f"{value:.8f}"
    return value


_SUMMARY_KEYS = {
    "days",
    "total_return",
    "gross_total_return",
    "benchmark_total_return",
    "active_total_return",
    "cagr",
    "sharpe",
    "information_ratio",
    "max_drawdown",
    "average_turnover",
    "total_transaction_cost",
    "total_short_borrow_cost",
    "total_commission_cost",
    "total_slippage_cost",
    "average_transaction_cost",
    "average_short_borrow_cost",
    "transaction_cost_drag",
}
