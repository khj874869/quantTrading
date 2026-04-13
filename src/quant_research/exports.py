from __future__ import annotations

import csv
import json
from pathlib import Path

from .pipeline import PreparedData
from .strategy import MultiSignalStrategy
from .utils import ensure_directory


def export_rebalance_signals(prepared_data: PreparedData, output_dir: Path) -> Path:
    ensure_directory(output_dir)
    output_path = output_dir / "rebalance_signals.csv"
    rows = []
    for rebalance_date, features in sorted(prepared_data.features_by_rebalance.items()):
        ranked = sorted(features, key=lambda item: item["composite_score"], reverse=True)
        for rank, row in enumerate(ranked, start=1):
            rows.append(
                {
                    "rebalance_date": rebalance_date.isoformat(),
                    "signal_date": row["signal_date"].isoformat(),
                    "rank": rank,
                    "permno": row["permno"],
                    "sector": row["sector"],
                    "composite_score": f"{row['composite_score']:.8f}",
                    "risk_adjusted_score": f"{row.get('risk_adjusted_score', row['composite_score']):.8f}",
                    "risk_penalty_base": f"{row.get('risk_penalty_base', 0.0):.8f}",
                    "risk_regime_multiplier": f"{row.get('risk_regime_multiplier', 1.0):.8f}",
                    "risk_penalty": f"{row.get('risk_penalty', 0.0):.8f}",
                    "market_cap": f"{row['market_cap']:.2f}",
                    "avg_dollar_volume": f"{row.get('avg_dollar_volume', 0.0):.2f}",
                    "liquidity_ratio": f"{row.get('liquidity_ratio', 0.0):.8f}",
                    "book_to_market": f"{row['book_to_market']:.8f}",
                    "roa": f"{row['roa']:.8f}",
                    "asset_growth": f"{row['asset_growth']:.8f}",
                    "cash_flow_ratio": f"{row['cash_flow_ratio']:.8f}",
                    "revision": f"{row['revision']:.8f}",
                    "dispersion": f"{row['dispersion']:.8f}",
                    "surprise": f"{row['surprise']:.8f}",
                    "patent_intensity": f"{row['patent_intensity']:.8f}",
                    "citation_intensity": f"{row['citation_intensity']:.8f}",
                    "net_upgrades": f"{row['net_upgrades']:.8f}",
                    "beta": f"{row.get('beta', 1.0):.8f}",
                    "downside_beta": f"{row.get('downside_beta', row.get('beta', 1.0)):.8f}",
                    "downside_beta_z": f"{row.get('downside_beta_z', 0.0):.8f}",
                    "idio_vol": f"{row.get('idio_vol', 0.0):.8f}",
                    "idio_vol_z": f"{row.get('idio_vol_z', 0.0):.8f}",
                    "macro_score": f"{row['macro_score']:.8f}",
                    "vix": f"{row['vix']:.8f}",
                }
            )
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["rebalance_date", "signal_date", "rank", "permno", "sector"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def export_universe_snapshot(prepared_data: PreparedData, output_dir: Path, benchmark_mode: str) -> Path:
    ensure_directory(output_dir)
    output_path = output_dir / "universe_snapshot.csv"
    rows = []
    for rebalance_date, features in sorted(prepared_data.features_by_rebalance.items()):
        ranked = sorted(
            features,
            key=lambda item: (item.get("market_cap", 0.0), item.get("avg_dollar_volume", 0.0), item["permno"]),
            reverse=True,
        )
        universe_size = len(ranked)
        equal_weight = (1.0 / universe_size) if benchmark_mode == "equal_weight_universe" and universe_size > 0 else 0.0
        for market_cap_rank, row in enumerate(ranked, start=1):
            rows.append(
                {
                    "rebalance_date": rebalance_date.isoformat(),
                    "permno": row["permno"],
                    "sector": row.get("sector", "UNKNOWN"),
                    "market_cap_rank": market_cap_rank,
                    "universe_size": universe_size,
                    "benchmark_mode": benchmark_mode,
                    "benchmark_weight_hint": f"{equal_weight:.8f}" if equal_weight > 0.0 else "",
                    "signal_date": row["signal_date"].isoformat(),
                    "market_cap": f"{row.get('market_cap', 0.0):.2f}",
                    "avg_dollar_volume": f"{row.get('avg_dollar_volume', 0.0):.2f}",
                    "liquidity_ratio": f"{row.get('liquidity_ratio', 0.0):.8f}",
                    "composite_score": f"{row.get('composite_score', 0.0):.8f}",
                    "risk_adjusted_score": f"{row.get('risk_adjusted_score', row.get('composite_score', 0.0)):.8f}",
                    "beta": f"{row.get('beta', 1.0):.8f}",
                    "downside_beta": f"{row.get('downside_beta', row.get('beta', 1.0)):.8f}",
                    "idio_vol": f"{row.get('idio_vol', 0.0):.8f}",
                }
            )
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(rows[0].keys()) if rows else ["rebalance_date", "permno", "sector", "benchmark_mode"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def export_order_blotter(
    prepared_data: PreparedData,
    strategy: MultiSignalStrategy,
    output_dir: Path,
    blotter_notional: float,
    order_type: str = "MOC",
) -> list[Path]:
    ensure_directory(output_dir)
    order_type_value = str(order_type).strip().upper() or "MOC"
    estimated_notional = max(float(blotter_notional), 0.0)
    all_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    previous_weights: dict[str, float] = {}
    latest_rebalance_date = None

    for rebalance_date in sorted(prepared_data.features_by_rebalance):
        features = prepared_data.features_by_rebalance[rebalance_date]
        portfolio = strategy.build_weights(rebalance_date, features, previous_weights=previous_weights)
        lookup = {row["permno"]: row for row in features}
        rebalance_rows: list[dict[str, object]] = []
        gross_buy_weight = 0.0
        gross_sell_weight = 0.0
        for permno in sorted(set(previous_weights) | set(portfolio.weights)):
            if permno == "__BENCH__":
                continue
            previous_weight = previous_weights.get(permno, 0.0)
            target_weight = portfolio.weights.get(permno, 0.0)
            order_weight = target_weight - previous_weight
            if abs(order_weight) <= 1e-12:
                continue
            feature = lookup.get(permno, {})
            price = float(feature.get("price", 0.0) or 0.0)
            order_notional = abs(order_weight) * estimated_notional
            estimated_shares = order_notional / price if price > 0.0 else 0.0
            if order_weight > 0.0:
                gross_buy_weight += order_weight
            else:
                gross_sell_weight += abs(order_weight)
            rebalance_rows.append(
                {
                    "rebalance_date": rebalance_date.isoformat(),
                    "signal_date": feature.get("signal_date").isoformat() if feature.get("signal_date") else "",
                    "permno": permno,
                    "sector": feature.get("sector", "UNKNOWN"),
                    "side": "BUY" if order_weight > 0.0 else "SELL",
                    "order_type": order_type_value,
                    "position_transition": _position_transition(previous_weight, target_weight),
                    "previous_weight": f"{previous_weight:.8f}",
                    "target_weight": f"{target_weight:.8f}",
                    "order_weight": f"{order_weight:.8f}",
                    "price": f"{price:.8f}" if price > 0.0 else "",
                    "estimated_notional": f"{order_notional:.2f}",
                    "estimated_shares": f"{estimated_shares:.4f}" if estimated_shares > 0.0 else "",
                    "risk_adjusted_score": f"{float(feature.get('risk_adjusted_score', feature.get('composite_score', 0.0))):.8f}",
                    "composite_score": f"{float(feature.get('composite_score', 0.0)):.8f}",
                    "avg_dollar_volume": f"{float(feature.get('avg_dollar_volume', 0.0)):.2f}",
                    "market_cap": f"{float(feature.get('market_cap', 0.0)):.2f}",
                }
            )
        if rebalance_rows:
            latest_rebalance_date = rebalance_date
            all_rows.extend(rebalance_rows)
            summary_rows.append(
                {
                    "rebalance_date": rebalance_date.isoformat(),
                    "order_count": len(rebalance_rows),
                    "gross_buy_weight": gross_buy_weight,
                    "gross_sell_weight": gross_sell_weight,
                    "gross_turnover": 0.5 * (gross_buy_weight + gross_sell_weight),
                    "estimated_buy_notional": gross_buy_weight * estimated_notional,
                    "estimated_sell_notional": gross_sell_weight * estimated_notional,
                    "estimated_turnover_notional": 0.5 * (gross_buy_weight + gross_sell_weight) * estimated_notional,
                }
            )
        previous_weights = {
            permno: weight
            for permno, weight in portfolio.weights.items()
            if permno != "__BENCH__"
        }

    order_blotter_path = output_dir / "order_blotter.csv"
    latest_order_blotter_path = output_dir / "order_blotter_latest.csv"
    summary_path = output_dir / "order_blotter_summary.json"
    _write_rows(
        order_blotter_path,
        all_rows,
        ["rebalance_date", "signal_date", "permno", "sector", "side", "order_type", "position_transition"],
    )
    latest_rows = [
        row
        for row in all_rows
        if latest_rebalance_date is not None and row["rebalance_date"] == latest_rebalance_date.isoformat()
    ]
    _write_rows(
        latest_order_blotter_path,
        latest_rows,
        ["rebalance_date", "signal_date", "permno", "sector", "side", "order_type", "position_transition"],
    )
    summary_payload = {
        "blotter_notional": estimated_notional,
        "order_type": order_type_value,
        "rebalance_count": len(summary_rows),
        "total_order_count": len(all_rows),
        "latest_rebalance_date": latest_rebalance_date.isoformat() if latest_rebalance_date else None,
        "rebalances": summary_rows,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    return [order_blotter_path, latest_order_blotter_path, summary_path]


def _write_rows(path: Path, rows: list[dict[str, object]], fallback_fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(rows[0].keys()) if rows else fallback_fieldnames
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _position_transition(previous_weight: float, target_weight: float) -> str:
    if abs(previous_weight) <= 1e-12 and abs(target_weight) > 1e-12:
        return "OPEN"
    if abs(previous_weight) > 1e-12 and abs(target_weight) <= 1e-12:
        return "CLOSE"
    if previous_weight * target_weight < 0.0:
        return "FLIP"
    if abs(target_weight) > abs(previous_weight):
        return "INCREASE"
    return "DECREASE"
