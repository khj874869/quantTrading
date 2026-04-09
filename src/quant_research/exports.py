from __future__ import annotations

import csv
from pathlib import Path

from .pipeline import PreparedData
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
