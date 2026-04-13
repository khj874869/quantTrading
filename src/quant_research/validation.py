from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from .config import Config
from .pipeline import DataPipeline, FeaturePanel, SourceData
from .utils import ensure_directory, month_end, pct_change, safe_div, write_csv_dicts


FACTOR_FIELDS = [
    "book_to_market",
    "roa",
    "asset_growth",
    "cash_flow_ratio",
    "revision",
    "dispersion",
    "surprise",
    "patent_intensity",
    "citation_intensity",
]


class DataValidator:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.pipeline = DataPipeline(config)

    def run(self) -> list[Path]:
        sources = self.pipeline.load_sources()
        feature_panel = self.pipeline.build_feature_panel(sources)
        summary, rebalance_rows = self._build_validation_report(sources, feature_panel)
        output_dir = self.config.resolve_path(self.config.paths.get("output_dir", "output"))
        ensure_directory(output_dir)

        summary_json_path = output_dir / "validation_summary.json"
        summary_csv_path = output_dir / "validation_summary.csv"
        rebalances_csv_path = output_dir / "validation_rebalances.csv"

        summary_json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        write_csv_dicts(summary_csv_path, [self._flatten_summary(summary)])
        write_csv_dicts(rebalances_csv_path, rebalance_rows)
        return [summary_json_path, summary_csv_path, rebalances_csv_path]

    def _build_validation_report(
        self,
        sources: SourceData,
        feature_panel: FeaturePanel,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        permno_to_symbol = self.pipeline._reverse_link(sources.ibes_links)
        rebalance_stats: dict = defaultdict(self._empty_rebalance_stats)
        overall = self._empty_rebalance_stats()

        for gvkey, reports in sources.fundamentals.items():
            for report_index, report in enumerate(reports):
                signal_date = self.pipeline._signal_date(report)
                rebalance_date = month_end(signal_date)
                if self.pipeline.start_date and rebalance_date < self.pipeline.start_date:
                    continue
                if self.pipeline.end_date and rebalance_date > self.pipeline.end_date:
                    continue

                stats = rebalance_stats[rebalance_date]
                self._increment(stats, overall, "report_count")

                linked_candidates = sources.ccm_links.get(gvkey)
                if not linked_candidates:
                    self._increment(stats, overall, "missing_link_count")
                    continue

                permno = self.pipeline._match_linked_id(linked_candidates, rebalance_date, "permno")
                if not permno:
                    self._increment(stats, overall, "unmatched_link_count")
                    continue
                self._increment(stats, overall, "linked_report_count")

                price_rows = sources.prices.get(permno)
                if not price_rows:
                    self._increment(stats, overall, "missing_price_history_count")
                    continue

                price_snapshot = self.pipeline._latest_before(price_rows, rebalance_date)
                if not price_snapshot:
                    self._increment(stats, overall, "missing_price_snapshot_count")
                    continue
                self._increment(stats, overall, "priced_report_count")

                market_cap = price_snapshot["prc"] * price_snapshot["shrout"] * 1000.0
                ticker = permno_to_symbol.get(permno)
                ibes_rows = sources.ibes_summary.get(ticker, []) if ticker else []
                ibes_snapshot = self.pipeline._latest_before(ibes_rows, rebalance_date) if ticker else None
                ibes_prev = self.pipeline._previous_row(ibes_rows, ibes_snapshot) if ibes_snapshot else None
                surprise_snapshot = self.pipeline._latest_before(sources.ibes_surprise.get(ticker, []), rebalance_date) if ticker else None
                patent_snapshot = self.pipeline._sum_patents_last_year(sources.patents.get(gvkey, []), rebalance_date)
                _ = self.pipeline._grade_pulse(sources.grades.get(ticker or "", []), rebalance_date)
                prior_report = reports[report_index - 4] if report_index >= 4 else None

                raw_factor_values = {
                    "book_to_market": safe_div(report["ceqq"], market_cap),
                    "roa": safe_div(report["ibq"], report["atq"]),
                    "asset_growth": pct_change(report["atq"], prior_report["atq"] if prior_report else None),
                    "cash_flow_ratio": safe_div(report["oancfy"], report["atq"]),
                    "revision": pct_change(
                        ibes_snapshot["meanest"] if ibes_snapshot else None,
                        ibes_prev["meanest"] if ibes_prev else None,
                    ),
                    "dispersion": safe_div(
                        ibes_snapshot["stdev"] if ibes_snapshot else None,
                        abs(ibes_snapshot["meanest"]) if ibes_snapshot and ibes_snapshot["meanest"] else None,
                    ),
                    "surprise": surprise_snapshot["surpct"] if surprise_snapshot and surprise_snapshot["surpct"] is not None else None,
                    "patent_intensity": safe_div(patent_snapshot["patent_count"], report["saleq"]),
                    "citation_intensity": safe_div(patent_snapshot["citation_count"], report["saleq"]),
                }
                self._increment(stats, overall, "factor_candidate_count")
                for factor_name, factor_value in raw_factor_values.items():
                    if factor_value is None:
                        self._increment(stats, overall, f"{factor_name}_missing_count")

                below_price_floor = price_snapshot["prc"] < self.pipeline.config.strategy.get("min_price", 5.0)
                below_market_cap_floor = market_cap < self.pipeline.config.strategy.get("min_market_cap", 100_000_000.0)
                if below_price_floor:
                    self._increment(stats, overall, "price_filter_drop_count")
                if below_market_cap_floor:
                    self._increment(stats, overall, "market_cap_filter_drop_count")
                if below_price_floor or below_market_cap_floor:
                    continue

                self._increment(stats, overall, "final_universe_count")
                if self.pipeline._can_estimate_beta(permno, rebalance_date, feature_panel.returns_by_date, feature_panel.benchmark_by_date):
                    self._increment(stats, overall, "beta_estimated_count")

        rebalance_rows = [
            self._rebalance_row(rebalance_date, stats)
            for rebalance_date, stats in sorted(rebalance_stats.items())
        ]
        summary = self._summary_payload(overall, rebalance_rows)
        return summary, rebalance_rows

    def _summary_payload(self, overall: dict[str, float], rebalance_rows: list[dict[str, object]]) -> dict[str, object]:
        factor_missing_rates = {
            factor_name: self._rate(overall.get(f"{factor_name}_missing_count", 0.0), overall.get("factor_candidate_count", 0.0))
            for factor_name in FACTOR_FIELDS
        }
        rebalance_dates = [str(row["rebalance_date"]) for row in rebalance_rows]
        return {
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "start_date": self.pipeline.start_date.isoformat() if self.pipeline.start_date else None,
            "end_date": self.pipeline.end_date.isoformat() if self.pipeline.end_date else None,
            "benchmark_mode": self.pipeline.benchmark_mode,
            "universe_rules": {
                "min_price": float(self.pipeline.config.strategy.get("min_price", 5.0)),
                "min_market_cap": float(self.pipeline.config.strategy.get("min_market_cap", 100_000_000.0)),
                "min_avg_dollar_volume": float(self.pipeline.config.strategy.get("min_avg_dollar_volume", 0.0)),
                "universe_include_sectors": sorted(self.pipeline.universe_include_sectors),
                "universe_exclude_sectors": sorted(self.pipeline.universe_exclude_sectors),
                "universe_top_n_by_market_cap": self.pipeline.universe_top_n_by_market_cap,
            },
            "rebalance_count": len(rebalance_rows),
            "reports_in_window": int(overall["report_count"]),
            "linked_reports": int(overall["linked_report_count"]),
            "link_match_rate": self._rate(overall["linked_report_count"], overall["report_count"]),
            "missing_link_count": int(overall["missing_link_count"]),
            "unmatched_link_count": int(overall["unmatched_link_count"]),
            "priced_reports": int(overall["priced_report_count"]),
            "price_snapshot_rate": self._rate(overall["priced_report_count"], overall["linked_report_count"]),
            "missing_price_history_count": int(overall["missing_price_history_count"]),
            "missing_price_snapshot_count": int(overall["missing_price_snapshot_count"]),
            "factor_candidate_count": int(overall["factor_candidate_count"]),
            "final_universe_count": int(overall["final_universe_count"]),
            "universe_survival_rate": self._rate(overall["final_universe_count"], overall["priced_report_count"]),
            "price_filter_drop_count": int(overall["price_filter_drop_count"]),
            "market_cap_filter_drop_count": int(overall["market_cap_filter_drop_count"]),
            "beta_estimated_count": int(overall["beta_estimated_count"]),
            "beta_coverage_rate": self._rate(overall["beta_estimated_count"], overall["final_universe_count"]),
            "factor_missing_rates": factor_missing_rates,
            "rebalance_dates": rebalance_dates,
        }

    def _rebalance_row(self, rebalance_date: object, stats: dict[str, float]) -> dict[str, object]:
        row = {
            "rebalance_date": rebalance_date.isoformat(),
            "report_count": int(stats["report_count"]),
            "linked_report_count": int(stats["linked_report_count"]),
            "link_match_rate": self._rate(stats["linked_report_count"], stats["report_count"]),
            "missing_link_count": int(stats["missing_link_count"]),
            "unmatched_link_count": int(stats["unmatched_link_count"]),
            "priced_report_count": int(stats["priced_report_count"]),
            "price_snapshot_rate": self._rate(stats["priced_report_count"], stats["linked_report_count"]),
            "missing_price_history_count": int(stats["missing_price_history_count"]),
            "missing_price_snapshot_count": int(stats["missing_price_snapshot_count"]),
            "factor_candidate_count": int(stats["factor_candidate_count"]),
            "final_universe_count": int(stats["final_universe_count"]),
            "universe_survival_rate": self._rate(stats["final_universe_count"], stats["priced_report_count"]),
            "price_filter_drop_count": int(stats["price_filter_drop_count"]),
            "market_cap_filter_drop_count": int(stats["market_cap_filter_drop_count"]),
            "beta_estimated_count": int(stats["beta_estimated_count"]),
            "beta_coverage_rate": self._rate(stats["beta_estimated_count"], stats["final_universe_count"]),
        }
        for factor_name in FACTOR_FIELDS:
            row[f"{factor_name}_missing_rate"] = self._rate(
                stats[f"{factor_name}_missing_count"],
                stats["factor_candidate_count"],
            )
        return row

    def _flatten_summary(self, summary: dict[str, object]) -> dict[str, object]:
        row = {
            "generated_at": summary["generated_at"],
            "start_date": summary["start_date"],
            "end_date": summary["end_date"],
            "rebalance_count": summary["rebalance_count"],
            "reports_in_window": summary["reports_in_window"],
            "linked_reports": summary["linked_reports"],
            "link_match_rate": summary["link_match_rate"],
            "missing_link_count": summary["missing_link_count"],
            "unmatched_link_count": summary["unmatched_link_count"],
            "priced_reports": summary["priced_reports"],
            "price_snapshot_rate": summary["price_snapshot_rate"],
            "missing_price_history_count": summary["missing_price_history_count"],
            "missing_price_snapshot_count": summary["missing_price_snapshot_count"],
            "factor_candidate_count": summary["factor_candidate_count"],
            "final_universe_count": summary["final_universe_count"],
            "universe_survival_rate": summary["universe_survival_rate"],
            "price_filter_drop_count": summary["price_filter_drop_count"],
            "market_cap_filter_drop_count": summary["market_cap_filter_drop_count"],
            "beta_estimated_count": summary["beta_estimated_count"],
            "beta_coverage_rate": summary["beta_coverage_rate"],
        }
        factor_missing_rates = summary.get("factor_missing_rates", {})
        if isinstance(factor_missing_rates, dict):
            for factor_name in FACTOR_FIELDS:
                row[f"{factor_name}_missing_rate"] = factor_missing_rates.get(factor_name, 0.0)
        return row

    def _empty_rebalance_stats(self) -> dict[str, float]:
        stats = {
            "report_count": 0.0,
            "linked_report_count": 0.0,
            "missing_link_count": 0.0,
            "unmatched_link_count": 0.0,
            "priced_report_count": 0.0,
            "missing_price_history_count": 0.0,
            "missing_price_snapshot_count": 0.0,
            "factor_candidate_count": 0.0,
            "final_universe_count": 0.0,
            "price_filter_drop_count": 0.0,
            "market_cap_filter_drop_count": 0.0,
            "beta_estimated_count": 0.0,
        }
        for factor_name in FACTOR_FIELDS:
            stats[f"{factor_name}_missing_count"] = 0.0
        return stats

    def _increment(self, stats: dict[str, float], overall: dict[str, float], key: str) -> None:
        stats[key] += 1.0
        overall[key] += 1.0

    def _rate(self, numerator: float, denominator: float) -> float:
        if denominator <= 0.0:
            return 0.0
        return numerator / denominator
