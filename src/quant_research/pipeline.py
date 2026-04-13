from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from math import exp, log
from pathlib import Path
from time import perf_counter

from .config import Config
from .utils import float_or_none, month_end, normalize_cross_section, parse_date, parse_optional_date, pct_change, read_csv_dicts, safe_div


@dataclass(slots=True)
class SourceData:
    fundamentals: dict[str, list[dict]]
    prices: dict[str, list[dict]]
    ccm_links: dict[str, list[dict]]
    ibes_links: dict[str, list[dict]]
    ibes_summary: dict[str, list[dict]]
    ibes_surprise: dict[str, list[dict]]
    patents: dict[str, list[dict]]
    macro: dict[str, dict[date, float]]
    grades: dict[str, list[dict]]
    factors: list[dict]


@dataclass(slots=True)
class PreparedData:
    features_by_rebalance: dict[date, list[dict]]
    returns_by_date: dict[date, dict[str, float]]
    benchmark_by_date: dict[date, float]
    risk_free_by_date: dict[date, float] = field(default_factory=dict)


@dataclass(slots=True)
class FeaturePanel:
    features_by_rebalance: dict[date, list[dict]]
    returns_by_date: dict[date, dict[str, float]]
    benchmark_by_date: dict[date, float]
    risk_free_by_date: dict[date, float] = field(default_factory=dict)


class DataPipeline:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.start_date = parse_optional_date(str(config.strategy.get("start_date", "")).strip())
        self.end_date = parse_optional_date(str(config.strategy.get("end_date", "")).strip())
        self.report_lag_days = int(config.strategy.get("report_lag_days", 45))
        self.liquidity_lookback_days = int(config.strategy.get("liquidity_lookback_days", 20))
        self.liquidity_fallback_adv_to_mcap = max(float(config.strategy.get("liquidity_fallback_adv_to_mcap", 0.02)), 1e-6)
        self.use_rdq = bool(config.strategy.get("use_rdq", True))
        self.beta_lookback_days = int(config.strategy.get("beta_lookback_days", 126))
        self.min_beta_observations = int(config.strategy.get("min_beta_observations", 60))
        self.beta_method = str(config.strategy.get("beta_method", "ols")).strip().lower() or "ols"
        self.beta_ewma_halflife_days = int(config.strategy.get("beta_ewma_halflife_days", 63))
        self.beta_shrinkage = float(config.strategy.get("beta_shrinkage", 0.0))
        self.beta_shrinkage_target = float(config.strategy.get("beta_shrinkage_target", 1.0))
        benchmark_mode = str(config.strategy.get("benchmark_mode", "ff_total_return")).strip().lower() or "ff_total_return"
        self.benchmark_mode = benchmark_mode if benchmark_mode in {"ff_total_return", "equal_weight_universe", "zero"} else "ff_total_return"
        self.risk_penalty_downside_beta_weight = float(config.strategy.get("risk_penalty_downside_beta_weight", 0.0))
        self.risk_penalty_idio_vol_weight = float(config.strategy.get("risk_penalty_idio_vol_weight", 0.0))
        self.feature_zscore_method = self._normalization_method(config.strategy.get("feature_zscore_method", "robust"))
        self.feature_winsor_quantile = self._normalization_quantile(config.strategy.get("feature_winsor_quantile", 0.05))
        self.risk_zscore_method = self._normalization_method(config.strategy.get("risk_zscore_method", "robust"))
        self.risk_winsor_quantile = self._normalization_quantile(config.strategy.get("risk_winsor_quantile", 0.05))
        self.regime_risk_scaling_enabled = bool(config.strategy.get("regime_risk_scaling_enabled", True))
        self.regime_vix_threshold = float(config.strategy.get("regime_vix_threshold", config.strategy.get("vix_de_risk_level", 30.0)))
        self.regime_vix_penalty_multiplier = float(config.strategy.get("regime_vix_penalty_multiplier", 1.5))
        self.regime_macro_threshold = float(config.strategy.get("regime_macro_threshold", 0.5))
        self.regime_macro_penalty_multiplier = float(config.strategy.get("regime_macro_penalty_multiplier", 1.5))
        self.regime_penalty_cap = float(config.strategy.get("regime_penalty_cap", 3.0))
        self.min_avg_dollar_volume = max(float(config.strategy.get("min_avg_dollar_volume", 0.0)), 0.0)
        self.universe_include_sectors = {
            str(value).strip()
            for value in config.strategy.get("universe_include_sectors", [])
            if str(value).strip()
        }
        self.universe_exclude_sectors = {
            str(value).strip()
            for value in config.strategy.get("universe_exclude_sectors", [])
            if str(value).strip()
        }
        self.universe_top_n_by_market_cap = max(int(config.strategy.get("universe_top_n_by_market_cap", 0)), 0)
        self.profile: dict[str, float] = {}

    def load(self) -> PreparedData:
        started = perf_counter()
        sources = self.load_sources()
        self.profile["load_sources_total_seconds"] = perf_counter() - started
        build_started = perf_counter()
        mark = build_started
        feature_panel = self.build_feature_panel(sources)
        self.profile["build_feature_panel_total_seconds"] = perf_counter() - mark
        mark = perf_counter()
        prepared = self.finalize_prepared_data(feature_panel)
        self.profile["finalize_prepared_total_seconds"] = perf_counter() - mark
        self.profile["build_prepared_total_seconds"] = perf_counter() - build_started
        self.profile["pipeline_total_seconds"] = perf_counter() - started
        return prepared

    def load_sources(self) -> SourceData:
        started = perf_counter()
        fundamentals = self._load_compustat()
        self.profile["load_compustat_seconds"] = perf_counter() - started
        mark = perf_counter()
        prices = self._load_crsp()
        self.profile["load_crsp_seconds"] = perf_counter() - mark
        mark = perf_counter()
        ccm_links = self._load_link_table(self.config.resolve("ccm_link"), "gvkey", "permno")
        ibes_links = self._load_link_table(self.config.resolve("ibes_link"), "ticker", "permno")
        ibes_summary = self._load_ibes_summary()
        ibes_surprise = self._load_ibes_surprise()
        patents = self._load_patents()
        macro = self._load_macro()
        grades = self._load_fmp_grades()
        factors = self._load_ff_factors()
        self.profile["load_auxiliary_seconds"] = perf_counter() - mark
        self.profile["load_sources_total_seconds"] = perf_counter() - started
        return SourceData(
            fundamentals=fundamentals,
            prices=prices,
            ccm_links=ccm_links,
            ibes_links=ibes_links,
            ibes_summary=ibes_summary,
            ibes_surprise=ibes_surprise,
            patents=patents,
            macro=macro,
            grades=grades,
            factors=factors,
        )

    def build_feature_panel(self, sources: SourceData) -> FeaturePanel:
        started = perf_counter()
        mark = perf_counter()
        returns = self._build_returns(sources.prices)
        risk_free = {row["date"]: row["rf"] for row in sources.factors}
        self.profile["build_market_series_seconds"] = perf_counter() - mark
        mark = perf_counter()
        features = self._build_security_features(
            fundamentals=sources.fundamentals,
            prices=sources.prices,
            ccm_links=sources.ccm_links,
            ibes_links=sources.ibes_links,
            ibes_summary=sources.ibes_summary,
            ibes_surprise=sources.ibes_surprise,
            patents=sources.patents,
            macro=sources.macro,
            grades=sources.grades,
        )
        benchmarks = self._build_benchmark_series(features, returns, sources.factors)
        self.profile["build_features_seconds"] = perf_counter() - mark
        self.profile["build_feature_panel_total_seconds"] = perf_counter() - started
        return FeaturePanel(
            features_by_rebalance=features,
            returns_by_date=returns,
            benchmark_by_date=benchmarks,
            risk_free_by_date=risk_free,
        )

    def finalize_prepared_data(self, feature_panel: FeaturePanel) -> PreparedData:
        started = perf_counter()
        features = {
            rebalance_date: [row.copy() for row in rows]
            for rebalance_date, rows in feature_panel.features_by_rebalance.items()
        }
        mark = perf_counter()
        self._attach_beta_estimates(features, feature_panel.returns_by_date, feature_panel.benchmark_by_date)
        self.profile["attach_beta_seconds"] = perf_counter() - mark
        mark = perf_counter()
        self._attach_risk_adjusted_scores(features)
        self.profile["attach_risk_penalty_seconds"] = perf_counter() - mark
        self.profile["finalize_prepared_total_seconds"] = perf_counter() - started
        return PreparedData(
            features_by_rebalance=features,
            returns_by_date=feature_panel.returns_by_date,
            benchmark_by_date=feature_panel.benchmark_by_date,
            risk_free_by_date=feature_panel.risk_free_by_date,
        )

    def build_prepared_data(self, sources: SourceData) -> PreparedData:
        started = perf_counter()
        feature_panel = self.build_feature_panel(sources)
        prepared = self.finalize_prepared_data(feature_panel)
        self.profile["build_prepared_total_seconds"] = perf_counter() - started
        return prepared

    def _load_compustat(self) -> dict[str, list[dict]]:
        rows = read_csv_dicts(self.config.resolve("compustat_quarterly"))
        by_gvkey: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            datadate = parse_date(row["datadate"])
            sic = (row.get("sic") or "").strip()
            gsector = (row.get("gsector") or "").strip()
            by_gvkey[row["gvkey"]].append(
                {
                    "date": datadate,
                    "rdq": parse_optional_date(row.get("rdq")),
                    "sector": gsector or sic[:2] or "UNKNOWN",
                    "atq": float_or_none(row.get("atq")),
                    "ltq": float_or_none(row.get("ltq")),
                    "ceqq": float_or_none(row.get("ceqq")),
                    "saleq": float_or_none(row.get("saleq")),
                    "ibq": float_or_none(row.get("ibq")),
                    "oancfy": float_or_none(row.get("oancfy")),
                }
            )
        for key in by_gvkey:
            by_gvkey[key].sort(key=lambda item: item["date"])
        return by_gvkey

    def _load_crsp(self) -> dict[str, list[dict]]:
        rows = read_csv_dicts(self.config.resolve("crsp_daily"))
        by_permno: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            trade_date = parse_date(row["date"])
            ret = float_or_none(row.get("ret")) or 0.0
            dlret = float_or_none(row.get("dlret")) or 0.0
            by_permno[row["permno"]].append(
                {
                    "date": trade_date,
                    "ret": (1.0 + ret) * (1.0 + dlret) - 1.0,
                    "prc": abs(float_or_none(row.get("prc")) or 0.0),
                    "shrout": float_or_none(row.get("shrout")) or 0.0,
                    "vol": float_or_none(row.get("vol")) or 0.0,
                }
            )
        for key in by_permno:
            by_permno[key].sort(key=lambda item: item["date"])
        return by_permno

    def _load_link_table(self, path: Path, left_key: str, right_key: str) -> dict[str, list[dict]]:
        rows = read_csv_dicts(path)
        mapping: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            mapping[row[left_key]].append(
                {
                    right_key: row[right_key],
                    "start": parse_date(row["linkdt"]),
                    "end": parse_date(row["linkenddt"]) if row.get("linkenddt") else date(2100, 1, 1),
                }
            )
        return mapping

    def _load_ibes_summary(self) -> dict[str, list[dict]]:
        rows = read_csv_dicts(self.config.resolve("ibes_summary"))
        by_ticker: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            if row.get("measure", "EPS").upper() != "EPS":
                continue
            stat_date = parse_date(row["statpers"])
            by_ticker[row["ticker"]].append(
                {
                    "date": stat_date,
                    "meanest": float_or_none(row.get("meanest")),
                    "stdev": float_or_none(row.get("stdev")),
                    "numest": float_or_none(row.get("numest")),
                }
            )
        for key in by_ticker:
            by_ticker[key].sort(key=lambda item: item["date"])
        return by_ticker

    def _load_ibes_surprise(self) -> dict[str, list[dict]]:
        rows = read_csv_dicts(self.config.resolve("ibes_surprise"))
        by_ticker: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            stat_date = parse_date(row["statpers"])
            by_ticker[row["ticker"]].append(
                {
                    "date": stat_date,
                    "actual": float_or_none(row.get("actual")),
                    "surprise": float_or_none(row.get("surprise")),
                    "surpct": float_or_none(row.get("surpct")),
                }
            )
        for key in by_ticker:
            by_ticker[key].sort(key=lambda item: item["date"])
        return by_ticker

    def _load_patents(self) -> dict[str, list[dict]]:
        rows = read_csv_dicts(self.config.resolve("kpss_patent"))
        by_gvkey: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            issue_date = parse_date(row["issue_date"])
            by_gvkey[row["gvkey"]].append(
                {
                    "date": issue_date,
                    "patent_count": float_or_none(row.get("patent_count")) or 0.0,
                    "citation_count": float_or_none(row.get("citation_count")) or 0.0,
                }
            )
        for key in by_gvkey:
            by_gvkey[key].sort(key=lambda item: item["date"])
        return by_gvkey

    def _load_macro(self) -> dict[str, dict[date, float]]:
        fred_rows = read_csv_dicts(self.config.resolve("fred_dgs10"))
        vix_rows = read_csv_dicts(self.config.resolve("cboe_vix"))
        dgs10 = {}
        for row in fred_rows:
            dgs10[parse_date(row["date"])] = float_or_none(row.get("value")) or 0.0
        vix = {}
        for row in vix_rows:
            date_key = parse_date(row.get("DATE", row.get("date", "")))
            close_value = row.get("CLOSE", row.get("close", row.get("Close", "")))
            vix[date_key] = float_or_none(close_value) or 0.0
        return {"dgs10": dgs10, "vix": vix}

    def _load_fmp_grades(self) -> dict[str, list[dict]]:
        rows = read_csv_dicts(self.config.resolve("fmp_grades"))
        by_symbol: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            published_raw = (row.get("publishedDate") or row.get("date") or "")[:10]
            if not published_raw:
                continue
            published = parse_date(published_raw)
            by_symbol[row["symbol"]].append(
                {
                    "date": published,
                    "action": (row.get("action") or "").lower(),
                    "newGrade": (row.get("newGrade") or "").lower(),
                    "previousGrade": (row.get("previousGrade") or "").lower(),
                }
            )
        for key in by_symbol:
            by_symbol[key].sort(key=lambda item: item["date"])
        return by_symbol

    def _load_ff_factors(self) -> list[dict]:
        rows = read_csv_dicts(self.config.resolve("ff_factors"))
        factors: list[dict] = []
        for row in rows:
            factor_date = parse_date(row["date"])
            if self.start_date and factor_date < self.start_date:
                continue
            if self.end_date and factor_date > self.end_date:
                continue
            factors.append(
                {
                    "date": factor_date,
                    "mktrf": (float_or_none(row.get("mktrf")) or 0.0) / 100.0,
                    "rf": (float_or_none(row.get("rf")) or 0.0) / 100.0,
                }
            )
        return factors

    def _build_security_features(
        self,
        fundamentals: dict[str, list[dict]],
        prices: dict[str, list[dict]],
        ccm_links: dict[str, list[dict]],
        ibes_links: dict[str, list[dict]],
        ibes_summary: dict[str, list[dict]],
        ibes_surprise: dict[str, list[dict]],
        patents: dict[str, list[dict]],
        macro: dict[str, dict[date, float]],
        grades: dict[str, list[dict]],
    ) -> dict[date, list[dict]]:
        features_by_rebalance: dict[date, list[dict]] = defaultdict(list)
        permno_to_symbol = self._reverse_link(ibes_links)
        for gvkey, reports in fundamentals.items():
            if gvkey not in ccm_links:
                continue
            for report_index, report in enumerate(reports):
                signal_date = self._signal_date(report)
                rebalance_date = month_end(signal_date)
                if self.start_date and rebalance_date < self.start_date:
                    continue
                if self.end_date and rebalance_date > self.end_date:
                    continue
                permno = self._match_linked_id(ccm_links[gvkey], rebalance_date, "permno")
                if not permno or permno not in prices:
                    continue
                price_snapshot = self._latest_before(prices[permno], rebalance_date)
                if not price_snapshot:
                    continue
                market_cap = price_snapshot["prc"] * price_snapshot["shrout"] * 1000.0
                avg_dollar_volume = self._average_dollar_volume(prices[permno], rebalance_date)
                if avg_dollar_volume is None or avg_dollar_volume <= 0.0:
                    avg_dollar_volume = market_cap * self.liquidity_fallback_adv_to_mcap
                liquidity_ratio = safe_div(avg_dollar_volume, market_cap) or self.liquidity_fallback_adv_to_mcap
                prior_report = reports[report_index - 4] if report_index >= 4 else None
                ticker = permno_to_symbol.get(permno)
                ibes_rows = ibes_summary.get(ticker, []) if ticker else []
                ibes_snapshot = self._latest_before(ibes_rows, rebalance_date) if ticker else None
                ibes_prev = self._previous_row(ibes_rows, ibes_snapshot) if ibes_snapshot else None
                surprise_snapshot = self._latest_before(ibes_surprise.get(ticker, []), rebalance_date) if ticker else None
                patent_snapshot = self._sum_patents_last_year(patents.get(gvkey, []), rebalance_date)
                grade_snapshot = self._grade_pulse(grades.get(ticker or "", []), rebalance_date)
                macro_snapshot = self._macro_snapshot(rebalance_date, macro)

                book_to_market = safe_div(report["ceqq"], market_cap)
                roa = safe_div(report["ibq"], report["atq"])
                asset_growth = pct_change(report["atq"], prior_report["atq"] if prior_report else None)
                cash_flow_ratio = safe_div(report["oancfy"], report["atq"])
                revision = pct_change(
                    ibes_snapshot["meanest"] if ibes_snapshot else None,
                    ibes_prev["meanest"] if ibes_prev else None,
                )
                dispersion = safe_div(
                    ibes_snapshot["stdev"] if ibes_snapshot else None,
                    abs(ibes_snapshot["meanest"]) if ibes_snapshot and ibes_snapshot["meanest"] else None,
                )

                row = {
                    "rebalance_date": rebalance_date,
                    "signal_date": signal_date,
                    "permno": permno,
                    "sector": report["sector"],
                    "market_cap": market_cap,
                    "avg_dollar_volume": avg_dollar_volume,
                    "liquidity_ratio": liquidity_ratio,
                    "price": price_snapshot["prc"],
                    "book_to_market": book_to_market or 0.0,
                    "roa": roa or 0.0,
                    "asset_growth": asset_growth or 0.0,
                    "cash_flow_ratio": cash_flow_ratio or 0.0,
                    "revision": revision or 0.0,
                    "dispersion": -(dispersion or 0.0),
                    "surprise": surprise_snapshot["surpct"] if surprise_snapshot and surprise_snapshot["surpct"] is not None else 0.0,
                    "patent_intensity": safe_div(patent_snapshot["patent_count"], report["saleq"]) or 0.0,
                    "citation_intensity": safe_div(patent_snapshot["citation_count"], report["saleq"]) or 0.0,
                    "net_upgrades": grade_snapshot,
                    "macro_score": macro_snapshot["macro_score"],
                    "vix": macro_snapshot["vix"],
                }
                features_by_rebalance[rebalance_date].append(row)

        for rebalance_date, rows in list(features_by_rebalance.items()):
            filtered = self._apply_universe_filters(rows)
            if not filtered:
                continue
            factor_names = [
                "book_to_market",
                "roa",
                "asset_growth",
                "cash_flow_ratio",
                "revision",
                "dispersion",
                "surprise",
                "patent_intensity",
                "citation_intensity",
                "net_upgrades",
            ]
            for factor_name in factor_names:
                scores = self._normalize_feature_values([row[factor_name] for row in filtered])
                for row, score in zip(filtered, scores, strict=True):
                    row[f"{factor_name}_z"] = score
            for row in filtered:
                row["composite_score"] = (
                    row["book_to_market_z"]
                    + row["roa_z"]
                    - row["asset_growth_z"]
                    + row["cash_flow_ratio_z"]
                    + row["revision_z"]
                    + row["dispersion_z"]
                    + row["surprise_z"]
                    + row["patent_intensity_z"]
                    + row["citation_intensity_z"]
                    + row["net_upgrades_z"]
                ) / 10.0
            features_by_rebalance[rebalance_date] = filtered
        return features_by_rebalance

    def _signal_date(self, report: dict) -> date:
        if self.use_rdq and report.get("rdq") is not None:
            return report["rdq"]
        return report["date"] + timedelta(days=self.report_lag_days)

    def _attach_beta_estimates(
        self,
        features_by_rebalance: dict[date, list[dict]],
        returns_by_date: dict[date, dict[str, float]],
        benchmark_by_date: dict[date, float],
    ) -> None:
        for rebalance_date, rows in features_by_rebalance.items():
            for row in rows:
                risk_metrics = self._estimate_risk_metrics(
                    permno=row["permno"],
                    rebalance_date=rebalance_date,
                    returns_by_date=returns_by_date,
                    benchmark_by_date=benchmark_by_date,
                )
                row.update(risk_metrics)

    def _estimate_beta(
        self,
        permno: str,
        rebalance_date: date,
        returns_by_date: dict[date, dict[str, float]],
        benchmark_by_date: dict[date, float],
    ) -> float:
        return self._estimate_risk_metrics(permno, rebalance_date, returns_by_date, benchmark_by_date)["beta"]

    def _estimate_risk_metrics(
        self,
        permno: str,
        rebalance_date: date,
        returns_by_date: dict[date, dict[str, float]],
        benchmark_by_date: dict[date, float],
    ) -> dict[str, float]:
        return_pairs = self._historical_return_pairs(permno, rebalance_date, returns_by_date, benchmark_by_date)
        if len(return_pairs) < self.min_beta_observations:
            return {"beta": 1.0, "downside_beta": 1.0, "idio_vol": 0.0}
        security_returns = [security_return for security_return, _ in return_pairs]
        benchmark_returns = [benchmark_return for _, benchmark_return in return_pairs]
        weights = self._beta_weights(len(security_returns))
        beta = self._weighted_beta(security_returns, benchmark_returns, weights)
        if beta is None:
            return {"beta": 1.0, "downside_beta": 1.0, "idio_vol": 0.0}
        shrinkage = min(max(self.beta_shrinkage, 0.0), 1.0)
        beta = (1.0 - shrinkage) * beta + shrinkage * self.beta_shrinkage_target

        downside_pairs = [
            (security, benchmark, weight)
            for security, benchmark, weight in zip(security_returns, benchmark_returns, weights, strict=True)
            if benchmark < 0.0
        ]
        downside_minimum = max(3, self.min_beta_observations // 2)
        if len(downside_pairs) >= downside_minimum:
            downside_security = [security for security, _, _ in downside_pairs]
            downside_benchmark = [benchmark for _, benchmark, _ in downside_pairs]
            downside_weights = [weight for _, _, weight in downside_pairs]
            downside_beta = self._weighted_beta(downside_security, downside_benchmark, downside_weights)
            if downside_beta is None:
                downside_beta = beta
        else:
            downside_beta = beta

        residuals = [
            security - beta * benchmark
            for security, benchmark in zip(security_returns, benchmark_returns, strict=True)
        ]
        residual_mean = self._weighted_mean(residuals, weights)
        residual_variance = self._weighted_mean(
            [(residual - residual_mean) ** 2 for residual in residuals],
            weights,
        )
        idio_vol = (residual_variance * 252.0) ** 0.5 if residual_variance > 0 else 0.0
        return {"beta": beta, "downside_beta": downside_beta, "idio_vol": idio_vol}

    def _can_estimate_beta(
        self,
        permno: str,
        rebalance_date: date,
        returns_by_date: dict[date, dict[str, float]],
        benchmark_by_date: dict[date, float],
    ) -> bool:
        return_pairs = self._historical_return_pairs(permno, rebalance_date, returns_by_date, benchmark_by_date)
        if len(return_pairs) < self.min_beta_observations:
            return False
        security_returns = [security_return for security_return, _ in return_pairs]
        benchmark_returns = [benchmark_return for _, benchmark_return in return_pairs]
        weights = self._beta_weights(len(return_pairs))
        return self._weighted_beta(security_returns, benchmark_returns, weights) is not None

    def _historical_return_pairs(
        self,
        permno: str,
        rebalance_date: date,
        returns_by_date: dict[date, dict[str, float]],
        benchmark_by_date: dict[date, float],
    ) -> list[tuple[float, float]]:
        lower_bound = rebalance_date.toordinal() - self.beta_lookback_days
        pairs: list[tuple[float, float]] = []
        for day in sorted(returns_by_date):
            if day >= rebalance_date:
                break
            if day.toordinal() < lower_bound:
                continue
            if permno not in returns_by_date[day] or day not in benchmark_by_date:
                continue
            pairs.append((returns_by_date[day][permno], benchmark_by_date[day]))
        return pairs

    def _weighted_beta(
        self,
        security_returns: list[float],
        benchmark_returns: list[float],
        weights: list[float],
    ) -> float | None:
        benchmark_mean = self._weighted_mean(benchmark_returns, weights)
        security_mean = self._weighted_mean(security_returns, weights)
        covariance = self._weighted_mean(
            [
                (security - security_mean) * (benchmark - benchmark_mean)
                for security, benchmark in zip(security_returns, benchmark_returns, strict=True)
            ],
            weights,
        )
        variance = self._weighted_mean(
            [(benchmark - benchmark_mean) ** 2 for benchmark in benchmark_returns],
            weights,
        )
        if variance <= 0:
            return None
        return covariance / variance

    def _attach_risk_adjusted_scores(self, features_by_rebalance: dict[date, list[dict]]) -> None:
        for rows in features_by_rebalance.values():
            if not rows:
                continue
            downside_beta_scores = self._normalize_risk_values([row.get("downside_beta", row.get("beta", 1.0)) for row in rows])
            idio_vol_scores = self._normalize_risk_values([row.get("idio_vol", 0.0) for row in rows])
            for row, downside_beta_z, idio_vol_z in zip(rows, downside_beta_scores, idio_vol_scores, strict=True):
                row["downside_beta_z"] = downside_beta_z
                row["idio_vol_z"] = idio_vol_z
                risk_penalty_base = (
                    self.risk_penalty_downside_beta_weight * downside_beta_z
                    + self.risk_penalty_idio_vol_weight * idio_vol_z
                )
                risk_regime_multiplier = self._regime_penalty_multiplier(row)
                risk_penalty = risk_penalty_base * risk_regime_multiplier
                row["risk_penalty_base"] = risk_penalty_base
                row["risk_regime_multiplier"] = risk_regime_multiplier
                row["risk_penalty"] = risk_penalty
                row["risk_adjusted_score"] = row["composite_score"] - risk_penalty

    def _regime_penalty_multiplier(self, row: dict) -> float:
        if not self.regime_risk_scaling_enabled:
            return 1.0
        multiplier = 1.0
        if float(row.get("vix", 0.0)) >= self.regime_vix_threshold:
            multiplier *= self.regime_vix_penalty_multiplier
        if float(row.get("macro_score", 0.0)) < self.regime_macro_threshold:
            multiplier *= self.regime_macro_penalty_multiplier
        return min(multiplier, max(self.regime_penalty_cap, 1.0))

    def _beta_weights(self, observation_count: int) -> list[float]:
        if observation_count <= 0:
            return []
        if self.beta_method == "ewma":
            halflife = max(self.beta_ewma_halflife_days, 1)
            decay = log(2.0) / halflife
            return [exp(-decay * (observation_count - index - 1)) for index in range(observation_count)]
        return [1.0] * observation_count

    def _weighted_mean(self, values: list[float], weights: list[float]) -> float:
        weight_sum = sum(weights)
        if weight_sum <= 0:
            return 0.0
        return sum(value * weight for value, weight in zip(values, weights, strict=True)) / weight_sum

    def _build_returns(self, prices: dict[str, list[dict]]) -> dict[date, dict[str, float]]:
        returns_by_date: dict[date, dict[str, float]] = defaultdict(dict)
        for permno, rows in prices.items():
            for row in rows:
                if self.start_date and row["date"] < self.start_date:
                    continue
                if self.end_date and row["date"] > self.end_date:
                    continue
                returns_by_date[row["date"]][permno] = row["ret"]
        return returns_by_date

    def _reverse_link(self, mapping: dict[str, list[dict]]) -> dict[str, str]:
        reversed_map: dict[str, str] = {}
        for key, candidates in mapping.items():
            for candidate in candidates:
                reversed_map[candidate["permno"]] = key
        return reversed_map

    def _match_linked_id(self, candidates: list[dict], target_date: date, key: str) -> str | None:
        for candidate in candidates:
            if candidate["start"] <= target_date <= candidate["end"]:
                return candidate[key]
        return None

    def _latest_before(self, rows: list[dict], target_date: date) -> dict | None:
        latest = None
        for row in rows:
            if row["date"] <= target_date:
                latest = row
            else:
                break
        return latest

    def _previous_row(self, rows: list[dict], current_row: dict) -> dict | None:
        previous = None
        for row in rows:
            if row is current_row:
                return previous
            previous = row
        return previous

    def _sum_patents_last_year(self, rows: list[dict], target_date: date) -> dict[str, float]:
        patent_count = 0.0
        citation_count = 0.0
        for row in rows:
            day_delta = (target_date - row["date"]).days
            if 0 <= day_delta <= 365:
                patent_count += row["patent_count"]
                citation_count += row["citation_count"]
        return {"patent_count": patent_count, "citation_count": citation_count}

    def _average_dollar_volume(self, rows: list[dict], target_date: date) -> float | None:
        lower_bound = target_date.toordinal() - max(self.liquidity_lookback_days, 1)
        dollar_volumes = []
        for row in rows:
            if row["date"] >= target_date:
                break
            if row["date"].toordinal() < lower_bound:
                continue
            volume = float(row.get("vol", 0.0))
            price = float(row.get("prc", 0.0))
            if volume <= 0.0 or price <= 0.0:
                continue
            dollar_volumes.append(price * volume)
        if not dollar_volumes:
            return None
        return sum(dollar_volumes) / len(dollar_volumes)

    def _grade_pulse(self, rows: list[dict], target_date: date) -> float:
        score = 0.0
        for row in rows:
            day_delta = (target_date - row["date"]).days
            if 0 <= day_delta <= 30:
                action = row["action"]
                if "upgrade" in action:
                    score += 1.0
                elif "downgrade" in action:
                    score -= 1.0
        return score

    def _macro_snapshot(self, target_date: date, macro: dict[str, dict[date, float]]) -> dict[str, float]:
        dgs10_value = self._nearest_series_value(macro["dgs10"], target_date)
        dgs10_prev = self._nearest_value_days_back(macro["dgs10"], target_date, 60)
        vix_value = self._nearest_series_value(macro["vix"], target_date) or 0.0
        macro_score = 0.0
        if dgs10_value is not None and dgs10_prev is not None and dgs10_value <= dgs10_prev:
            macro_score += 0.5
        if vix_value < self.config.strategy.get("vix_de_risk_level", 30.0):
            macro_score += 0.5
        return {"macro_score": macro_score, "vix": vix_value}

    def _nearest_series_value(self, series: dict[date, float], target_date: date) -> float | None:
        eligible = [day for day in series if day <= target_date]
        if not eligible:
            return None
        return series[max(eligible)]

    def _nearest_value_days_back(self, series: dict[date, float], target_date: date, days_back: int) -> float | None:
        eligible = [day for day in series if day <= target_date]
        if not eligible:
            return None
        cutoff = max(eligible)
        target_ordinal = cutoff.toordinal() - days_back
        historical = [day for day in series if day.toordinal() <= target_ordinal]
        if not historical:
            return None
        return series[max(historical)]

    def _apply_universe_filters(self, rows: list[dict]) -> list[dict]:
        minimum_price = self.config.strategy.get("min_price", 5.0)
        minimum_market_cap = self.config.strategy.get("min_market_cap", 100_000_000.0)
        filtered = [
            row
            for row in rows
            if row["price"] >= minimum_price
            and row["market_cap"] >= minimum_market_cap
            and row.get("avg_dollar_volume", 0.0) >= self.min_avg_dollar_volume
            and (not self.universe_include_sectors or row.get("sector", "UNKNOWN") in self.universe_include_sectors)
            and row.get("sector", "UNKNOWN") not in self.universe_exclude_sectors
        ]
        if self.universe_top_n_by_market_cap > 0 and len(filtered) > self.universe_top_n_by_market_cap:
            filtered = sorted(
                filtered,
                key=lambda row: (
                    row.get("market_cap", 0.0),
                    row.get("avg_dollar_volume", 0.0),
                    row.get("permno", ""),
                ),
                reverse=True,
            )[:self.universe_top_n_by_market_cap]
        return filtered

    def _build_benchmark_series(
        self,
        features_by_rebalance: dict[date, list[dict]],
        returns_by_date: dict[date, dict[str, float]],
        factors: list[dict],
    ) -> dict[date, float]:
        if self.benchmark_mode == "zero":
            return {day: 0.0 for day in returns_by_date}
        if self.benchmark_mode == "equal_weight_universe":
            return self._build_equal_weight_universe_benchmark(features_by_rebalance, returns_by_date)
        return {row["date"]: row["mktrf"] + row["rf"] for row in factors}

    def _build_equal_weight_universe_benchmark(
        self,
        features_by_rebalance: dict[date, list[dict]],
        returns_by_date: dict[date, dict[str, float]],
    ) -> dict[date, float]:
        benchmark_by_date: dict[date, float] = {}
        rebalance_dates = sorted(features_by_rebalance)
        rebalance_index = 0
        current_universe: list[str] = []
        for day in sorted(returns_by_date):
            while rebalance_index < len(rebalance_dates) and rebalance_dates[rebalance_index] <= day:
                rebalance_date = rebalance_dates[rebalance_index]
                current_universe = [row["permno"] for row in features_by_rebalance.get(rebalance_date, [])]
                rebalance_index += 1
            if not current_universe:
                benchmark_by_date[day] = 0.0
                continue
            available_returns = [
                returns_by_date[day][permno]
                for permno in current_universe
                if permno in returns_by_date[day]
            ]
            benchmark_by_date[day] = (
                sum(available_returns) / len(available_returns)
                if available_returns
                else 0.0
            )
        return benchmark_by_date

    def _normalize_feature_values(self, values: list[float]) -> list[float]:
        return normalize_cross_section(
            values,
            method=self.feature_zscore_method,
            winsor_quantile=self.feature_winsor_quantile,
        )

    def _normalize_risk_values(self, values: list[float]) -> list[float]:
        return normalize_cross_section(
            values,
            method=self.risk_zscore_method,
            winsor_quantile=self.risk_winsor_quantile,
        )

    def _normalization_method(self, value: object) -> str:
        method = str(value).strip().lower() or "standard"
        if method not in {"standard", "robust"}:
            return "standard"
        return method

    def _normalization_quantile(self, value: object) -> float:
        return min(max(float(value), 0.0), 0.49)
