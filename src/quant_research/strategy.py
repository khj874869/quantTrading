from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date


@dataclass(slots=True)
class PortfolioWeights:
    rebalance_date: date
    weights: dict[str, float]
    exposure: float
    diagnostics: dict[str, float] | None = None


class MultiSignalStrategy:
    def __init__(self, strategy_config: dict) -> None:
        self.config = strategy_config
        self.score_field = str(strategy_config.get("score_field", "risk_adjusted_score")).strip() or "risk_adjusted_score"

    def build_weights(self, rebalance_date: date, rows: list[dict]) -> PortfolioWeights:
        ranked = sorted(rows, key=self._score_value, reverse=True)
        top_count = min(self.config.get("holding_count", 20), len(ranked))
        if top_count == 0:
            return PortfolioWeights(rebalance_date=rebalance_date, weights={}, exposure=0.0, diagnostics={})

        de_risk_level = self.config.get("vix_de_risk_level", 30.0)
        flatten_level = self.config.get("vix_flatten_level", 40.0)
        top_rows = ranked[:top_count]
        avg_macro_score = sum(row.get("macro_score", 0.0) for row in top_rows) / len(top_rows)
        max_vix = max(row.get("vix", 0.0) for row in top_rows)

        exposure = 1.0 if avg_macro_score >= 0.5 else 0.5
        if max_vix >= flatten_level:
            exposure = 0.0
        elif max_vix >= de_risk_level:
            exposure = min(exposure, 0.5)

        long_names = self._select_long_names(ranked, top_count)
        if not long_names:
            return PortfolioWeights(rebalance_date=rebalance_date, weights={}, exposure=0.0, diagnostics={})
        weights = {row["permno"]: exposure / len(long_names) for row in long_names}
        selected_rows = list(long_names)
        if self.config.get("long_short", False):
            bottom_quantile = self.config.get("bottom_quantile", 0.15)
            short_count = max(1, math.floor(len(ranked) * bottom_quantile))
            short_names = self._select_short_names(ranked, short_count)
            if not short_names:
                return PortfolioWeights(
                    rebalance_date=rebalance_date,
                    weights=weights,
                    exposure=exposure,
                    diagnostics=self._constraint_diagnostics(weights, selected_rows),
                )
            for row in short_names:
                weights[row["permno"]] = weights.get(row["permno"], 0.0) - exposure / len(short_names)
            selected_rows.extend(short_names)
            if self.config.get("beta_neutral", False):
                weights = self._apply_long_short_beta_neutral(weights, long_names, short_names, exposure)
            if self.config.get("constraint_neutral", False):
                weights = self._apply_constraint_neutralization(weights, selected_rows, exposure)
        elif self.config.get("beta_neutral", False) and self.config.get("benchmark_hedge", True):
            weights = self._apply_benchmark_hedge(weights, long_names)
        return PortfolioWeights(
            rebalance_date=rebalance_date,
            weights=weights,
            exposure=exposure,
            diagnostics=self._constraint_diagnostics(weights, selected_rows),
        )

    def _select_long_names(self, ranked: list[dict], target_count: int) -> list[dict]:
        if not self.config.get("sector_neutral", False):
            return ranked[:target_count]
        return self._select_sector_neutral(ranked, target_count, reverse=True)

    def _select_short_names(self, ranked: list[dict], target_count: int) -> list[dict]:
        if not self.config.get("sector_neutral", False):
            return ranked[-target_count:]
        return self._select_sector_neutral(ranked, target_count, reverse=False)

    def _select_sector_neutral(self, ranked: list[dict], target_count: int, reverse: bool) -> list[dict]:
        buckets: dict[str, list[dict]] = {}
        for row in ranked:
            buckets.setdefault(row.get("sector", "UNKNOWN"), []).append(row)
        for sector_rows in buckets.values():
            sector_rows.sort(key=self._score_value, reverse=reverse)

        sector_order = sorted(
            buckets,
            key=lambda sector: self._score_value(buckets[sector][0]),
            reverse=reverse,
        )
        active_sector_count = min(len(sector_order), target_count)
        active_sectors = sector_order[:active_sector_count]
        if not active_sectors:
            return []

        base_count = target_count // active_sector_count
        remainder = target_count % active_sector_count
        counts = {sector: min(base_count, len(buckets[sector])) for sector in active_sectors}

        if base_count == 0:
            counts = {sector: 0 for sector in active_sectors}

        for sector in active_sectors:
            if remainder <= 0:
                break
            if counts[sector] < len(buckets[sector]):
                counts[sector] += 1
                remainder -= 1

        while sum(counts.values()) < target_count:
            progressed = False
            for sector in active_sectors:
                if counts[sector] < len(buckets[sector]):
                    counts[sector] += 1
                    progressed = True
                    if sum(counts.values()) == target_count:
                        break
            if not progressed:
                break

        selected = []
        for sector in active_sectors:
            selected.extend(buckets[sector][: counts[sector]])
        selected.sort(key=self._score_value, reverse=reverse)
        return selected[:target_count]

    def _score_value(self, row: dict) -> float:
        return float(row.get(self.score_field, row.get("risk_adjusted_score", row.get("composite_score", 0.0))))

    def _apply_long_short_beta_neutral(
        self,
        weights: dict[str, float],
        long_names: list[dict],
        short_names: list[dict],
        exposure: float,
    ) -> dict[str, float]:
        long_beta = sum(max(weights.get(row["permno"], 0.0), 0.0) * row.get("beta", 1.0) for row in long_names)
        short_beta_abs = sum(abs(min(weights.get(row["permno"], 0.0), 0.0)) * row.get("beta", 1.0) for row in short_names)
        adjusted = dict(weights)
        if short_beta_abs > 0:
            short_scale = long_beta / short_beta_abs if long_beta > 0 else 1.0
            for row in short_names:
                permno = row["permno"]
                adjusted[permno] = adjusted.get(permno, 0.0) * short_scale
        gross = sum(abs(value) for value in adjusted.values())
        target_gross = max(exposure * 2.0, 0.0)
        if gross > 0 and target_gross > 0:
            scale = target_gross / gross
            adjusted = {permno: value * scale for permno, value in adjusted.items()}
        return adjusted

    def _apply_benchmark_hedge(self, weights: dict[str, float], long_names: list[dict]) -> dict[str, float]:
        long_beta = sum(weights.get(row["permno"], 0.0) * row.get("beta", 1.0) for row in long_names)
        adjusted = dict(weights)
        if abs(long_beta) > 0:
            adjusted["__BENCH__"] = -long_beta
        return adjusted

    def _apply_constraint_neutralization(
        self,
        weights: dict[str, float],
        selected_rows: list[dict],
        exposure: float,
    ) -> dict[str, float]:
        constraints = self._build_constraint_rows(selected_rows)
        if not constraints:
            return weights
        permnos = [row["permno"] for row in selected_rows]
        weight_vector = [weights.get(permno, 0.0) for permno in permnos]
        ata = self._build_constraint_gram(constraints)
        atw = [sum(constraint[index] * weight_vector[index] for index in range(len(weight_vector))) for constraint in constraints]
        lambdas = self._solve_linear_system(ata, atw)
        if lambdas is None:
            return weights
        adjusted_vector = []
        for asset_index, current_weight in enumerate(weight_vector):
            projection = sum(constraints[row_index][asset_index] * lambdas[row_index] for row_index in range(len(constraints)))
            adjusted_vector.append(current_weight - projection)
        adjusted = {permno: adjusted_vector[index] for index, permno in enumerate(permnos)}
        gross = sum(abs(value) for value in adjusted.values())
        target_gross = max(exposure * 2.0, 0.0)
        if gross > 0 and target_gross > 0:
            scale = target_gross / gross
            adjusted = {permno: value * scale for permno, value in adjusted.items()}
        return adjusted

    def _build_constraint_rows(self, selected_rows: list[dict]) -> list[list[float]]:
        factors = self.config.get("constraint_neutral_factors", ["beta", "size", "sector"])
        if len(selected_rows) <= 1:
            return []
        constraints: list[list[float]] = []
        constraints.append([1.0 for _ in selected_rows])
        if "beta" in factors:
            constraints.append([row.get("beta", 1.0) for row in selected_rows])
        if "downside_beta" in factors:
            constraints.append([row.get("downside_beta", row.get("beta", 1.0)) for row in selected_rows])
        if "idio_vol" in factors:
            idio_vols = [row.get("idio_vol", 0.0) for row in selected_rows]
            mean_idio_vol = sum(idio_vols) / len(idio_vols)
            constraints.append([value - mean_idio_vol for value in idio_vols])
        if "size" in factors:
            log_caps = [math.log(max(row.get("market_cap", 1.0), 1.0)) for row in selected_rows]
            mean_log_cap = sum(log_caps) / len(log_caps)
            constraints.append([value - mean_log_cap for value in log_caps])
        if "sector" in factors:
            sectors = sorted({row.get("sector", "UNKNOWN") for row in selected_rows})
            for sector in sectors[:-1]:
                constraints.append([1.0 if row.get("sector", "UNKNOWN") == sector else 0.0 for row in selected_rows])
        return [row for row in constraints if any(abs(value) > 1e-12 for value in row)]

    def _build_constraint_gram(self, constraints: list[list[float]]) -> list[list[float]]:
        matrix = []
        ridge = float(self.config.get("constraint_ridge", 1e-8))
        for row_index, row in enumerate(constraints):
            gram_row = []
            for col_index, other_row in enumerate(constraints):
                value = sum(left * right for left, right in zip(row, other_row, strict=True))
                if row_index == col_index:
                    value += ridge
                gram_row.append(value)
            matrix.append(gram_row)
        return matrix

    def _solve_linear_system(self, matrix: list[list[float]], vector: list[float]) -> list[float] | None:
        size = len(vector)
        augmented = [matrix[row_index][:] + [vector[row_index]] for row_index in range(size)]
        for pivot_index in range(size):
            pivot_row = max(range(pivot_index, size), key=lambda row_index: abs(augmented[row_index][pivot_index]))
            pivot_value = augmented[pivot_row][pivot_index]
            if abs(pivot_value) < 1e-12:
                return None
            if pivot_row != pivot_index:
                augmented[pivot_index], augmented[pivot_row] = augmented[pivot_row], augmented[pivot_index]
            pivot_value = augmented[pivot_index][pivot_index]
            for col_index in range(pivot_index, size + 1):
                augmented[pivot_index][col_index] /= pivot_value
            for row_index in range(size):
                if row_index == pivot_index:
                    continue
                factor = augmented[row_index][pivot_index]
                if factor == 0:
                    continue
                for col_index in range(pivot_index, size + 1):
                    augmented[row_index][col_index] -= factor * augmented[pivot_index][col_index]
        return [augmented[row_index][size] for row_index in range(size)]

    def _constraint_diagnostics(self, weights: dict[str, float], selected_rows: list[dict]) -> dict[str, float]:
        selected_lookup = {row["permno"]: row for row in selected_rows}
        diagnostics = {
            "net_weight": sum(weights.values()),
            "gross_weight": sum(abs(weight) for permno, weight in weights.items() if permno != "__BENCH__"),
            "beta_exposure": 0.0,
            "downside_beta_exposure": 0.0,
            "idio_vol_exposure": 0.0,
            "size_exposure": 0.0,
        }
        benchmark_weight = weights.get("__BENCH__", 0.0)
        diagnostics["beta_exposure"] += benchmark_weight
        diagnostics["downside_beta_exposure"] += benchmark_weight
        if selected_lookup:
            log_caps = {
                permno: math.log(max(row.get("market_cap", 1.0), 1.0))
                for permno, row in selected_lookup.items()
            }
            idio_vols = {
                permno: row.get("idio_vol", 0.0)
                for permno, row in selected_lookup.items()
            }
            mean_log_cap = sum(log_caps.values()) / len(log_caps)
            mean_idio_vol = sum(idio_vols.values()) / len(idio_vols)
            diagnostics["size_exposure"] = sum(
                weights.get(permno, 0.0) * (log_caps[permno] - mean_log_cap)
                for permno in selected_lookup
            )
            diagnostics["beta_exposure"] += sum(
                weights.get(permno, 0.0) * selected_lookup[permno].get("beta", 1.0)
                for permno in selected_lookup
            )
            diagnostics["downside_beta_exposure"] += sum(
                weights.get(permno, 0.0) * selected_lookup[permno].get("downside_beta", selected_lookup[permno].get("beta", 1.0))
                for permno in selected_lookup
            )
            diagnostics["idio_vol_exposure"] = sum(
                weights.get(permno, 0.0) * (idio_vols[permno] - mean_idio_vol)
                for permno in selected_lookup
            )
            sectors = {row.get("sector", "UNKNOWN") for row in selected_lookup.values()}
            for sector in sorted(sectors):
                diagnostics[f"sector_{sector}_exposure"] = sum(
                    weights.get(permno, 0.0)
                    for permno, row in selected_lookup.items()
                    if row.get("sector", "UNKNOWN") == sector
                )
        return diagnostics
