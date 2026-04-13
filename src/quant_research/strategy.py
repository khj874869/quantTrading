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
    selected_rows: list[dict] | None = None
    metadata: dict[str, float] | None = None
    target_weights: dict[str, float] | None = None


class MultiSignalStrategy:
    def __init__(self, strategy_config: dict) -> None:
        self.config = strategy_config
        self.score_field = str(strategy_config.get("score_field", "risk_adjusted_score")).strip() or "risk_adjusted_score"
        self.incumbent_score_bonus = max(float(strategy_config.get("incumbent_score_bonus", 0.0)), 0.0)
        self.entry_score_threshold = max(float(strategy_config.get("entry_score_threshold", 0.0)), 0.0)
        self.entry_score_threshold_dynamic_scale = max(float(strategy_config.get("entry_score_threshold_dynamic_scale", 0.0)), 0.0)
        self.entry_turnover_penalty_per_weight = max(float(strategy_config.get("entry_turnover_penalty_per_weight", 0.0)), 0.0)
        self.entry_liquidity_penalty_scale = max(float(strategy_config.get("entry_liquidity_penalty_scale", 0.0)), 0.0)
        self.entry_liquidity_field = str(strategy_config.get("entry_liquidity_field", "liquidity_ratio")).strip() or "liquidity_ratio"
        self.entry_liquidity_floor = max(float(strategy_config.get("entry_liquidity_floor", 0.01)), 1e-9)
        portfolio_construction = str(strategy_config.get("portfolio_construction", "heuristic")).strip().lower() or "heuristic"
        self.portfolio_construction = portfolio_construction if portfolio_construction in {"heuristic", "optimizer"} else "heuristic"
        weighting_scheme = str(strategy_config.get("weighting_scheme", "equal")).strip().lower() or "equal"
        allowed_weighting_schemes = {"equal", "score", "inverse_vol", "score_inverse_vol"}
        self.weighting_scheme = weighting_scheme if weighting_scheme in allowed_weighting_schemes else "equal"
        self.risk_weight_field = str(strategy_config.get("risk_weight_field", "idio_vol")).strip() or "idio_vol"
        self.risk_weight_floor = max(float(strategy_config.get("risk_weight_floor", 0.10)), 1e-6)
        self.score_weight_floor = max(float(strategy_config.get("score_weight_floor", 0.05)), 1e-6)
        self.optimizer_risk_aversion = max(float(strategy_config.get("optimizer_risk_aversion", 0.5)), 0.0)
        self.optimizer_covariance_penalty = max(float(strategy_config.get("optimizer_covariance_penalty", 0.0)), 0.0)
        configured_covariance_fields = strategy_config.get("optimizer_covariance_fields", ["beta", "downside_beta", "size", "sector"])
        if not isinstance(configured_covariance_fields, list):
            configured_covariance_fields = ["beta", "downside_beta", "size", "sector"]
        self.optimizer_covariance_fields = [
            str(field).strip()
            for field in configured_covariance_fields
            if str(field).strip()
        ]
        self.optimizer_turnover_penalty = max(float(strategy_config.get("optimizer_turnover_penalty", 0.25)), 0.0)
        self.optimizer_iterations = max(int(strategy_config.get("optimizer_iterations", 200)), 1)
        self.optimizer_step_size = max(float(strategy_config.get("optimizer_step_size", 0.05)), 1e-6)
        self.max_position_weight = max(float(strategy_config.get("max_position_weight", 0.0)), 0.0)
        self.liquidity_position_cap_ratio = max(float(strategy_config.get("liquidity_position_cap_ratio", 0.0)), 0.0)
        self.liquidity_position_cap_field = str(strategy_config.get("liquidity_position_cap_field", "avg_dollar_volume")).strip() or "avg_dollar_volume"
        self.liquidity_position_cap_floor = max(float(strategy_config.get("liquidity_position_cap_floor", strategy_config.get("slippage_adv_floor", 100_000.0))), 1.0)
        self.liquidity_position_cap_notional = max(float(strategy_config.get("liquidity_position_cap_notional", strategy_config.get("slippage_notional", 1_000_000.0))), 1.0)
        self.short_min_avg_dollar_volume = max(float(strategy_config.get("short_min_avg_dollar_volume", 0.0)), 0.0)
        self.short_min_market_cap = max(float(strategy_config.get("short_min_market_cap", 0.0)), 0.0)
        self.short_min_liquidity_ratio = max(float(strategy_config.get("short_min_liquidity_ratio", 0.0)), 0.0)
        self.short_locate_required = bool(strategy_config.get("short_locate_required", False))
        self.short_locate_available_field = str(strategy_config.get("short_locate_available_field", "short_locate_available")).strip() or "short_locate_available"
        self.short_locate_score_field = str(strategy_config.get("short_locate_score_field", "short_locate_score")).strip() or "short_locate_score"
        self.short_locate_min_score = max(float(strategy_config.get("short_locate_min_score", 0.0)), 0.0)
        self.short_max_borrow_cost_bps_annual = max(float(strategy_config.get("short_max_borrow_cost_bps_annual", 0.0)), 0.0)
        self.short_borrow_cost_field = str(strategy_config.get("short_borrow_cost_field", "short_borrow_cost_bps_annual")).strip() or "short_borrow_cost_bps_annual"
        self.short_exclude_sectors = {
            str(value).strip()
            for value in strategy_config.get("short_exclude_sectors", [])
            if str(value).strip()
        }

    def build_weights(self, rebalance_date: date, rows: list[dict], previous_weights: dict[str, float] | None = None) -> PortfolioWeights:
        previous_weights = previous_weights or {}
        ranked = sorted(rows, key=self._score_value, reverse=True)
        top_count = min(self.config.get("holding_count", 20), len(ranked))
        if top_count == 0:
            return PortfolioWeights(
                rebalance_date=rebalance_date,
                weights={},
                exposure=0.0,
                diagnostics={},
                selected_rows=[],
                metadata={"selection_score_dispersion": self._score_dispersion(rows)},
            )

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

        long_names = self._select_long_names(ranked, top_count, previous_weights, exposure)
        if not long_names:
            return PortfolioWeights(
                rebalance_date=rebalance_date,
                weights={},
                exposure=0.0,
                diagnostics={},
                selected_rows=[],
                metadata={"selection_score_dispersion": self._score_dispersion(rows)},
            )
        weights = self._build_side_weights(long_names, exposure, side="long", previous_weights=previous_weights)
        selected_rows = list(long_names)
        metadata = {"selection_score_dispersion": self._score_dispersion(rows)}
        if self.config.get("long_short", False):
            bottom_quantile = self.config.get("bottom_quantile", 0.15)
            short_count = max(1, math.floor(len(ranked) * bottom_quantile))
            short_names = self._select_short_names(ranked, short_count, previous_weights, exposure)
            if not short_names:
                return PortfolioWeights(
                    rebalance_date=rebalance_date,
                    weights=weights,
                    exposure=exposure,
                    diagnostics=self._constraint_diagnostics(weights, selected_rows),
                    selected_rows=selected_rows,
                    metadata=metadata,
                )
            short_weights = self._build_side_weights(short_names, exposure, side="short", previous_weights=previous_weights)
            for permno, weight in short_weights.items():
                weights[permno] = weights.get(permno, 0.0) + weight
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
            selected_rows=selected_rows,
            metadata=metadata,
        )

    def _select_long_names(self, ranked: list[dict], target_count: int, previous_weights: dict[str, float], exposure: float) -> list[dict]:
        if not self.config.get("sector_neutral", False):
            ordered = sorted(
                ranked,
                key=lambda row: self._selection_score(row, previous_weights, side="long", target_count=target_count, exposure=exposure),
                reverse=True,
            )
            return self._select_with_entry_threshold(ordered, target_count, previous_weights, side="long", exposure=exposure)
        return self._select_sector_neutral(ranked, target_count, reverse=True, previous_weights=previous_weights, side="long", exposure=exposure)

    def _select_short_names(self, ranked: list[dict], target_count: int, previous_weights: dict[str, float], exposure: float) -> list[dict]:
        eligible_ranked = [row for row in ranked if self._shorting_eligible(row)]
        if not eligible_ranked:
            return []
        if not self.config.get("sector_neutral", False):
            ordered = sorted(
                eligible_ranked,
                key=lambda row: self._selection_score(row, previous_weights, side="short", target_count=target_count, exposure=exposure),
            )
            return self._select_with_entry_threshold(ordered, target_count, previous_weights, side="short", exposure=exposure)
        return self._select_sector_neutral(eligible_ranked, target_count, reverse=False, previous_weights=previous_weights, side="short", exposure=exposure)

    def _select_sector_neutral(
        self,
        ranked: list[dict],
        target_count: int,
        reverse: bool,
        previous_weights: dict[str, float],
        side: str,
        exposure: float,
    ) -> list[dict]:
        buckets: dict[str, list[dict]] = {}
        for row in ranked:
            buckets.setdefault(row.get("sector", "UNKNOWN"), []).append(row)
        for sector_rows in buckets.values():
            sector_rows.sort(
                key=lambda row: self._selection_score(
                    row,
                    previous_weights,
                    side=side,
                    target_count=target_count,
                    exposure=exposure,
                ),
                reverse=reverse,
            )

        sector_order = sorted(
            buckets,
            key=lambda sector: self._selection_score(
                buckets[sector][0],
                previous_weights,
                side=side,
                target_count=target_count,
                exposure=exposure,
            ),
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
            selected.extend(
                self._select_with_entry_threshold(
                    buckets[sector],
                    counts[sector],
                    previous_weights,
                    side=side,
                    exposure=exposure,
                )
            )
        selected.sort(
            key=lambda row: self._selection_score(
                row,
                previous_weights,
                side=side,
                target_count=target_count,
                exposure=exposure,
            ),
            reverse=reverse,
        )
        return selected[:target_count]

    def _score_value(self, row: dict) -> float:
        return float(row.get(self.score_field, row.get("risk_adjusted_score", row.get("composite_score", 0.0))))

    def _shorting_eligible(self, row: dict) -> bool:
        sector = str(row.get("sector", "UNKNOWN"))
        if sector in self.short_exclude_sectors:
            return False
        if float(row.get("avg_dollar_volume", 0.0)) < self.short_min_avg_dollar_volume:
            return False
        if float(row.get("market_cap", 0.0)) < self.short_min_market_cap:
            return False
        if float(row.get("liquidity_ratio", 0.0)) < self.short_min_liquidity_ratio:
            return False
        if self.short_locate_required and not self._short_locate_available(row):
            return False
        if self.short_max_borrow_cost_bps_annual > 0.0:
            borrow_cost = float(row.get(self.short_borrow_cost_field, 0.0) or 0.0)
            if borrow_cost > self.short_max_borrow_cost_bps_annual:
                return False
        return True

    def _short_locate_available(self, row: dict) -> bool:
        availability_value = row.get(self.short_locate_available_field)
        if availability_value is not None:
            return self._bool_value(availability_value)
        locate_score = row.get(self.short_locate_score_field)
        if locate_score is None:
            return False
        return float(locate_score) >= self.short_locate_min_score

    def _bool_value(self, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return float(value) != 0.0
        text = str(value).strip().lower()
        return text not in {"", "0", "false", "f", "no", "n", "off", "none", "na"}

    def _selection_score(
        self,
        row: dict,
        previous_weights: dict[str, float],
        side: str,
        target_count: int,
        exposure: float,
    ) -> float:
        score = self._score_value(row)
        incumbent_weight = previous_weights.get(row["permno"], 0.0)
        if self.incumbent_score_bonus > 0.0:
            if side == "long" and incumbent_weight > 0.0:
                score += self.incumbent_score_bonus
            elif side == "short" and incumbent_weight < 0.0:
                score -= self.incumbent_score_bonus
        if self.entry_turnover_penalty_per_weight > 0.0 and not self._is_incumbent(row, previous_weights, side):
            score -= self.entry_turnover_penalty_per_weight * self._estimated_entry_turnover(previous_weights, side, target_count, exposure)
        if self.entry_liquidity_penalty_scale > 0.0 and not self._is_incumbent(row, previous_weights, side):
            liquidity_value = max(abs(float(row.get(self.entry_liquidity_field, 0.0))), self.entry_liquidity_floor)
            score -= (
                self.entry_liquidity_penalty_scale
                * self._estimated_entry_turnover(previous_weights, side, target_count, exposure)
                / liquidity_value
            )
        return score

    def _select_with_entry_threshold(
        self,
        ordered_rows: list[dict],
        target_count: int,
        previous_weights: dict[str, float],
        side: str,
        exposure: float,
    ) -> list[dict]:
        effective_threshold = self._effective_entry_threshold(ordered_rows)
        selected = list(ordered_rows[:target_count])
        if (
            target_count <= 0
            or effective_threshold <= 0.0
            or not previous_weights
            or not selected
        ):
            return selected

        rejected_incumbents = [
            row
            for row in ordered_rows
            if self._is_incumbent(row, previous_weights, side) and row["permno"] not in {candidate["permno"] for candidate in selected}
        ]
        if not rejected_incumbents:
            return selected

        selected_by_permno = {row["permno"]: row for row in selected}
        for incumbent in rejected_incumbents:
            new_entrants = [
                row
                for row in selected_by_permno.values()
                if not self._is_incumbent(row, previous_weights, side)
            ]
            if not new_entrants:
                break
            entrant_to_replace = self._weakest_selected(new_entrants, previous_weights, side)
            if self._entry_gap(entrant_to_replace, incumbent, side) >= effective_threshold:
                continue
            selected_by_permno.pop(entrant_to_replace["permno"], None)
            selected_by_permno[incumbent["permno"]] = incumbent

        selected_rows = list(selected_by_permno.values())
        return sorted(
            selected_rows,
            key=lambda row: self._selection_score(
                row,
                previous_weights,
                side=side,
                target_count=target_count,
                exposure=exposure,
            ),
            reverse=(side == "long"),
        )[:target_count]

    def _is_incumbent(self, row: dict, previous_weights: dict[str, float], side: str) -> bool:
        incumbent_weight = previous_weights.get(row["permno"], 0.0)
        if side == "long":
            return incumbent_weight > 0.0
        return incumbent_weight < 0.0

    def _weakest_selected(self, rows: list[dict], previous_weights: dict[str, float], side: str) -> dict:
        return sorted(
            rows,
            key=lambda row: self._score_value(row),
            reverse=(side == "short"),
        )[0]

    def _entry_gap(self, entrant: dict, incumbent: dict, side: str) -> float:
        entrant_score = self._score_value(entrant)
        incumbent_score = self._score_value(incumbent)
        if side == "short":
            return incumbent_score - entrant_score
        return entrant_score - incumbent_score

    def _effective_entry_threshold(self, rows: list[dict]) -> float:
        dynamic_threshold = 0.0
        if self.entry_score_threshold_dynamic_scale > 0.0 and len(rows) >= 2:
            scores = [self._score_value(row) for row in rows]
            mean_score = sum(scores) / len(scores)
            variance = sum((score - mean_score) ** 2 for score in scores) / len(scores)
            dynamic_threshold = math.sqrt(max(variance, 0.0)) * self.entry_score_threshold_dynamic_scale
        return max(self.entry_score_threshold, dynamic_threshold)

    def _estimated_entry_turnover(
        self,
        previous_weights: dict[str, float],
        side: str,
        target_count: int,
        exposure: float,
    ) -> float:
        if target_count <= 0 or exposure <= 0.0:
            return 0.0
        if side == "long":
            side_weights = sorted(weight for weight in previous_weights.values() if weight > 0.0)
        else:
            side_weights = sorted(abs(weight) for weight in previous_weights.values() if weight < 0.0)
        if not side_weights:
            return 0.0
        slot_weight = exposure / max(target_count, 1)
        displaced_weight = side_weights[0]
        return 0.5 * (slot_weight + displaced_weight)

    def _score_dispersion(self, rows: list[dict]) -> float:
        if len(rows) < 2:
            return 0.0
        scores = [self._score_value(row) for row in rows]
        mean_score = sum(scores) / len(scores)
        variance = sum((score - mean_score) ** 2 for score in scores) / len(scores)
        return math.sqrt(max(variance, 0.0))

    def _build_side_weights(
        self,
        selected_rows: list[dict],
        exposure: float,
        side: str,
        previous_weights: dict[str, float] | None = None,
    ) -> dict[str, float]:
        if not selected_rows or exposure <= 0:
            return {}
        previous_weights = previous_weights or {}
        if self.portfolio_construction == "optimizer":
            optimized = self._optimized_side_weights(selected_rows, exposure, side, previous_weights)
            capped = self._apply_position_cap(optimized, exposure, position_caps=self._position_caps(selected_rows, exposure))
        else:
            raw_weights = self._raw_side_weights(selected_rows, side)
            total_raw = sum(raw_weights)
            if total_raw <= 0:
                raw_weights = [1.0 for _ in selected_rows]
                total_raw = float(len(selected_rows))
            normalized = {
                row["permno"]: exposure * raw_weight / total_raw
                for row, raw_weight in zip(selected_rows, raw_weights, strict=True)
            }
            capped = self._apply_position_cap(normalized, exposure, position_caps=self._position_caps(selected_rows, exposure))
        sign = -1.0 if side == "short" else 1.0
        return {
            permno: sign * weight
            for permno, weight in capped.items()
        }

    def _optimized_side_weights(
        self,
        selected_rows: list[dict],
        exposure: float,
        side: str,
        previous_weights: dict[str, float],
    ) -> dict[str, float]:
        permnos = [row["permno"] for row in selected_rows]
        alpha = self._optimizer_alpha_values(selected_rows, side)
        risk = [
            max(abs(float(row.get(self.risk_weight_field, 0.0))), self.risk_weight_floor)
            for row in selected_rows
        ]
        covariance = self._optimizer_covariance_matrix(selected_rows)
        position_caps = self._position_caps(selected_rows, exposure) or {permno: exposure for permno in permnos}
        previous_side_weights = {
            permno: (
                max(previous_weights.get(permno, 0.0), 0.0)
                if side == "long"
                else abs(min(previous_weights.get(permno, 0.0), 0.0))
            )
            for permno in permnos
        }
        starting_weights = self._optimizer_starting_weights(permnos, previous_side_weights, exposure, position_caps)
        weights = [starting_weights[permno] for permno in permnos]
        previous = [previous_side_weights[permno] for permno in permnos]
        caps = [position_caps.get(permno, exposure) for permno in permnos]
        for _ in range(self.optimizer_iterations):
            covariance_gradient = self._matrix_vector(covariance, weights)
            gradient = [
                alpha_value
                - self.optimizer_risk_aversion * risk_value * weight
                - self.optimizer_covariance_penalty * covariance_value
                - self.optimizer_turnover_penalty * (weight - previous_weight)
                for alpha_value, risk_value, weight, previous_weight, covariance_value in zip(alpha, risk, weights, previous, covariance_gradient, strict=True)
            ]
            updated = [
                weight + self.optimizer_step_size * grad
                for weight, grad in zip(weights, gradient, strict=True)
            ]
            weights = self._project_bounded_simplex(updated, exposure, caps)
        return {
            permno: weight
            for permno, weight in zip(permnos, weights, strict=True)
            if weight > 1e-12
        }

    def _optimizer_alpha_values(self, selected_rows: list[dict], side: str) -> list[float]:
        scores = [self._score_value(row) for row in selected_rows]
        if not scores:
            return []
        if side == "short":
            anchor = max(scores)
            return [max(anchor - score, 0.0) + self.score_weight_floor for score in scores]
        anchor = min(scores)
        return [max(score - anchor, 0.0) + self.score_weight_floor for score in scores]

    def _optimizer_starting_weights(
        self,
        permnos: list[str],
        previous_side_weights: dict[str, float],
        exposure: float,
        position_caps: dict[str, float],
    ) -> dict[str, float]:
        incumbent_total = sum(previous_side_weights.values())
        if incumbent_total > 1e-12:
            scaled = {
                permno: exposure * previous_side_weights[permno] / incumbent_total
                for permno in permnos
            }
            projected = self._project_bounded_simplex(
                [scaled[permno] for permno in permnos],
                exposure,
                [position_caps.get(permno, exposure) for permno in permnos],
            )
            return {permno: weight for permno, weight in zip(permnos, projected, strict=True)}
        equal_weight = exposure / max(len(permnos), 1)
        projected = self._project_bounded_simplex(
            [equal_weight for _ in permnos],
            exposure,
            [position_caps.get(permno, exposure) for permno in permnos],
        )
        return {permno: weight for permno, weight in zip(permnos, projected, strict=True)}

    def _project_bounded_simplex(self, values: list[float], target_sum: float, upper_bounds: list[float]) -> list[float]:
        if not values:
            return []
        bounded_caps = [max(cap, 0.0) for cap in upper_bounds]
        if sum(bounded_caps) <= target_sum + 1e-12:
            return bounded_caps
        low = min(value - cap for value, cap in zip(values, bounded_caps, strict=True))
        high = max(values)
        for _ in range(80):
            midpoint = 0.5 * (low + high)
            projected = [
                min(max(value - midpoint, 0.0), cap)
                for value, cap in zip(values, bounded_caps, strict=True)
            ]
            projected_sum = sum(projected)
            if projected_sum > target_sum:
                low = midpoint
            else:
                high = midpoint
        return [
            min(max(value - high, 0.0), cap)
            for value, cap in zip(values, bounded_caps, strict=True)
        ]

    def _optimizer_covariance_matrix(self, selected_rows: list[dict]) -> list[list[float]]:
        vectors = self._optimizer_covariance_vectors(selected_rows)
        if not vectors:
            return [[0.0 for _ in selected_rows] for _ in selected_rows]
        vector_width = len(vectors[0]) if vectors[0] else 0
        if vector_width <= 0:
            return [[0.0 for _ in selected_rows] for _ in selected_rows]
        scale = 1.0 / vector_width
        matrix = []
        for left in vectors:
            row = []
            for right in vectors:
                row.append(scale * sum(left_value * right_value for left_value, right_value in zip(left, right, strict=True)))
            matrix.append(row)
        return matrix

    def _optimizer_covariance_vectors(self, selected_rows: list[dict]) -> list[list[float]]:
        if not selected_rows or not self.optimizer_covariance_fields:
            return []
        numeric_fields = [field for field in self.optimizer_covariance_fields if field != "sector"]
        columns: list[list[float]] = []
        for field in numeric_fields:
            values = [float(row.get(field, 0.0)) for row in selected_rows]
            mean_value = sum(values) / len(values)
            variance = sum((value - mean_value) ** 2 for value in values) / len(values)
            std_value = math.sqrt(max(variance, 0.0))
            if std_value <= 1e-12:
                continue
            columns.append([(value - mean_value) / std_value for value in values])
        if "sector" in self.optimizer_covariance_fields:
            sectors = sorted({str(row.get("sector", "UNKNOWN")) for row in selected_rows})
            for sector in sectors:
                indicator = [1.0 if str(row.get("sector", "UNKNOWN")) == sector else 0.0 for row in selected_rows]
                mean_value = sum(indicator) / len(indicator)
                variance = sum((value - mean_value) ** 2 for value in indicator) / len(indicator)
                std_value = math.sqrt(max(variance, 0.0))
                if std_value <= 1e-12:
                    continue
                columns.append([(value - mean_value) / std_value for value in indicator])
        if not columns:
            return [[0.0] for _ in selected_rows]
        return [
            [column[index] for column in columns]
            for index in range(len(selected_rows))
        ]

    def _matrix_vector(self, matrix: list[list[float]], vector: list[float]) -> list[float]:
        if not matrix:
            return [0.0 for _ in vector]
        return [
            sum(value * weight for value, weight in zip(row, vector, strict=True))
            for row in matrix
        ]

    def _raw_side_weights(self, selected_rows: list[dict], side: str) -> list[float]:
        use_score_weights = self.weighting_scheme in {"score", "score_inverse_vol"}
        use_inverse_vol_weights = self.weighting_scheme in {"inverse_vol", "score_inverse_vol"}
        scores = [self._score_value(row) for row in selected_rows]
        if use_score_weights:
            if side == "short":
                score_anchor = max(scores)
                score_component = [
                    max(score_anchor - score, 0.0) + self.score_weight_floor
                    for score in scores
                ]
            else:
                score_anchor = min(scores)
                score_component = [
                    max(score - score_anchor, 0.0) + self.score_weight_floor
                    for score in scores
                ]
        else:
            score_component = [1.0 for _ in selected_rows]

        if use_inverse_vol_weights:
            risk_component = [
                1.0 / max(abs(float(row.get(self.risk_weight_field, 0.0))), self.risk_weight_floor)
                for row in selected_rows
            ]
        else:
            risk_component = [1.0 for _ in selected_rows]

        return [
            score_weight * risk_weight
            for score_weight, risk_weight in zip(score_component, risk_component, strict=True)
        ]

    def _position_caps(self, selected_rows: list[dict], target_exposure: float) -> dict[str, float] | None:
        if not selected_rows:
            return None
        caps: dict[str, float] = {}
        static_cap = self.max_position_weight if self.max_position_weight > 0.0 else target_exposure
        liquidity_enabled = self.liquidity_position_cap_ratio > 0.0 and self.liquidity_position_cap_notional > 0.0
        if static_cap >= target_exposure and not liquidity_enabled:
            return None
        for row in selected_rows:
            cap = static_cap
            if liquidity_enabled:
                liquidity_value = max(abs(float(row.get(self.liquidity_position_cap_field, 0.0))), self.liquidity_position_cap_floor)
                liquidity_cap = self.liquidity_position_cap_ratio * liquidity_value / self.liquidity_position_cap_notional
                cap = min(cap, liquidity_cap)
            caps[row["permno"]] = min(max(cap, 0.0), target_exposure)
        return caps

    def _apply_position_cap(
        self,
        weights: dict[str, float],
        target_exposure: float,
        position_caps: dict[str, float] | None = None,
    ) -> dict[str, float]:
        if not weights:
            return {}
        if position_caps is None:
            cap = self.max_position_weight
            if cap <= 0 or cap >= target_exposure:
                return weights
            position_caps = {permno: cap for permno in weights}
        if sum(position_caps.get(permno, target_exposure) for permno in weights) < target_exposure - 1e-12:
            return weights

        remaining = dict(weights)
        adjusted: dict[str, float] = {}
        remaining_target = target_exposure
        while remaining:
            remaining_total = sum(remaining.values())
            if remaining_total <= 0 or remaining_target <= 0:
                break
            capped_permnos = [
                permno
                for permno, weight in remaining.items()
                if remaining_target * weight / remaining_total > position_caps.get(permno, target_exposure) + 1e-12
            ]
            if not capped_permnos:
                break
            for permno in capped_permnos:
                capped_weight = position_caps.get(permno, target_exposure)
                adjusted[permno] = capped_weight
                remaining_target -= capped_weight
                remaining.pop(permno)

        remaining_total = sum(remaining.values())
        if remaining_total > 0 and remaining_target > 0:
            for permno, weight in remaining.items():
                adjusted[permno] = remaining_target * weight / remaining_total
        if adjusted:
            return adjusted
        return weights

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

    def recompute_diagnostics(self, weights: dict[str, float], selected_rows: list[dict] | None = None) -> dict[str, float]:
        return self._constraint_diagnostics(weights, selected_rows or [])
