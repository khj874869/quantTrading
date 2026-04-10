from __future__ import annotations

import hashlib
import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from .config import Config
from .pipeline import DataPipeline, FeaturePanel, PreparedData
from .utils import ensure_directory


CACHE_VERSION = 2
FEATURE_STRATEGY_KEYS = [
    "start_date",
    "end_date",
    "report_lag_days",
    "use_rdq",
    "liquidity_lookback_days",
    "liquidity_fallback_adv_to_mcap",
    "min_price",
    "min_market_cap",
    "vix_de_risk_level",
    "feature_zscore_method",
    "feature_winsor_quantile",
]
BETA_STRATEGY_KEYS = [
    "beta_lookback_days",
    "min_beta_observations",
    "beta_method",
    "beta_ewma_halflife_days",
    "beta_shrinkage",
    "beta_shrinkage_target",
    "risk_penalty_downside_beta_weight",
    "risk_penalty_idio_vol_weight",
    "risk_zscore_method",
    "risk_winsor_quantile",
    "regime_risk_scaling_enabled",
    "regime_vix_threshold",
    "regime_vix_penalty_multiplier",
    "regime_macro_threshold",
    "regime_macro_penalty_multiplier",
    "regime_penalty_cap",
]


@dataclass(slots=True)
class CacheResult:
    prepared_data: PreparedData
    cache_hit: bool
    cache_path: Path
    profile: dict[str, float]
    source_cache_hit: bool
    feature_cache_hit: bool
    prepared_cache_hit: bool


class PreparedDataCache:
    def __init__(self, config: Config) -> None:
        self.config = config
        cache_config = config.raw.get("cache", {})
        self.enabled = bool(cache_config.get("enabled", True))
        self.cache_dir = config.resolve_path(cache_config.get("cache_dir", "output/cache"))

    def load_or_build(self) -> CacheResult:
        source_cache_path = self.cache_dir / "source_data.pkl"
        feature_cache_path = self.cache_dir / "feature_panel.pkl"
        prepared_cache_path = self.cache_dir / "prepared_data.pkl"
        source_fingerprint = self._source_fingerprint()
        source_cache_hit = False
        feature_cache_hit = False
        prepared_cache_hit = False
        profile: dict[str, float] = {}

        pipeline = DataPipeline(self.config)
        if self.enabled and source_cache_path.exists():
            started = perf_counter()
            payload = self._read(source_cache_path)
            if payload.get("fingerprint") == source_fingerprint:
                source_data = payload["source_data"]
                source_cache_hit = True
                profile["source_cache_read_seconds"] = perf_counter() - started
            else:
                source_data = pipeline.load_sources()
        else:
            source_data = pipeline.load_sources()

        if not source_cache_hit:
            profile.update({key: value for key, value in pipeline.profile.items() if key.startswith("load_")})
            if self.enabled:
                self._write(
                    source_cache_path,
                    {
                        "fingerprint": source_fingerprint,
                        "source_data": source_data,
                    },
                )

        feature_fingerprint = self._feature_fingerprint(source_fingerprint)
        if self.enabled and feature_cache_path.exists():
            started = perf_counter()
            payload = self._read(feature_cache_path)
            if payload.get("fingerprint") == feature_fingerprint:
                feature_panel = payload["feature_panel"]
                feature_cache_hit = True
                profile["feature_cache_read_seconds"] = perf_counter() - started
            else:
                pipeline.profile = {}
                feature_panel = pipeline.build_feature_panel(source_data)
        else:
            pipeline.profile = {}
            feature_panel = pipeline.build_feature_panel(source_data)

        if not feature_cache_hit:
            profile.update(
                {
                    key: value
                    for key, value in pipeline.profile.items()
                    if key.startswith("build_market_")
                    or key.startswith("build_features_")
                    or key.startswith("build_feature_panel_")
                }
            )
            if self.enabled:
                self._write(
                    feature_cache_path,
                    {
                        "fingerprint": feature_fingerprint,
                        "feature_panel": feature_panel,
                    },
                )

        prepared_fingerprint = self._prepared_fingerprint(feature_fingerprint)
        if self.enabled and prepared_cache_path.exists():
            started = perf_counter()
            payload = self._read(prepared_cache_path)
            if payload.get("fingerprint") == prepared_fingerprint:
                prepared_cache_hit = True
                profile["prepared_cache_read_seconds"] = perf_counter() - started
                profile.setdefault("cache_read_seconds", 0.0)
                profile["cache_read_seconds"] += (
                    profile["prepared_cache_read_seconds"]
                    + profile.get("feature_cache_read_seconds", 0.0)
                    + profile.get("source_cache_read_seconds", 0.0)
                )
                return CacheResult(
                    prepared_data=payload["prepared_data"],
                    cache_hit=True,
                    cache_path=prepared_cache_path,
                    profile=profile,
                    source_cache_hit=source_cache_hit,
                    feature_cache_hit=feature_cache_hit,
                    prepared_cache_hit=True,
                )

        pipeline.profile = {}
        prepared_data = pipeline.finalize_prepared_data(feature_panel)
        profile.update(
            {
                key: value
                for key, value in pipeline.profile.items()
                if key.startswith("finalize_") or key.startswith("attach_")
            }
        )
        if self.enabled:
            self._write(
                prepared_cache_path,
                {
                    "fingerprint": prepared_fingerprint,
                    "prepared_data": prepared_data,
                },
            )
        return CacheResult(
            prepared_data=prepared_data,
            cache_hit=False,
            cache_path=prepared_cache_path,
            profile=profile,
            source_cache_hit=source_cache_hit,
            feature_cache_hit=feature_cache_hit,
            prepared_cache_hit=False,
        )

    def _source_fingerprint(self) -> str:
        source_paths = []
        for key, value in sorted(self.config.paths.items()):
            if key in {"data_dir", "output_dir"}:
                continue
            source_paths.append((key, self._path_metadata(self.config.resolve_path(value))))
        payload = {
            "cache_version": CACHE_VERSION,
            "sources": source_paths,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _feature_fingerprint(self, source_fingerprint: str) -> str:
        payload = {
            "cache_version": CACHE_VERSION,
            "source_fingerprint": source_fingerprint,
            "pipeline_config": self._pipeline_config(FEATURE_STRATEGY_KEYS),
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _prepared_fingerprint(self, feature_fingerprint: str) -> str:
        payload = {
            "cache_version": CACHE_VERSION,
            "feature_fingerprint": feature_fingerprint,
            "pipeline_config": self._pipeline_config(BETA_STRATEGY_KEYS),
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def _pipeline_config(self, strategy_keys: list[str]) -> dict:
        strategy = self.config.strategy
        return {
            "paths": {
                key: value
                for key, value in self.config.paths.items()
                if key not in {"data_dir", "output_dir"}
            },
            "strategy": {key: strategy.get(key) for key in strategy_keys},
        }

    def _path_metadata(self, path: Path) -> dict[str, int | str | bool]:
        if not path.exists():
            return {"path": str(path), "exists": False}
        stat = path.stat()
        return {
            "path": str(path),
            "exists": True,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        }

    def _write(self, path: Path, payload: dict) -> None:
        ensure_directory(path.parent)
        with path.open("wb") as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def _read(self, path: Path) -> dict:
        with path.open("rb") as handle:
            return pickle.load(handle)
