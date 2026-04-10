from __future__ import annotations

import argparse

from .backtest import Backtester
from .cache import PreparedDataCache
from .config import Config
from .data_sources import MarketDataFetcher
from .exports import export_rebalance_signals
from .pipeline import DataPipeline
from .research import apply_recommended_config, run_parameter_sweep, run_walk_forward_optimization, write_applied_recommended_config
from .strategy import MultiSignalStrategy
from .wrds_runner import WRDSExportRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Quant research stack")
    parser.add_argument("command", choices=["fetch", "signals", "backtest", "wrds-export", "sweep", "walk-forward", "apply-recommended"])
    parser.add_argument("--config", required=True)
    parser.add_argument("--step")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--recommended-config")
    parser.add_argument("--applied-config-output")
    parser.add_argument("--target", choices=["signals", "backtest"], default="backtest")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = Config.load(args.config)
    effective_command = args.command
    if args.command == "apply-recommended":
        original_config = config
        config = apply_recommended_config(config, args.recommended_config)
        applied_config_path = write_applied_recommended_config(
            original_config,
            config,
            recommendation_path=args.recommended_config,
            output_path=args.applied_config_output,
        )
        print(applied_config_path)
        effective_command = args.target
    if args.command == "fetch":
        outputs = MarketDataFetcher(config).fetch_all()
        for output in outputs:
            print(output)
        return
    if args.command == "wrds-export":
        outputs = WRDSExportRunner(config).export(step=args.step, dry_run=args.dry_run)
        for output in outputs:
            print(output)
        return
    if effective_command == "sweep":
        output_path = run_parameter_sweep(config)
        print(output_path)
        return
    if effective_command == "walk-forward":
        output_path = run_walk_forward_optimization(config)
        print(output_path)
        return
    if args.no_cache:
        pipeline = DataPipeline(config)
        prepared = pipeline.load()
        _print_profile(pipeline.profile)
    else:
        cache_result = PreparedDataCache(config).load_or_build()
        prepared = cache_result.prepared_data
        print(f"cache_hit={int(cache_result.cache_hit)}")
        print(f"source_cache_hit={int(cache_result.source_cache_hit)}")
        print(f"feature_cache_hit={int(cache_result.feature_cache_hit)}")
        print(f"prepared_cache_hit={int(cache_result.prepared_cache_hit)}")
        _print_profile(cache_result.profile)
    output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
    if effective_command == "signals":
        output_path = export_rebalance_signals(prepared, output_dir)
        print(output_path)
        return
    strategy = MultiSignalStrategy(config.strategy)
    summary = Backtester(
        prepared,
        strategy,
        output_dir=output_dir,
        transaction_cost_bps=float(config.strategy.get("transaction_cost_bps", 10.0)),
        commission_cost_bps=float(config.strategy.get("commission_cost_bps", 0.0)),
        slippage_cost_bps=float(config.strategy.get("slippage_cost_bps", max(float(config.strategy.get("transaction_cost_bps", 10.0)) - float(config.strategy.get("commission_cost_bps", 0.0)), 0.0))),
    ).run()
    for key, value in summary.items():
        print(f"{key}={value:.8f}")


def _print_profile(profile: dict[str, float]) -> None:
    for key, value in sorted(profile.items()):
        print(f"{key}={value:.6f}")


if __name__ == "__main__":
    main()
