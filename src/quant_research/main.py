from __future__ import annotations

import argparse

from .backtest import Backtester
from .cache import PreparedDataCache
from .config import Config
from .data_sources import MarketDataFetcher
from .exports import export_rebalance_signals
from .pipeline import DataPipeline
from .strategy import MultiSignalStrategy
from .wrds_runner import WRDSExportRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Quant research stack")
    parser.add_argument("command", choices=["fetch", "signals", "backtest", "wrds-export"])
    parser.add_argument("--config", required=True)
    parser.add_argument("--step")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = Config.load(args.config)
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
    if args.command == "signals":
        output_path = export_rebalance_signals(prepared, output_dir)
        print(output_path)
        return
    strategy = MultiSignalStrategy(config.strategy)
    summary = Backtester(
        prepared,
        strategy,
        output_dir=output_dir,
        transaction_cost_bps=float(config.strategy.get("transaction_cost_bps", 10.0)),
    ).run()
    for key, value in summary.items():
        print(f"{key}={value:.8f}")


def _print_profile(profile: dict[str, float]) -> None:
    for key, value in sorted(profile.items()):
        print(f"{key}={value:.6f}")


if __name__ == "__main__":
    main()
