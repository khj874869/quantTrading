from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from . import __version__
from .backtest import Backtester
from .cache import PreparedDataCache
from .config import Config
from .data_sources import MarketDataFetcher
from .doctor import ConfigDoctor
from .execution import ExecutionReconciler
from .exports import export_order_blotter, export_rebalance_signals, export_universe_snapshot
from .gallery import StrategyGalleryBuilder
from .manifest import RunManifestWriter
from .pipeline import DataPipeline
from .publishing import DemoPublisher
from .reporting import PerformanceReporter
from .research import apply_recommended_config, run_parameter_sweep, run_walk_forward_optimization, write_applied_recommended_config
from .strategy import MultiSignalStrategy
from .validation import DataValidator
from .wrds_runner import WRDSExportRunner

COMMANDS = [
    "fetch",
    "doctor",
    "signals",
    "orders",
    "reconcile",
    "publish-demo",
    "gallery",
    "backtest",
    "report",
    "wrds-export",
    "sweep",
    "walk-forward",
    "apply-recommended",
    "validate",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quant-research",
        description="Quant research stack",
    )
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--config")
    parser.add_argument("--step")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-cache", action="store_true")
    parser.add_argument("--recommended-config")
    parser.add_argument("--applied-config-output")
    parser.add_argument("--target", choices=["signals", "backtest"], default="backtest")
    parser.add_argument("--output-dir")
    parser.add_argument("--demo-site-dir")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--profile-row-limit", type=int, default=1000)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    argv = sys.argv[1:]
    config = Config.load(_resolve_config_path(parser, args)).with_path_overrides(
        output_dir=args.output_dir,
        demo_site_dir=args.demo_site_dir,
    )
    requested_command = args.command
    effective_command = args.command
    manifest_outputs: list[Path] = []
    manifest_profile: dict[str, float] = {}
    manifest_cache: dict[str, object] = {"used": False}
    manifest_summary: dict[str, object] | None = None
    manifest_extra: dict[str, object] = {}
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
        manifest_outputs.append(applied_config_path)
        manifest_extra["applied_recommended_config_path"] = str(applied_config_path)
        manifest_extra["recommended_config_path"] = str(args.recommended_config) if args.recommended_config else None
        effective_command = args.target
    manifest_writer = RunManifestWriter(config)
    if args.command == "fetch":
        outputs = MarketDataFetcher(config).fetch_all()
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if args.command == "doctor":
        outputs, doctor_summary = ConfigDoctor(config, profile_row_limit=args.profile_row_limit).run()
        manifest_outputs.extend(outputs)
        manifest_summary = doctor_summary
        if not args.json:
            for output in outputs:
                print(output)
            for check in doctor_summary["checks"]:
                print(f"{check['status']} {check['category']}.{check['name']}: {check['message']}")
        manifest_path = _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
            print_path=not args.json,
        )
        exit_code = _doctor_exit_code(doctor_summary, strict=args.strict)
        if args.json:
            json_payload = {
                **doctor_summary,
                "strict": args.strict,
                "exit_code": exit_code,
                "outputs": [str(output) for output in outputs],
                "manifest_path": str(manifest_path),
            }
            print(json.dumps(json_payload, indent=2))
        if exit_code:
            raise SystemExit(1)
        return
    if args.command == "wrds-export":
        outputs = WRDSExportRunner(config).export(step=args.step, dry_run=args.dry_run)
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        manifest_extra["dry_run"] = args.dry_run
        if args.step:
            manifest_extra["wrds_step"] = args.step
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if args.command == "validate":
        outputs = DataValidator(config).run()
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if args.command == "publish-demo":
        outputs, publish_summary = DemoPublisher(config).publish()
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        manifest_summary = publish_summary
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if effective_command == "sweep":
        output_path = run_parameter_sweep(config)
        print(output_path)
        manifest_outputs.append(output_path)
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if effective_command == "walk-forward":
        output_path = run_walk_forward_optimization(config)
        print(output_path)
        manifest_outputs.append(output_path)
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if args.no_cache:
        pipeline = DataPipeline(config)
        prepared = pipeline.load()
        manifest_profile = pipeline.profile
        manifest_cache = {"used": False, "no_cache": True}
        _print_profile(manifest_profile)
    else:
        cache_result = PreparedDataCache(config).load_or_build()
        prepared = cache_result.prepared_data
        print(f"cache_hit={int(cache_result.cache_hit)}")
        print(f"source_cache_hit={int(cache_result.source_cache_hit)}")
        print(f"feature_cache_hit={int(cache_result.feature_cache_hit)}")
        print(f"prepared_cache_hit={int(cache_result.prepared_cache_hit)}")
        manifest_profile = cache_result.profile
        manifest_cache = {
            "used": True,
            "cache_hit": cache_result.cache_hit,
            "source_cache_hit": cache_result.source_cache_hit,
            "feature_cache_hit": cache_result.feature_cache_hit,
            "prepared_cache_hit": cache_result.prepared_cache_hit,
            "cache_path": str(cache_result.cache_path),
        }
        _print_profile(manifest_profile)
    output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
    universe_snapshot_path = export_universe_snapshot(
        prepared,
        output_dir,
        benchmark_mode=str(config.strategy.get("benchmark_mode", "ff_total_return")),
    )
    if effective_command == "signals":
        output_path = export_rebalance_signals(prepared, output_dir)
        print(output_path)
        manifest_outputs.append(output_path)
        manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if effective_command == "orders":
        strategy = MultiSignalStrategy(config.strategy)
        outputs = export_order_blotter(
            prepared,
            strategy,
            output_dir,
            blotter_notional=float(config.strategy.get("order_blotter_notional", config.strategy.get("slippage_notional", config.strategy.get("capacity_baseline_aum", 1_000_000.0)))),
            order_type=str(config.strategy.get("order_blotter_order_type", "MOC")),
        )
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if effective_command == "reconcile":
        outputs, execution_summary = ExecutionReconciler(config, prepared, output_dir).run()
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        manifest_summary = execution_summary
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if effective_command == "report":
        outputs, report_summary = PerformanceReporter(config, prepared, output_dir).run()
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        manifest_summary = report_summary
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
        return
    if effective_command == "gallery":
        outputs, gallery_summary = StrategyGalleryBuilder(config, prepared).publish()
        for output in outputs:
            print(output)
        manifest_outputs.extend(outputs)
        manifest_summary = gallery_summary
        _write_manifest(
            manifest_writer,
            args,
            argv,
            requested_command,
            effective_command,
            manifest_outputs,
            summary=manifest_summary,
            profile=manifest_profile,
            cache=manifest_cache,
            extra=manifest_extra,
        )
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
    manifest_outputs.extend(_backtest_outputs(output_dir))
    manifest_outputs.append(universe_snapshot_path)
    manifest_summary = {key: float(value) for key, value in summary.items()}
    _write_manifest(
        manifest_writer,
        args,
        argv,
        requested_command,
        effective_command,
        manifest_outputs,
        summary=manifest_summary,
        profile=manifest_profile,
        cache=manifest_cache,
        extra=manifest_extra,
    )
    for key, value in summary.items():
        print(f"{key}={value:.8f}")


def _print_profile(profile: dict[str, float]) -> None:
    for key, value in sorted(profile.items()):
        print(f"{key}={value:.6f}")


def _write_manifest(
    manifest_writer: RunManifestWriter,
    args: argparse.Namespace,
    argv: list[str],
    requested_command: str,
    executed_command: str,
    outputs: list[Path],
    summary: dict[str, object] | None,
    profile: dict[str, float],
    cache: dict[str, object],
    extra: dict[str, object],
    print_path: bool = True,
) -> Path:
    manifest_path = manifest_writer.write(
        args=args,
        argv=argv,
        requested_command=requested_command,
        executed_command=executed_command,
        outputs=outputs,
        summary=summary,
        profile=profile,
        cache=cache,
        extra=extra,
    )
    if print_path:
        print(manifest_path)
    return manifest_path


def _doctor_exit_code(summary: dict[str, object], strict: bool) -> int:
    if summary["status"] == "fail":
        return 1
    if strict and summary["status"] == "warn":
        return 1
    return 0


def _backtest_outputs(output_dir: Path) -> list[Path]:
    return [
        output_dir / "portfolio_rebalances.csv",
        output_dir / "portfolio_daily_returns.csv",
        output_dir / "summary.json",
        output_dir / "execution_diagnostics.csv",
        output_dir / "execution_diagnostics_by_bucket.csv",
        output_dir / "execution_diagnostics_by_bucket_timeseries.csv",
        output_dir / "execution_backlog_aging.csv",
        output_dir / "execution_backlog_aging_events.csv",
        output_dir / "execution_backlog_dropoff.csv",
        output_dir / "execution_backlog_dropoff_events.csv",
        output_dir / "execution_backlog_dropoff_timeseries.csv",
        output_dir / "execution_backlog_dropoff_by_regime.csv",
    ]


def _resolve_config_path(parser: argparse.ArgumentParser, args: argparse.Namespace) -> str:
    if args.config:
        return str(args.config)
    env_config_path = os.getenv("QUANT_RESEARCH_CONFIG")
    if env_config_path:
        return env_config_path
    parser.error("--config is required unless QUANT_RESEARCH_CONFIG is set")
    raise AssertionError("argparse error should have exited")


if __name__ == "__main__":
    main()
