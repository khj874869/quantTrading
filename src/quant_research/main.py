from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
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
from .pipeline import DataPipeline, PreparedData
from .publishing import DemoPublisher
from .reporting import PerformanceReporter
from .research import apply_recommended_config, run_parameter_sweep, run_walk_forward_optimization, write_applied_recommended_config
from .strategy import MultiSignalStrategy
from .utils import resolve_backtest_costs, resolve_order_blotter_settings
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


@dataclass(slots=True)
class CliState:
    parser: argparse.ArgumentParser
    args: argparse.Namespace
    argv: list[str]
    config: Config
    requested_command: str
    effective_command: str
    manifest_outputs: list[Path] = field(default_factory=list)
    manifest_profile: dict[str, float] = field(default_factory=dict)
    manifest_cache: dict[str, object] = field(default_factory=lambda: {"used": False})
    manifest_summary: dict[str, object] | None = None
    manifest_extra: dict[str, object] = field(default_factory=dict)


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
    state = _build_cli_state(parser, args, sys.argv[1:])
    _apply_recommended_target(state)
    manifest_writer = RunManifestWriter(state.config)
    if _run_fast_command(state, manifest_writer):
        return
    prepared, output_dir, universe_snapshot_path = _load_runtime_data(state)
    _run_prepared_command(state, manifest_writer, prepared, output_dir, universe_snapshot_path)


def _build_cli_state(parser: argparse.ArgumentParser, args: argparse.Namespace, argv: list[str]) -> CliState:
    config = Config.load(_resolve_config_path(parser, args)).with_path_overrides(
        output_dir=args.output_dir,
        demo_site_dir=args.demo_site_dir,
    )
    return CliState(
        parser=parser,
        args=args,
        argv=argv,
        config=config,
        requested_command=args.command,
        effective_command=args.command,
    )


def _apply_recommended_target(state: CliState) -> None:
    if state.requested_command != "apply-recommended":
        return
    original_config = state.config
    state.config = apply_recommended_config(state.config, state.args.recommended_config)
    applied_config_path = write_applied_recommended_config(
        original_config,
        state.config,
        recommendation_path=state.args.recommended_config,
        output_path=state.args.applied_config_output,
    )
    print(applied_config_path)
    state.manifest_outputs.append(applied_config_path)
    state.manifest_extra["applied_recommended_config_path"] = str(applied_config_path)
    state.manifest_extra["recommended_config_path"] = (
        str(state.args.recommended_config) if state.args.recommended_config else None
    )
    state.effective_command = state.args.target


def _run_fast_command(state: CliState, manifest_writer: RunManifestWriter) -> bool:
    if state.requested_command == "fetch":
        outputs = MarketDataFetcher(state.config).fetch_all()
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        _write_state_manifest(manifest_writer, state)
        return True
    if state.requested_command == "doctor":
        outputs, doctor_summary = ConfigDoctor(
            state.config,
            profile_row_limit=state.args.profile_row_limit,
        ).run()
        state.manifest_outputs.extend(outputs)
        state.manifest_summary = doctor_summary
        if not state.args.json:
            _print_paths(outputs)
            for check in doctor_summary["checks"]:
                print(f"{check['status']} {check['category']}.{check['name']}: {check['message']}")
        manifest_path = _write_state_manifest(manifest_writer, state, print_path=not state.args.json)
        exit_code = _doctor_exit_code(doctor_summary, strict=state.args.strict)
        if state.args.json:
            json_payload = {
                **doctor_summary,
                "strict": state.args.strict,
                "exit_code": exit_code,
                "outputs": [str(output) for output in outputs],
                "manifest_path": str(manifest_path),
            }
            print(json.dumps(json_payload, indent=2))
        if exit_code:
            raise SystemExit(1)
        return True
    if state.requested_command == "wrds-export":
        outputs = WRDSExportRunner(state.config).export(step=state.args.step, dry_run=state.args.dry_run)
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        state.manifest_extra["dry_run"] = state.args.dry_run
        if state.args.step:
            state.manifest_extra["wrds_step"] = state.args.step
        _write_state_manifest(manifest_writer, state)
        return True
    if state.requested_command == "validate":
        outputs = DataValidator(state.config).run()
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        _write_state_manifest(manifest_writer, state)
        return True
    if state.requested_command == "publish-demo":
        outputs, publish_summary = DemoPublisher(state.config).publish()
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        state.manifest_summary = publish_summary
        _write_state_manifest(manifest_writer, state)
        return True
    if state.effective_command == "sweep":
        output_path = run_parameter_sweep(state.config)
        print(output_path)
        state.manifest_outputs.append(output_path)
        _write_state_manifest(manifest_writer, state)
        return True
    if state.effective_command == "walk-forward":
        output_path = run_walk_forward_optimization(state.config)
        print(output_path)
        state.manifest_outputs.append(output_path)
        _write_state_manifest(manifest_writer, state)
        return True
    return False


def _load_runtime_data(state: CliState) -> tuple[PreparedData, Path, Path]:
    if state.args.no_cache:
        pipeline = DataPipeline(state.config)
        prepared = pipeline.load()
        state.manifest_profile = pipeline.profile
        state.manifest_cache = {"used": False, "no_cache": True}
        _print_profile(state.manifest_profile)
    else:
        cache_result = PreparedDataCache(state.config).load_or_build()
        prepared = cache_result.prepared_data
        print(f"cache_hit={int(cache_result.cache_hit)}")
        print(f"source_cache_hit={int(cache_result.source_cache_hit)}")
        print(f"feature_cache_hit={int(cache_result.feature_cache_hit)}")
        print(f"prepared_cache_hit={int(cache_result.prepared_cache_hit)}")
        state.manifest_profile = cache_result.profile
        state.manifest_cache = {
            "used": True,
            "cache_hit": cache_result.cache_hit,
            "source_cache_hit": cache_result.source_cache_hit,
            "feature_cache_hit": cache_result.feature_cache_hit,
            "prepared_cache_hit": cache_result.prepared_cache_hit,
            "cache_path": str(cache_result.cache_path),
        }
        _print_profile(state.manifest_profile)
    output_dir = state.config.resolve_path(state.config.paths.get("output_dir", "output"))
    universe_snapshot_path = export_universe_snapshot(
        prepared,
        output_dir,
        benchmark_mode=str(state.config.strategy.get("benchmark_mode", "ff_total_return")),
    )
    return prepared, output_dir, universe_snapshot_path


def _run_prepared_command(
    state: CliState,
    manifest_writer: RunManifestWriter,
    prepared: PreparedData,
    output_dir: Path,
    universe_snapshot_path: Path,
) -> None:
    if state.effective_command == "signals":
        output_path = export_rebalance_signals(prepared, output_dir)
        print(output_path)
        state.manifest_outputs.append(output_path)
        state.manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        _write_state_manifest(manifest_writer, state)
        return
    if state.effective_command == "orders":
        strategy = MultiSignalStrategy(state.config.strategy)
        blotter_settings = resolve_order_blotter_settings(state.config.strategy)
        outputs = export_order_blotter(
            prepared,
            strategy,
            output_dir,
            blotter_notional=float(blotter_settings["blotter_notional"]),
            order_type=str(blotter_settings["order_type"]),
        )
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        state.manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        _write_state_manifest(manifest_writer, state)
        return
    if state.effective_command == "reconcile":
        outputs, execution_summary = ExecutionReconciler(state.config, prepared, output_dir).run()
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        state.manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        state.manifest_summary = execution_summary
        _write_state_manifest(manifest_writer, state)
        return
    if state.effective_command == "report":
        outputs, report_summary = PerformanceReporter(state.config, prepared, output_dir).run()
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        state.manifest_outputs.append(universe_snapshot_path)
        print(universe_snapshot_path)
        state.manifest_summary = report_summary
        _write_state_manifest(manifest_writer, state)
        return
    if state.effective_command == "gallery":
        outputs, gallery_summary = StrategyGalleryBuilder(state.config, prepared).publish()
        _print_paths(outputs)
        state.manifest_outputs.extend(outputs)
        state.manifest_summary = gallery_summary
        _write_state_manifest(manifest_writer, state)
        return
    strategy = MultiSignalStrategy(state.config.strategy)
    costs = resolve_backtest_costs(state.config.strategy)
    summary = Backtester(
        prepared,
        strategy,
        output_dir=output_dir,
        **costs,
    ).run()
    state.manifest_outputs.extend(_backtest_outputs(output_dir))
    state.manifest_outputs.append(universe_snapshot_path)
    state.manifest_summary = {key: float(value) for key, value in summary.items()}
    _write_state_manifest(manifest_writer, state)
    for key, value in summary.items():
        print(f"{key}={value:.8f}")


def _print_paths(paths: list[Path]) -> None:
    for path in paths:
        print(path)


def _write_state_manifest(
    manifest_writer: RunManifestWriter,
    state: CliState,
    print_path: bool = True,
) -> Path:
    return _write_manifest(
        manifest_writer,
        state.args,
        state.argv,
        state.requested_command,
        state.effective_command,
        state.manifest_outputs,
        summary=state.manifest_summary,
        profile=state.manifest_profile,
        cache=state.manifest_cache,
        extra=state.manifest_extra,
        print_path=print_path,
    )


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
