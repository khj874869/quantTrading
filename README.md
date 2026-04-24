# Quant Research Stack

Quant Research Stack is a local-first equity research and backtesting toolkit built around WRDS-style fundamentals, CRSP-style returns, FRED macro data, Cboe VIX, and analyst revision inputs.

It is designed for one workflow:

1. Build a feature panel from raw research data.
2. Generate signals and constrained portfolios.
3. Backtest with more realistic execution assumptions.
4. Export validation, attribution, execution, and capacity diagnostics.
5. Review everything in a static HTML dashboard.

## Why This Exists

Most small quant projects stop at "a Sharpe ratio in a notebook."

This project goes further:

- Data validation is explicit.
- Run provenance is explicit.
- Capacity and execution realism are explicit.
- Short borrow and locate assumptions are explicit.
- Results are exported as CSV, JSON, and HTML instead of staying trapped in memory.

## Current Scope

The stack currently supports:

- `validate`: data quality diagnostics and rebalance coverage checks
- `doctor`: fast config, input path, CSV schema, sample-row parsing, lightweight data profiling, strategy sanity, and output path readiness checks
- `signals`: rebalance signal export
- `orders`: order blotter generation
- `reconcile`: compare expected orders vs broker fills
- `publish-demo`: build a GitHub Pages friendly static demo bundle
- `gallery`: build a filterable multi-preset strategy gallery under `docs/demo/`
- `backtest`: portfolio simulation with costs and execution controls
- `report`: attribution, factor diagnostics, capacity diagnostics, stress views, and HTML dashboard
- `wrds-export`: WRDS extraction helpers
- `sweep`: parameter sweep research
- `walk-forward`: walk-forward selection loop
- `apply-recommended`: apply a recommended config before another command

## Quick Start

Install with Python 3.11+ and run from the repo root.

```bash
python -m pip install -e .[dev]
quant-research doctor --config config/sample_config.json
quant-research backtest --config config/sample_config.json
quant-research report --config config/sample_config.json
quant-research reconcile --config config/sample_config.json
quant-research publish-demo --config config/sample_config.json
quant-research gallery --config config/sample_config.json
```

The sample config is self-contained and intended as the fastest way to inspect the stack.

If you prefer the module entrypoint after installation, `python -m quant_research ...` is also supported.
The legacy `python run_quant.py ...` wrapper remains available for source checkouts that have not been installed yet.
You can also set `QUANT_RESEARCH_CONFIG=config/sample_config.json` to avoid repeating `--config` on every command.
Both JSON and TOML config files are supported, so `--config config/sample_config.toml` works as long as the file uses the same section layout.
Use `quant-research --version` to confirm the installed CLI version.
Use `--output-dir` and `--demo-site-dir` when you want scratch runs or CI artifacts without editing the base config file.
Run `python -m build` when you want to verify the sdist and wheel before publishing or attaching artifacts.

If you want a hosted demo, the repository now includes a GitHub Pages workflow at `.github/workflows/deploy-pages-demo.yml` that builds the sample bundle and deploys `docs/demo/`.
The workflow asks GitHub to enable Pages for Actions-based publishing and always uploads the built site as an artifact; if your repository policy blocks automatic enablement, enable Pages manually in repository settings and select GitHub Actions as the source.
The repository also includes `.github/workflows/ci.yml` to verify editable installs, the packaged CLI, strict doctor checks, and sample-data smoke runs across supported Python versions.

## Main Outputs

Common files written under the configured `output_dir`:

- `doctor_report.json`
- `doctor_report.csv`
- `doctor_report.html` with status filters
- `validation_summary.json`
- `rebalance_signals.csv`
- `portfolio_rebalances.csv`
- `portfolio_daily_returns.csv`
- `summary.json`
- `run_manifest.json`
- `report_summary.json`
- `report_dashboard.html`
- `order_blotter.csv`
- `execution_reconciliation.csv`
- `docs/demo/index.html`
- `docs/demo/gallery.html`
- `docs/demo/gallery/<preset>/share_card.svg`
- `docs/demo/latest_winner.json`
- `docs/demo/latest_winner_badge.svg`
- `docs/demo/latest_winner_readme_snippet.md`
- `docs/demo/latest_winner_release_note.md`
- `docs/demo/latest_winner_social_post.txt`

When `gallery` is generated, the root `docs/demo/index.html` is refreshed to spotlight the latest top preset, its share card, and channel-ready winner snippets for README, release notes, and social copy.

The fastest artifact to inspect is:

- `config/output/report_dashboard.html`

## Doctor Checks

Run `doctor` before heavier validation, backtests, or CI smoke runs when you want a fast readiness check.

```bash
quant-research doctor --config config/example_config.json
quant-research doctor --config config/example_config.json --strict --json
quant-research doctor --config config/example_config.json --profile-row-limit 100
```

`doctor` checks configured input paths, required CSV headers, sample row date and numeric parsing, required-column blank rates, output directory writability, and core strategy settings. By default it profiles up to 1000 rows per CSV; use `--profile-row-limit` to trade speed against broader sampling.

The command writes JSON, CSV, and filterable HTML reports under `output_dir`. `--strict` exits nonzero on warnings as well as failures, which is useful for CI.

## Research Features

- Universe filters with sector include/exclude and top-N market-cap controls
- Benchmark selection with explicit benchmark mode
- Long-only and long/short portfolio paths
- Borrow cost and short locate constraints
- Heuristic and optimizer-based portfolio construction
- Covariance-aware optimizer penalty
- Adaptive turnover budgeting
- Multi-day execution simulation
- Capacity curve and participation breach analysis
- Execution reconciliation against broker fills
- Factor IC, quintile spread, and regime diagnostics
- Stress scenario breakdowns

## Typical Workflow

### 1. Validate data quality

```bash
quant-research doctor --config config/example_config.json
quant-research doctor --config config/example_config.json --strict --json
quant-research doctor --config config/example_config.json --profile-row-limit 100
quant-research validate --config config/example_config.json
```

### 2. Generate signals or orders

```bash
quant-research signals --config config/example_config.json
quant-research orders --config config/example_config.json
```

### 3. Run backtest and report

```bash
quant-research backtest --config config/example_config.json
quant-research report --config config/example_config.json
```

### 4. Reconcile expected orders vs fills

```bash
quant-research reconcile --config config/example_config.json
```

### 5. Publish a static demo bundle

```bash
quant-research publish-demo --config config/example_config.json
```

### 6. Build a preset comparison gallery

```bash
quant-research gallery --config config/example_config.json
```

## Project Layout

- `src/quant_research/`: core implementation
- `config/`: sample and example configs
- `data/`: sample inputs and local datasets
- `docs/`: workflow docs and static landing material
- `sql/wrds/`: WRDS extraction SQL
- `tests/`: regression coverage

## Notes

- The project is local-first, not hosted SaaS.
- The current hosted surface is a sample-demo Pages site, not a full multi-user product.
- If you want external adoption, start by sharing `report_dashboard.html` and a cleaned config/output bundle.

## WRDS Workflow

See:

- `docs/wrds_workflow.md`
- `docs/commercial_readiness.md`
- `docs/config_reference.md`
- `docs/output_reference.md`
- `docs/data_sources_and_compliance.md`
- `docs/operations_runbook.md`
- `SUPPORT.md`
- `SECURITY.md`
- `CHANGELOG.md`

## Commercial Use

This repository is positioned as an expert-facing research toolkit, not a retail consumer app and not an investment advisory service.

Use the following documents before distributing it outside your own desk or team:

- `LICENSE.txt`: proprietary license baseline
- `SUPPORT.md`: support scope, response model, and escalation inputs
- `SECURITY.md`: disclosure process and operational security boundaries
- `docs/commercial_readiness.md`: target user, product boundary, and release acceptance checklist
- `docs/data_sources_and_compliance.md`: source-specific usage responsibilities
- `docs/operations_runbook.md`: install, release, smoke test, and rollback process

Before any paid delivery, make sure the customer-specific order form or statement of work defines:

- licensed users or desks
- support window and response targets
- permitted data sources
- update cadence
- acceptance criteria
- commercial contact and private security reporting channel
