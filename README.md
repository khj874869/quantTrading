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
python run_quant.py backtest --config config/sample_config.json
python run_quant.py report --config config/sample_config.json
python run_quant.py reconcile --config config/sample_config.json
python run_quant.py publish-demo --config config/sample_config.json
python run_quant.py gallery --config config/sample_config.json
```

The sample config is self-contained and intended as the fastest way to inspect the stack.

If you want a hosted demo, the repository now includes a GitHub Pages workflow at `.github/workflows/deploy-pages-demo.yml` that builds the sample bundle and deploys `docs/demo/`.

## Main Outputs

Common files written under the configured `output_dir`:

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
python run_quant.py validate --config config/example_config.json
```

### 2. Generate signals or orders

```bash
python run_quant.py signals --config config/example_config.json
python run_quant.py orders --config config/example_config.json
```

### 3. Run backtest and report

```bash
python run_quant.py backtest --config config/example_config.json
python run_quant.py report --config config/example_config.json
```

### 4. Reconcile expected orders vs fills

```bash
python run_quant.py reconcile --config config/example_config.json
```

### 5. Publish a static demo bundle

```bash
python run_quant.py publish-demo --config config/example_config.json
```

### 6. Build a preset comparison gallery

```bash
python run_quant.py gallery --config config/example_config.json
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
