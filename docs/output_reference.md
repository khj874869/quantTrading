# Output Reference

This file summarizes the main artifacts produced by the toolkit and how they should be interpreted.

## Core Validation and Audit Artifacts

- `doctor_report.json`: machine-readable readiness checks for config and inputs.
- `doctor_report.csv`: tabular export of the same checks.
- `doctor_report.html`: human-readable doctor dashboard.
- `validation_summary.json`: data validation summary after running `validate`.
- `run_manifest.json`: execution metadata, config fingerprint, git info, and output inventory.

## Trading Research Artifacts

- `rebalance_signals.csv`: signal output for each rebalance date.
- `order_blotter.csv`: expected orders based on the current strategy configuration.
- `execution_reconciliation.csv`: comparison between expected orders and provided fills.
- `execution_summary.json`: reconciliation summary and cost aggregation.

## Backtest Artifacts

- `portfolio_rebalances.csv`: target portfolio weights by rebalance.
- `portfolio_daily_returns.csv`: daily gross, net, benchmark, and cost fields.
- `summary.json`: headline backtest metrics.
- `execution_diagnostics*.csv`: implementation and backlog diagnostics from the backtest engine.

## Reporting Artifacts

- `report_summary.json`: high-level report metrics and diagnostic summaries.
- `report_dashboard.html`: single-file dashboard for review and sharing.
- `report_monthly_returns.csv`: month-level return and cost breakdown.
- `report_factor_diagnostics.csv`: factor IC and quintile spread diagnostics.
- `report_capacity_curve.csv`: capacity stress estimates across AUM levels.
- `report_stress_scenarios.csv`: regime and scenario breakdowns.

## Demo and Gallery Artifacts

- `docs/demo/index.html`: static demo landing page.
- `docs/demo/gallery.html`: preset comparison gallery.
- `docs/demo/gallery/<preset>/index.html`: preset-specific static bundle.
- `docs/demo/gallery/<preset>/share_card.svg`: share-ready visual summary.
- `docs/demo/latest_winner.json`: spotlight preset summary for downstream reuse.

## Interpretation Warning

Backtest and report artifacts are analytical outputs, not guarantees.
They depend on:

- the quality of the input data;
- the realism of cost and liquidity assumptions;
- the correctness of symbol mapping and timing logic;
- the appropriateness of the strategy parameters for the actual deployment context.
