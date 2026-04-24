# Commercial Readiness

## Product Definition

Quant Research Stack is a local-first research and backtesting toolkit for expert users who already understand market data, portfolio construction, and model validation.

Recommended target users:

- independent quant researchers;
- small research teams;
- internal strategy or PM tooling teams;
- consultants delivering custom research workflows.

Not a target product shape:

- unmanaged retail consumer app;
- copy-trading or brokerage frontend;
- investment advisory or fiduciary service;
- black-box auto-trading service with guaranteed outcomes.

## Supported Sale Modes

These modes are realistic for the current system:

- paid pilot with installation and onboarding support;
- internal-use commercial license for a desk or research team;
- consulting-led delivery with source access and configuration support.

These modes require more infrastructure before launch:

- consumer self-serve SaaS;
- multi-tenant hosted analytics product;
- managed execution service;
- regulated advice product.

## Release Acceptance Checklist

Treat a release as commercially acceptable only when all of the following are true:

- `python -m build` succeeds;
- `pytest -q` succeeds;
- CI smoke workflow passes on supported Python versions;
- sample config completes `doctor`, `backtest`, `report`, and `publish-demo`;
- support scope is current in `SUPPORT.md`;
- security guidance is current in `SECURITY.md`;
- data-source obligations are current in `docs/data_sources_and_compliance.md`;
- changelog entry exists for the version being shipped;
- customer-facing limitations are documented;
- rollback instructions are current in `docs/operations_runbook.md`.

## Commercial Boundary

The software should be sold with these statements intact:

- backtests are hypothetical and not predictive guarantees;
- the customer is responsible for lawful access to external data;
- the customer is responsible for validating live trading suitability;
- support covers the toolkit, not all upstream data or broker behavior;
- customer-specific SLAs and support channels belong in the order form or statement of work.

## Customer Onboarding Minimum

Before a customer starts using the software, confirm:

- supported Python version;
- customer-owned output directory;
- customer-owned config distribution method;
- who owns API keys and vendor contracts;
- who receives support requests;
- who approves upgrades in production or research environments.
