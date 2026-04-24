# Changelog

All notable changes to this project should be recorded in this file.

The format is intentionally simple:

- one section per released version;
- operationally relevant changes first;
- note breaking changes explicitly;
- keep customer-visible behavior separate from internal refactors when possible.

## Unreleased

- No unreleased entries yet.

## 0.4.0 - 2026-04-24

- Added TOML config support alongside JSON config loading.
- Added shared config override helpers for paths and strategy sections.
- Refactored CLI execution flow into clearer state-driven handlers while preserving command behavior.
- Centralized backtest cost and order blotter setting resolution.
- Improved data source fetching with injectable network client, timeout configuration, response validation, and symbol normalization.
- Added packaging metadata for build verification, coverage config, and typed package marker.
- Added commercial-readiness documentation, support policy, security policy, changelog, license file, operations runbook, config reference, output reference, and data/compliance guide.
- Expanded regression coverage for config upgrades, CLI manifests, utility helpers, and market-data fetching.
