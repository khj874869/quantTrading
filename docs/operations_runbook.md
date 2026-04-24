# Operations Runbook

## Supported Runtime Baseline

- Python `3.11` or newer;
- local filesystem write access to the configured output path;
- access to required external data files and APIs;
- reproducible install from repository root.

## Standard Install

```bash
python -m pip install -e .[dev]
quant-research --version
quant-research doctor --config config/sample_config.json --strict
```

## Release Checklist

Run this before shipping a version to a customer:

```bash
python -m build
pytest -q
quant-research doctor --config config/sample_config.json --strict --json
quant-research backtest --config config/sample_config.json --output-dir test_output/release-output
quant-research report --config config/sample_config.json --output-dir test_output/release-output
quant-research publish-demo --config config/sample_config.json --output-dir test_output/release-output --demo-site-dir test_output/release-demo
```

Then confirm:

- `CHANGELOG.md` contains the release entry;
- support and security docs still match the operating model;
- no live secrets are present in sample configs or generated artifacts;
- customer-facing acceptance criteria are still accurate.

## Upgrade Procedure

- build and test the new version in a clean environment;
- back up the previous packaged build and the customer config;
- run `doctor` with the customer config first;
- run the customer smoke workflow on a scratch output directory;
- compare headline metrics and key artifact presence against the prior version;
- promote only after the comparison is reviewed.

## Rollback Procedure

- restore the previously approved package version;
- restore the previously approved config if a config migration shipped with the release;
- rerun `doctor` and the customer smoke workflow;
- archive the failed release artifacts for later debugging.

## Minimum Smoke Workflow for a Customer Config

```bash
quant-research doctor --config path/to/customer_config.json --strict
quant-research validate --config path/to/customer_config.json
quant-research backtest --config path/to/customer_config.json
quant-research report --config path/to/customer_config.json
```

If the customer uses execution reconciliation or demo publishing, include:

```bash
quant-research reconcile --config path/to/customer_config.json
quant-research publish-demo --config path/to/customer_config.json
```

## Incident Handling

For every production-facing incident, capture:

- installed version;
- Python version and host environment;
- command that failed;
- relevant traceback or manifest;
- whether sample config still passes;
- whether the issue is data-specific or code-specific.
