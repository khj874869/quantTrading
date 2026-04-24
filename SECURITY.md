# Security Policy

## Reporting

Do not disclose vulnerabilities, credentials, customer datasets, or internal network details in a public issue.
Use the private reporting channel defined in the relevant customer agreement, internal incident process, or repository ownership process.

If you do not yet have a private reporting channel, create one before external delivery.
A public repository without a private disclosure path is not ready for commercial distribution.

## Supported Versions

Security fixes should be applied to:

- the latest released version;
- the customer-pinned version if that support commitment exists in contract;
- the current `main` branch while preparing the next release.

## Security Boundaries

This project is local-first.
It does not provide a hosted SaaS control plane, multi-tenant auth layer, or managed secret store.
Operational security therefore depends on the deployment environment as much as the codebase.

Minimum expectations before paid use:

- run on a managed workstation or server with disk access controls;
- keep API keys in environment variables or private config distribution paths;
- avoid committing real credentials into config files;
- treat generated artifacts as potentially sensitive if they include proprietary data;
- review `run_manifest.json` outputs before sharing them externally.

## Current Controls in This Repository

- config redaction for common secret-like keys in manifests;
- local-only default workflow;
- explicit output directories;
- reproducible CI and smoke tests;
- schema and input validation via `doctor`.

## Gaps You Must Handle Operationally

- secret rotation and key escrow;
- encryption at rest for customer datasets;
- role-based access control around generated outputs;
- endpoint protection, patching, and disk backup policy;
- private support and incident communication workflow.

## Hardening Checklist

- confirm no real credentials are committed;
- run `quant-research doctor --strict` before release;
- build and test on a clean environment;
- verify sample outputs do not embed secrets;
- review dependency and Python runtime update policy;
- define rollback procedure in `docs/operations_runbook.md`.
