# Support Policy

This repository is supported as an expert-facing local research toolkit.
Support should be tied to a commercial agreement, order form, or internal ownership assignment before external delivery.

## Supported Scope

Support normally includes:

- installation and upgrade guidance;
- reproducible CLI failures on supported Python versions;
- sample-config smoke test failures;
- config schema interpretation;
- manifest, report, and output-path troubleshooting;
- release-to-release migration guidance for documented breaking changes.

Support normally excludes:

- investment advice or strategy approval;
- custom alpha research;
- proprietary data vendor disputes;
- broker or market-data outages outside this codebase;
- customer-specific feature development unless separately contracted;
- execution, trading, compliance, tax, or regulatory sign-off.

## Severity Model

Use this severity model in customer-facing agreements:

- `sev1`: installation or core CLI workflow is blocked with no workaround;
- `sev2`: a major feature is degraded but a workaround exists;
- `sev3`: non-blocking bug, documentation defect, or usability issue;
- `sev4`: enhancement request or backlog item.

## Response Guidance

Recommended commercial targets:

- `sev1`: acknowledge within 4 business hours;
- `sev2`: acknowledge within 1 business day;
- `sev3`: acknowledge within 2 business days;
- `sev4`: review in normal backlog planning.

The actual response target should be written into the customer-specific agreement.

## Required Information for a Support Request

Every support request should include:

- installed package version from `quant-research --version`;
- Python version and OS;
- exact command used;
- config file section relevant to the issue, with secrets removed;
- failing output or traceback;
- whether the issue reproduces on `config/sample_config.json`;
- whether custom data vendors or customer-only files are involved.

## Public vs Private Channels

Use a public issue only for non-sensitive reproducible defects.
Do not post secrets, customer data, vendor credentials, or private broker files in a public issue.
Security reports should follow `SECURITY.md`.
Commercial customers should use the private support channel named in their order form or statement of work.
