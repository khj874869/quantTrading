# Data Sources and Compliance

## Purpose

This document defines the expected handling model for external data connected to Quant Research Stack.
It is not legal advice.
Every customer must review its own vendor contracts, compliance obligations, and retention rules.

## Source Inventory

The repository currently supports workflows around these categories:

- WRDS-style fundamentals and returns exports;
- FRED macro series;
- Cboe VIX history CSV;
- Financial Modeling Prep analyst upgrade and downgrade data;
- broker fills supplied by the user;
- customer-provided internal CSV inputs.

## Customer Responsibilities

Before paid deployment, the customer must confirm:

- it has lawful access to each upstream dataset;
- it may store and process the data in the chosen environment;
- it may use derived outputs in internal reports;
- it understands any redistribution restrictions;
- it understands any retention or deletion obligations.

## Redistribution Rule

Do not assume that raw vendor data can be forwarded to third parties just because derived analytics were produced from it.
If a customer shares reports externally, the customer must confirm that the underlying vendor license allows the relevant display, extract, or derivative use.

## Secrets and Credentials

Recommended practice:

- keep API keys in environment variables or private config delivery paths;
- do not commit live credentials into version control;
- rotate keys when operators change;
- do not include live credentials in support tickets.

## Storage Model

The toolkit is local-first.
Outputs are written to the configured `output_dir` and optional demo directory.
That means the operator is responsible for:

- local disk encryption or host-level protections;
- backup and retention policy;
- access control over generated CSV, JSON, and HTML artifacts;
- safe deletion of scratch outputs.

## Sensitive Data Handling

Broker fills, proprietary factor data, and customer-only mappings should be treated as sensitive by default.
Do not publish generated reports or manifests without reviewing them for confidential fields and proprietary data exposure.

## Validation Expectation

The `doctor` command helps identify malformed or incomplete CSV inputs.
It does not replace full vendor reconciliation, legal review, or production data quality controls.
