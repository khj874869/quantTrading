# Config Reference

Quant Research Stack config files use a sectioned object model.
Both JSON and TOML are supported.

## `paths`

Defines file and directory locations used by the workflow.
Common keys:

- `output_dir`
- `demo_site_dir`
- `compustat_quarterly`
- `crsp_daily`
- `ccm_link`
- `ibes_link`
- `ibes_summary`
- `ibes_surprise`
- `kpss_patent`
- `ff_factors`
- `fred_dgs10`
- `cboe_vix`
- `fmp_grades`
- `broker_fills`

## `api`

Defines remote data access settings.
Common keys:

- `fred_api_key`
- `fmp_api_key`
- `fmp_symbols`
- `cboe_vix_csv_url`
- `request_timeout_seconds`

## `wrds`

Defines WRDS extraction settings.
Common keys:

- `host`
- `port`
- `database`
- `sslmode`
- `username_env`
- `password_env`
- `fetch_size`
- `placeholders`
- `exports`

## `cache`

Controls prepared-data caching.
Common keys:

- `enabled`
- `cache_dir`

## `sweep`

Controls parameter sweep combinations.
Common keys:

- `strategy_grid`

## `walk_forward`

Controls walk-forward window generation and leaderboard logic.
Common keys:

- `selection_metric`
- `leaderboard_top_n`
- `leaderboard_sort_by`
- `leaderboard_min_selection_rate`
- `leaderboard_min_positive_window_rate`
- `train_months`
- `test_months`
- `step_months`
- `windows`

## `strategy`

Controls universe, scoring, portfolio construction, costs, capacity, and reporting assumptions.
The fastest reference remains `config/sample_config.json` and `config/example_config.json`.

## Environment Variables

Supported environment-driven paths and credentials include:

- `QUANT_RESEARCH_CONFIG`
- provider-specific API key env vars if you choose not to store them in config
- WRDS username and password variables configured under `wrds.username_env` and `wrds.password_env`

## Override Model

CLI path overrides:

- `--output-dir`
- `--demo-site-dir`

Recommended-config workflow:

- `apply-recommended` writes an applied config artifact;
- the manifest records both the requested and executed command.
