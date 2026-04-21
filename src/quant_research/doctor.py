from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from .config import Config
from .doctor_report import render_doctor_html_report
from .utils import ensure_directory, float_or_none, parse_date, parse_optional_date, write_csv_dicts


REQUIRED_INPUT_PATHS = [
    "compustat_quarterly",
    "crsp_daily",
    "ccm_link",
    "ibes_link",
    "ibes_summary",
    "ibes_surprise",
    "kpss_patent",
    "ff_factors",
    "fred_dgs10",
    "cboe_vix",
    "fmp_grades",
]

OPTIONAL_INPUT_PATHS = [
    "broker_fills",
]

SAMPLE_ROW_LIMIT = 25
PROFILE_ROW_LIMIT = 1000
PROFILE_WARN_BLANK_RATE = 0.50

INPUT_SCHEMAS = {
    "compustat_quarterly": [
        ("gvkey",),
        ("datadate",),
        ("atq",),
        ("ceqq",),
        ("saleq",),
        ("ibq",),
        ("oancfy",),
    ],
    "crsp_daily": [
        ("permno",),
        ("date",),
        ("ret",),
        ("prc",),
        ("shrout",),
    ],
    "ccm_link": [
        ("gvkey",),
        ("permno",),
        ("linkdt",),
    ],
    "ibes_link": [
        ("ticker",),
        ("permno",),
        ("linkdt",),
    ],
    "ibes_summary": [
        ("ticker",),
        ("statpers",),
        ("meanest",),
        ("stdev",),
        ("numest",),
    ],
    "ibes_surprise": [
        ("ticker",),
        ("statpers",),
        ("actual",),
        ("surprise",),
        ("surpct",),
    ],
    "kpss_patent": [
        ("gvkey",),
        ("issue_date",),
        ("patent_count",),
        ("citation_count",),
    ],
    "ff_factors": [
        ("date",),
        ("mktrf",),
        ("rf",),
    ],
    "fred_dgs10": [
        ("date",),
        ("value",),
    ],
    "cboe_vix": [
        ("DATE", "date"),
        ("CLOSE", "close", "Close"),
    ],
    "fmp_grades": [
        ("symbol",),
        ("publishedDate", "date"),
    ],
    "broker_fills": [
        ("rebalance_date",),
        ("permno",),
        ("side",),
        ("filled_shares",),
        ("fill_price",),
    ],
}

INPUT_PARSE_RULES = {
    "compustat_quarterly": {
        "dates": [("datadate",)],
        "optional_dates": [("rdq",)],
        "numbers": [("atq",), ("ltq",), ("ceqq",), ("saleq",), ("ibq",), ("oancfy",)],
    },
    "crsp_daily": {
        "dates": [("date",)],
        "numbers": [("ret",), ("dlret",), ("prc",), ("shrout",), ("vol",)],
    },
    "ccm_link": {
        "dates": [("linkdt",)],
        "optional_dates": [("linkenddt",)],
    },
    "ibes_link": {
        "dates": [("linkdt",)],
        "optional_dates": [("linkenddt",)],
    },
    "ibes_summary": {
        "dates": [("statpers",), ("fpedats",)],
        "numbers": [("meanest",), ("stdev",), ("numest",)],
    },
    "ibes_surprise": {
        "dates": [("statpers",), ("fpedats",)],
        "numbers": [("actual",), ("surprise",), ("surpct",)],
    },
    "kpss_patent": {
        "dates": [("issue_date",)],
        "numbers": [("patent_count",), ("citation_count",)],
    },
    "ff_factors": {
        "dates": [("date",)],
        "numbers": [("mktrf",), ("smb",), ("hml",), ("umd",), ("rf",)],
    },
    "fred_dgs10": {
        "dates": [("date",)],
        "numbers": [("value",)],
    },
    "cboe_vix": {
        "dates": [("DATE", "date")],
        "numbers": [("OPEN", "open", "Open"), ("HIGH", "high", "High"), ("LOW", "low", "Low"), ("CLOSE", "close", "Close")],
    },
    "broker_fills": {
        "dates": [("rebalance_date",)],
        "numbers": [
            ("filled_shares",),
            ("fill_price",),
            ("filled_notional",),
            ("commission",),
            ("broker_commission",),
            ("exchange_fee",),
            ("regulatory_fee",),
            ("tax",),
            ("other_fee",),
        ],
    },
}

ALLOWED_STRATEGY_VALUES = {
    "benchmark_mode": {"ff_total_return", "equal_weight_universe", "zero"},
    "beta_method": {"ols", "ewma"},
    "feature_zscore_method": {"standard", "robust"},
    "risk_zscore_method": {"standard", "robust"},
    "portfolio_construction": {"heuristic", "optimizer"},
    "weighting_scheme": {"equal", "score", "inverse_vol", "score_inverse_vol"},
}


class ConfigDoctor:
    def __init__(self, config: Config, profile_row_limit: int = PROFILE_ROW_LIMIT) -> None:
        self.config = config
        self.profile_row_limit = max(int(profile_row_limit), 1)

    def run(self) -> tuple[list[Path], dict[str, object]]:
        checks = self._path_checks()
        checks.extend(self._schema_checks())
        checks.extend(self._sample_checks())
        checks.extend(self._profile_checks())
        checks.extend(self._output_checks())
        checks.extend(self._strategy_checks())
        summary = self._summary(checks)

        output_dir = self.config.resolve_path(self.config.paths.get("output_dir", "output"))
        ensure_directory(output_dir)
        json_path = output_dir / "doctor_report.json"
        csv_path = output_dir / "doctor_report.csv"
        html_path = output_dir / "doctor_report.html"
        json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        write_csv_dicts(csv_path, self._check_rows(checks))
        html_path.write_text(render_doctor_html_report(summary), encoding="utf-8")
        return [json_path, csv_path, html_path], summary

    def _path_checks(self) -> list[dict[str, object]]:
        checks: list[dict[str, object]] = []
        for key in REQUIRED_INPUT_PATHS:
            checks.append(self._input_path_check(key, required=True))
        for key in OPTIONAL_INPUT_PATHS:
            if key in self.config.paths:
                checks.append(self._input_path_check(key, required=False))
        return checks

    def _schema_checks(self) -> list[dict[str, object]]:
        checks: list[dict[str, object]] = []
        for key in REQUIRED_INPUT_PATHS:
            checks.extend(self._input_schema_checks(key, required=True))
        for key in OPTIONAL_INPUT_PATHS:
            if key in self.config.paths:
                checks.extend(self._input_schema_checks(key, required=False))
        return checks

    def _sample_checks(self) -> list[dict[str, object]]:
        checks: list[dict[str, object]] = []
        for key in REQUIRED_INPUT_PATHS:
            checks.extend(self._input_sample_checks(key, required=True))
        for key in OPTIONAL_INPUT_PATHS:
            if key in self.config.paths:
                checks.extend(self._input_sample_checks(key, required=False))
        return checks

    def _profile_checks(self) -> list[dict[str, object]]:
        checks: list[dict[str, object]] = []
        for key in REQUIRED_INPUT_PATHS:
            checks.extend(self._input_profile_checks(key, required=True))
        for key in OPTIONAL_INPUT_PATHS:
            if key in self.config.paths:
                checks.extend(self._input_profile_checks(key, required=False))
        return checks

    def _input_path_check(self, key: str, required: bool) -> dict[str, object]:
        configured = self.config.paths.get(key)
        severity = "error" if required else "warning"
        if not configured:
            return {
                "name": key,
                "category": "input_path",
                "status": "fail" if required else "warn",
                "severity": severity,
                "message": "Path is not configured.",
                "path": None,
            }
        path = self.config.resolve_path(configured)
        if not path.exists():
            return {
                "name": key,
                "category": "input_path",
                "status": "fail" if required else "warn",
                "severity": severity,
                "message": "Path does not exist.",
                "path": str(path),
            }
        if not path.is_file():
            return {
                "name": key,
                "category": "input_path",
                "status": "fail" if required else "warn",
                "severity": severity,
                "message": "Path exists but is not a file.",
                "path": str(path),
            }
        stat = path.stat()
        return {
            "name": key,
            "category": "input_path",
            "status": "pass",
            "severity": "info",
            "message": f"Found file ({stat.st_size} bytes).",
            "path": str(path),
            "size": stat.st_size,
        }

    def _input_schema_checks(self, key: str, required: bool) -> list[dict[str, object]]:
        configured = self.config.paths.get(key)
        severity = "error" if required else "warning"
        if not configured:
            return []
        path = self.config.resolve_path(configured)
        if not path.is_file():
            return []
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                headers = reader.fieldnames or []
        except OSError as exc:
            return [
                {
                    "name": key,
                    "category": "input_schema",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": f"Could not read CSV header: {exc}",
                    "path": str(path),
                }
            ]

        if not headers:
            return [
                {
                    "name": key,
                    "category": "input_schema",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": "CSV header is missing.",
                    "path": str(path),
                }
            ]

        missing_groups = [
            group
            for group in INPUT_SCHEMAS.get(key, [])
            if not any(candidate in headers for candidate in group)
        ]
        if missing_groups:
            missing = ", ".join("/".join(group) for group in missing_groups)
            return [
                {
                    "name": key,
                    "category": "input_schema",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": f"Missing required column(s): {missing}.",
                    "path": str(path),
                }
            ]

        return [
            {
                "name": key,
                "category": "input_schema",
                "status": "pass",
                "severity": "info",
                "message": f"CSV header includes required columns ({len(headers)} columns).",
                "path": str(path),
            }
        ]

    def _input_sample_checks(self, key: str, required: bool) -> list[dict[str, object]]:
        configured = self.config.paths.get(key)
        severity = "error" if required else "warning"
        if not configured:
            return []
        path = self.config.resolve_path(configured)
        if not path.is_file():
            return []

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                rows = []
                for _ in range(SAMPLE_ROW_LIMIT):
                    try:
                        rows.append(next(reader))
                    except StopIteration:
                        break
        except (csv.Error, OSError) as exc:
            return [
                {
                    "name": key,
                    "category": "input_sample",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": f"Could not read CSV sample: {exc}",
                    "path": str(path),
                }
            ]

        if not rows:
            return [
                {
                    "name": key,
                    "category": "input_sample",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": "CSV has no data rows.",
                    "path": str(path),
                }
            ]

        problems = self._sample_parse_problems(key, rows)
        if problems:
            return [
                {
                    "name": key,
                    "category": "input_sample",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": "Sample row parse problem(s): " + "; ".join(problems[:5]),
                    "path": str(path),
                    "sample_rows": len(rows),
                }
            ]
        return [
            {
                "name": key,
                "category": "input_sample",
                "status": "pass",
                "severity": "info",
                "message": f"First {len(rows)} row(s) are parseable.",
                "path": str(path),
                "sample_rows": len(rows),
            }
        ]

    def _sample_parse_problems(self, key: str, rows: list[dict[str, str]]) -> list[str]:
        rules = INPUT_PARSE_RULES.get(key, {})
        problems = []
        for row_index, row in enumerate(rows, start=2):
            for field_group in rules.get("dates", []):
                field_name, value = self._sample_value(row, field_group)
                if field_name is None:
                    continue
                if not str(value or "").strip():
                    problems.append(f"line {row_index} {field_name} is blank")
                    continue
                try:
                    parse_date(str(value)[:10])
                except ValueError:
                    problems.append(f"line {row_index} {field_name} has unsupported date '{value}'")
            for field_group in rules.get("optional_dates", []):
                field_name, value = self._sample_value(row, field_group)
                if field_name is None or not str(value or "").strip():
                    continue
                try:
                    parse_date(str(value)[:10])
                except ValueError:
                    problems.append(f"line {row_index} {field_name} has unsupported date '{value}'")
            for field_group in rules.get("numbers", []):
                field_name, value = self._sample_value(row, field_group)
                if field_name is None or not str(value or "").strip():
                    continue
                try:
                    float_or_none(str(value))
                except ValueError:
                    problems.append(f"line {row_index} {field_name} has non-numeric value '{value}'")
        return problems

    def _sample_value(self, row: dict[str, str], field_group: tuple[str, ...]) -> tuple[str | None, str | None]:
        for field_name in field_group:
            if field_name in row:
                return field_name, row.get(field_name)
        return None, None

    def _input_profile_checks(self, key: str, required: bool) -> list[dict[str, object]]:
        configured = self.config.paths.get(key)
        severity = "error" if required else "warning"
        if not configured:
            return []
        path = self.config.resolve_path(configured)
        if not path.is_file():
            return []

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                headers = reader.fieldnames or []
                profile = self._profile_csv_rows(key, headers, reader)
        except (csv.Error, OSError) as exc:
            return [
                {
                    "name": key,
                    "category": "input_profile",
                    "status": "fail" if required else "warn",
                    "severity": severity,
                    "message": f"Could not profile CSV rows: {exc}",
                    "path": str(path),
                }
            ]

        if profile["profiled_rows"] <= 0:
            return []

        blank_columns = profile["blank_columns"]
        if blank_columns:
            blank_text = ", ".join(
                f"{column}={rate:.1%}"
                for column, rate in blank_columns
            )
            return [
                {
                    "name": key,
                    "category": "input_profile",
                    "status": "warn",
                    "severity": "warning",
                    "message": (
                        f"Profiled {profile['profiled_rows']} row(s)"
                        f"{' before stopping at the profile limit' if profile['truncated'] else ''}; "
                        f"high required-column blank rate: {blank_text}."
                    ),
                    "path": str(path),
                    "profiled_rows": profile["profiled_rows"],
                    "profile_truncated": profile["truncated"],
                    "max_blank_rate": profile["max_blank_rate"],
                }
            ]

        return [
            {
                "name": key,
                "category": "input_profile",
                "status": "pass",
                "severity": "info",
                "message": (
                    f"Profiled {profile['profiled_rows']} row(s)"
                    f"{' before stopping at the profile limit' if profile['truncated'] else ''}; "
                    f"max required-column blank rate {profile['max_blank_rate']:.1%}."
                ),
                "path": str(path),
                "profiled_rows": profile["profiled_rows"],
                "profile_truncated": profile["truncated"],
                "max_blank_rate": profile["max_blank_rate"],
            }
        ]

    def _profile_csv_rows(
        self,
        key: str,
        headers: list[str],
        reader: csv.DictReader,
    ) -> dict[str, object]:
        required_fields = [
            field_name
            for field_group in INPUT_SCHEMAS.get(key, [])
            for field_name in [self._first_present_field(headers, field_group)]
            if field_name is not None
        ]
        blank_counts = {field_name: 0 for field_name in required_fields}
        profiled_rows = 0
        truncated = False
        for row in reader:
            if profiled_rows >= self.profile_row_limit:
                truncated = True
                break
            profiled_rows += 1
            for field_name in required_fields:
                if not str(row.get(field_name) or "").strip():
                    blank_counts[field_name] += 1

        blank_rates = {
            field_name: (blank_count / profiled_rows) if profiled_rows else 0.0
            for field_name, blank_count in blank_counts.items()
        }
        blank_columns = [
            (field_name, rate)
            for field_name, rate in blank_rates.items()
            if rate >= PROFILE_WARN_BLANK_RATE
        ]
        blank_columns.sort(key=lambda item: (-item[1], item[0]))
        return {
            "profiled_rows": profiled_rows,
            "truncated": truncated,
            "max_blank_rate": max(blank_rates.values(), default=0.0),
            "blank_columns": blank_columns,
        }

    def _first_present_field(self, headers: list[str], field_group: tuple[str, ...]) -> str | None:
        for field_name in field_group:
            if field_name in headers:
                return field_name
        return None

    def _output_checks(self) -> list[dict[str, object]]:
        output_dir = self.config.resolve_path(self.config.paths.get("output_dir", "output"))
        try:
            ensure_directory(output_dir)
            probe_path = output_dir / ".doctor_write_check"
            probe_path.write_text("ok", encoding="utf-8")
            probe_path.unlink()
        except OSError as exc:
            return [
                {
                    "name": "output_dir",
                    "category": "output_path",
                    "status": "fail",
                    "severity": "error",
                    "message": f"Output directory is not writable: {exc}",
                    "path": str(output_dir),
                }
            ]
        return [
            {
                "name": "output_dir",
                "category": "output_path",
                "status": "pass",
                "severity": "info",
                "message": "Output directory is writable.",
                "path": str(output_dir),
            }
        ]

    def _strategy_checks(self) -> list[dict[str, object]]:
        checks = [
            self._date_range_check(),
            self._holding_count_check(),
        ]
        checks.extend(self._numeric_strategy_checks())
        checks.extend(self._allowed_value_checks())
        return checks

    def _date_range_check(self) -> dict[str, object]:
        start_value = self.config.strategy.get("start_date")
        end_value = self.config.strategy.get("end_date")
        try:
            start_date = parse_optional_date(str(start_value)) if start_value is not None else None
            end_date = parse_optional_date(str(end_value)) if end_value is not None else None
        except ValueError as exc:
            return {
                "name": "date_range",
                "category": "strategy",
                "status": "fail",
                "severity": "error",
                "message": str(exc),
                "path": None,
            }
        if start_date and end_date and start_date > end_date:
            return {
                "name": "date_range",
                "category": "strategy",
                "status": "fail",
                "severity": "error",
                "message": "start_date must be less than or equal to end_date.",
                "path": None,
            }
        return {
            "name": "date_range",
            "category": "strategy",
            "status": "pass",
            "severity": "info",
            "message": "Date range is usable.",
            "path": None,
        }

    def _holding_count_check(self) -> dict[str, object]:
        try:
            holding_count = int(self.config.strategy.get("holding_count", 0))
        except (TypeError, ValueError):
            return {
                "name": "holding_count",
                "category": "strategy",
                "status": "fail",
                "severity": "error",
                "message": "holding_count must be an integer greater than zero.",
                "path": None,
            }
        if holding_count <= 0:
            return {
                "name": "holding_count",
                "category": "strategy",
                "status": "fail",
                "severity": "error",
                "message": "holding_count must be greater than zero.",
                "path": None,
            }
        return {
            "name": "holding_count",
            "category": "strategy",
            "status": "pass",
            "severity": "info",
            "message": "holding_count is positive.",
            "path": None,
        }

    def _numeric_strategy_checks(self) -> list[dict[str, object]]:
        numeric_rules = [
            ("max_position_weight", 0.0, 1.0, "inclusive"),
            ("feature_winsor_quantile", 0.0, 0.49, "inclusive"),
            ("risk_winsor_quantile", 0.0, 0.49, "inclusive"),
            ("top_quantile", 0.0, 1.0, "inclusive"),
            ("bottom_quantile", 0.0, 1.0, "inclusive"),
            ("transaction_cost_bps", 0.0, None, "minimum"),
            ("commission_cost_bps", 0.0, None, "minimum"),
            ("slippage_cost_bps", 0.0, None, "minimum"),
            ("min_price", 0.0, None, "minimum"),
            ("min_market_cap", 0.0, None, "minimum"),
            ("min_avg_dollar_volume", 0.0, None, "minimum"),
        ]
        return [
            self._numeric_strategy_check(name, minimum, maximum, bound_type)
            for name, minimum, maximum, bound_type in numeric_rules
            if name in self.config.strategy
        ]

    def _numeric_strategy_check(
        self,
        name: str,
        minimum: float,
        maximum: float | None,
        bound_type: str,
    ) -> dict[str, object]:
        raw_value = self.config.strategy.get(name)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return {
                "name": name,
                "category": "strategy",
                "status": "fail",
                "severity": "error",
                "message": f"{name} must be numeric.",
                "path": None,
            }

        if maximum is None:
            valid = value >= minimum
            expected = f">= {minimum:g}"
        elif bound_type == "inclusive":
            valid = minimum <= value <= maximum
            expected = f"between {minimum:g} and {maximum:g}"
        else:
            valid = minimum < value < maximum
            expected = f"between {minimum:g} and {maximum:g}"

        if not valid:
            return {
                "name": name,
                "category": "strategy",
                "status": "fail",
                "severity": "error",
                "message": f"{name} must be {expected}.",
                "path": None,
            }
        return {
            "name": name,
            "category": "strategy",
            "status": "pass",
            "severity": "info",
            "message": f"{name} is {expected}.",
            "path": None,
        }

    def _allowed_value_checks(self) -> list[dict[str, object]]:
        checks = []
        for name, allowed_values in ALLOWED_STRATEGY_VALUES.items():
            if name not in self.config.strategy:
                continue
            value = str(self.config.strategy.get(name, "")).strip().lower()
            if value in allowed_values:
                checks.append(
                    {
                        "name": name,
                        "category": "strategy",
                        "status": "pass",
                        "severity": "info",
                        "message": f"{name} uses supported value '{value}'.",
                        "path": None,
                    }
                )
                continue
            allowed = ", ".join(sorted(allowed_values))
            checks.append(
                {
                    "name": name,
                    "category": "strategy",
                    "status": "warn",
                    "severity": "warning",
                    "message": f"{name} has unsupported value '{value}' and will fall back internally. Expected one of: {allowed}.",
                    "path": None,
                }
            )
        return checks

    def _summary(self, checks: list[dict[str, object]]) -> dict[str, object]:
        fail_count = sum(1 for check in checks if check["status"] == "fail")
        warn_count = sum(1 for check in checks if check["status"] == "warn")
        return {
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "status": "fail" if fail_count else "warn" if warn_count else "pass",
            "check_count": len(checks),
            "fail_count": fail_count,
            "warn_count": warn_count,
            "pass_count": sum(1 for check in checks if check["status"] == "pass"),
            "config_path": str(self.config.path),
            "profile_row_limit": self.profile_row_limit,
            "checks": checks,
        }

    def _check_rows(self, checks: list[dict[str, object]]) -> list[dict[str, object]]:
        return [
            {
                "status": check.get("status"),
                "severity": check.get("severity"),
                "category": check.get("category"),
                "name": check.get("name"),
                "path": check.get("path"),
                "message": check.get("message"),
                "profiled_rows": check.get("profiled_rows"),
                "profile_truncated": check.get("profile_truncated"),
                "max_blank_rate": check.get("max_blank_rate"),
            }
            for check in checks
        ]
