from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Config
from .utils import ensure_directory


DEFAULT_WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
DEFAULT_WRDS_PORT = 9737
DEFAULT_WRDS_DATABASE = "wrds"
PLACEHOLDER_PATTERN = re.compile(r"<([A-Z0-9_]+)>")


@dataclass(slots=True)
class WRDSExportSpec:
    name: str
    sql_path: Path
    output_path: Path


class WRDSConfigError(RuntimeError):
    pass


class WRDSExportRunner:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.wrds_config = config.raw.get("wrds", {})

    def export(self, step: str | None = None, dry_run: bool = False) -> list[Path]:
        specs = self._load_specs()
        if step:
            specs = [spec for spec in specs if spec.name == step]
            if not specs:
                raise WRDSConfigError(f"Unknown WRDS export step: {step}")
        placeholders = self._placeholders()
        if dry_run:
            for spec in specs:
                _ = self._render_sql(spec.sql_path.read_text(encoding="utf-8"), placeholders)
            return [spec.output_path for spec in specs]
        connection = self._connect()
        try:
            outputs = []
            for spec in specs:
                sql = self._render_sql(spec.sql_path.read_text(encoding="utf-8"), placeholders)
                self._query_to_csv(connection, sql, spec.output_path)
                outputs.append(spec.output_path)
            return outputs
        finally:
            connection.close()

    def _load_specs(self) -> list[WRDSExportSpec]:
        exports = self.wrds_config.get("exports")
        if exports:
            return [
                WRDSExportSpec(
                    name=item["name"],
                    sql_path=self.config.resolve_path(item["sql"]),
                    output_path=self.config.resolve_path(item["output"]),
                )
                for item in exports
            ]
        return [
            WRDSExportSpec("compustat_quarterly", self.config.resolve_path("sql/wrds/01_compustat_quarterly.sql"), self.config.resolve("compustat_quarterly")),
            WRDSExportSpec("ccm_link", self.config.resolve_path("sql/wrds/02_ccm_link.sql"), self.config.resolve("ccm_link")),
            WRDSExportSpec("crsp_daily", self.config.resolve_path("sql/wrds/03_crsp_daily.sql"), self.config.resolve("crsp_daily")),
            WRDSExportSpec("ibes_link", self.config.resolve_path("sql/wrds/04_ibes_link_template.sql"), self.config.resolve("ibes_link")),
            WRDSExportSpec("ibes_summary", self.config.resolve_path("sql/wrds/05_ibes_summary_template.sql"), self.config.resolve("ibes_summary")),
            WRDSExportSpec("ibes_surprise", self.config.resolve_path("sql/wrds/06_ibes_surprise_template.sql"), self.config.resolve("ibes_surprise")),
            WRDSExportSpec("kpss_patent", self.config.resolve_path("sql/wrds/07_kpss_patent_template.sql"), self.config.resolve("kpss_patent")),
            WRDSExportSpec("ff_factors", self.config.resolve_path("sql/wrds/08_ff_factors_template.sql"), self.config.resolve("ff_factors")),
        ]

    def _placeholders(self) -> dict[str, str]:
        return {str(key).upper(): str(value) for key, value in self.wrds_config.get("placeholders", {}).items()}

    def _render_sql(self, sql_text: str, placeholders: dict[str, str]) -> str:
        def replace(match: re.Match[str]) -> str:
            key = match.group(1)
            if key not in placeholders:
                raise WRDSConfigError(
                    f"Missing WRDS placeholder for <{key}>. Set wrds.placeholders.{key} in your config."
                )
            return placeholders[key]

        rendered = PLACEHOLDER_PATTERN.sub(replace, sql_text)
        unresolved = PLACEHOLDER_PATTERN.findall(rendered)
        if unresolved:
            raise WRDSConfigError(f"Unresolved WRDS placeholders remain: {', '.join(unresolved)}")
        return rendered

    def _connect(self) -> Any:
        username_env = self.wrds_config.get("username_env", "WRDS_USERNAME")
        password_env = self.wrds_config.get("password_env", "WRDS_PASSWORD")
        user = os.getenv(username_env, "")
        password = os.getenv(password_env, "")
        if not user or not password:
            raise WRDSConfigError(
                f"Missing WRDS credentials. Set environment variables {username_env} and {password_env}."
            )

        connection_kwargs = {
            "host": self.wrds_config.get("host", DEFAULT_WRDS_HOST),
            "port": int(self.wrds_config.get("port", DEFAULT_WRDS_PORT)),
            "dbname": self.wrds_config.get("database", DEFAULT_WRDS_DATABASE),
            "user": user,
            "password": password,
            "sslmode": self.wrds_config.get("sslmode", "require"),
        }
        psycopg = self._load_psycopg()
        if psycopg["driver"] == "psycopg":
            return psycopg["module"].connect(**connection_kwargs)
        connection_kwargs["database"] = connection_kwargs.pop("dbname")
        return psycopg["module"].connect(**connection_kwargs)

    def _load_psycopg(self) -> dict[str, Any]:
        try:
            import psycopg  # type: ignore

            return {"driver": "psycopg", "module": psycopg}
        except ImportError:
            pass
        try:
            import psycopg2  # type: ignore

            return {"driver": "psycopg2", "module": psycopg2}
        except ImportError as exc:
            raise WRDSConfigError(
                "No PostgreSQL driver found. Install `psycopg[binary]` or `psycopg2-binary` to use wrds-export."
            ) from exc

    def _query_to_csv(self, connection: Any, sql: str, output_path: Path) -> None:
        ensure_directory(output_path.parent)
        chunk_size = int(self.wrds_config.get("fetch_size", 5000))
        with connection.cursor() as cursor:
            cursor.execute(sql)
            headers = [column[0] for column in cursor.description]
            with output_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    writer.writerows(rows)
