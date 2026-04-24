from __future__ import annotations

import json
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from .config import Config
from .utils import ensure_directory, write_csv_dicts


class MarketDataFetcher:
    def __init__(
        self,
        config: Config,
        urlopen: Callable[..., Any] | None = None,
    ) -> None:
        self.config = config
        self._urlopen = urlopen or urllib.request.urlopen
        self.timeout = float(self.config.api.get("request_timeout_seconds", 30.0))

    def fetch_all(self) -> list[Path]:
        outputs: list[Path] = []
        fred_key = self.config.api.get("fred_api_key")
        if fred_key:
            outputs.append(self.fetch_fred_series("DGS10", self.config.resolve("fred_dgs10")))

        cboe_url = self.config.api.get("cboe_vix_csv_url")
        if cboe_url:
            outputs.append(self.fetch_binary_file(cboe_url, self.config.resolve("cboe_vix")))

        fmp_key = self.config.api.get("fmp_api_key")
        if fmp_key:
            symbols = self._configured_symbols(self.config.api.get("fmp_symbols", ["AAPL"]))
            if symbols:
                outputs.append(
                    self.fetch_fmp_upgrades(
                        self.config.resolve("fmp_grades"),
                        symbols,
                    )
                )
        return outputs

    def fetch_fred_series(self, series_id: str, output_path: Path) -> Path:
        payload = self._get_json(
            self._build_url(
                "https://api.stlouisfed.org/fred/series/observations",
                {
                    "series_id": series_id,
                    "api_key": self.config.api["fred_api_key"],
                    "file_type": "json",
                },
            )
        )
        if not isinstance(payload, dict):
            raise ValueError("FRED response must be a JSON object")
        rows = [
            {"date": item["date"], "value": item["value"]}
            for item in payload.get("observations", [])
            if item.get("value") not in {".", None, ""}
        ]
        write_csv_dicts(output_path, rows)
        return output_path

    def fetch_fmp_upgrades(self, output_path: Path, symbols: list[str]) -> Path:
        rows = []
        for symbol in self._configured_symbols(symbols):
            payload = self._get_json(
                self._build_url(
                    "https://financialmodelingprep.com/api/v4/upgrades-downgrades",
                    {
                        "symbol": symbol,
                        "apikey": self.config.api["fmp_api_key"],
                    },
                )
            )
            if not isinstance(payload, list):
                raise ValueError("FMP upgrades response must be a JSON array")
            for item in payload:
                rows.append(
                    {
                        "symbol": item.get("symbol", symbol),
                        "publishedDate": item.get("publishedDate", ""),
                        "newGrade": item.get("newGrade", ""),
                        "previousGrade": item.get("previousGrade", ""),
                        "gradingCompany": item.get("gradingCompany", ""),
                        "action": item.get("action", ""),
                    }
                )
        write_csv_dicts(output_path, rows)
        return output_path

    def fetch_binary_file(self, url: str, output_path: Path) -> Path:
        ensure_directory(output_path.parent)
        output_path.write_bytes(self._get_bytes(url))
        return output_path

    def _get_json(self, url: str) -> dict | list:
        return json.loads(self._get_bytes(url).decode("utf-8"))

    def _get_bytes(self, url: str) -> bytes:
        with self._urlopen(url, timeout=self.timeout) as response:
            return response.read()

    def _build_url(self, base_url: str, params: dict[str, object]) -> str:
        return base_url + "?" + urllib.parse.urlencode(params)

    def _configured_symbols(self, raw_symbols: Iterable[object]) -> list[str]:
        symbols: list[str] = []
        seen: set[str] = set()
        for value in raw_symbols:
            symbol = str(value).strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)
        return symbols
