from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

from .config import Config
from .utils import ensure_directory, write_csv_dicts


class MarketDataFetcher:
    def __init__(self, config: Config) -> None:
        self.config = config

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
            outputs.append(
                self.fetch_fmp_upgrades(
                    self.config.resolve("fmp_grades"),
                    self.config.api.get("fmp_symbols", ["AAPL"]),
                )
            )
        return outputs

    def fetch_fred_series(self, series_id: str, output_path: Path) -> Path:
        params = {
            "series_id": series_id,
            "api_key": self.config.api["fred_api_key"],
            "file_type": "json",
        }
        url = (
            "https://api.stlouisfed.org/fred/series/observations?"
            + urllib.parse.urlencode(params)
        )
        payload = self._get_json(url)
        rows = [
            {"date": item["date"], "value": item["value"]}
            for item in payload.get("observations", [])
            if item.get("value") not in {".", None, ""}
        ]
        write_csv_dicts(output_path, rows)
        return output_path

    def fetch_fmp_upgrades(self, output_path: Path, symbols: list[str]) -> Path:
        rows = []
        for symbol in symbols:
            params = {
                "symbol": symbol,
                "apikey": self.config.api["fmp_api_key"],
            }
            url = (
                "https://financialmodelingprep.com/api/v4/upgrades-downgrades?"
                + urllib.parse.urlencode(params)
            )
            payload = self._get_json(url)
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
        with urllib.request.urlopen(url, timeout=30) as response:
            output_path.write_bytes(response.read())
        return output_path

    def _get_json(self, url: str) -> dict | list:
        with urllib.request.urlopen(url, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
