from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from quant_research.config import Config
from quant_research.data_sources import MarketDataFetcher


class _FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


class MarketDataFetcherTest(unittest.TestCase):
    def test_fetch_fred_series_filters_missing_observations(self) -> None:
        calls: list[tuple[str, float]] = []

        def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
            calls.append((url, timeout))
            return _FakeResponse(
                json.dumps(
                    {
                        "observations": [
                            {"date": "2024-01-01", "value": "."},
                            {"date": "2024-01-02", "value": ""},
                            {"date": "2024-01-03", "value": "4.25"},
                        ]
                    }
                ).encode("utf-8")
            )

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "fred.csv"
            fetcher = MarketDataFetcher(
                Config(path=Path("config.json"), raw={"api": {"fred_api_key": "fred-key"}}),
                urlopen=fake_urlopen,
            )

            fetcher.fetch_fred_series("DGS10", output_path)

            with output_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(rows, [{"date": "2024-01-03", "value": "4.25"}])
        self.assertEqual(len(calls), 1)
        parsed = urlparse(calls[0][0])
        self.assertEqual(calls[0][1], 30.0)
        self.assertEqual(parse_qs(parsed.query)["series_id"], ["DGS10"])
        self.assertEqual(parse_qs(parsed.query)["api_key"], ["fred-key"])

    def test_fetch_fmp_upgrades_normalizes_symbols_and_uses_fallback_symbol(self) -> None:
        requested_symbols: list[str] = []

        def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
            requested_symbols.append(parse_qs(urlparse(url).query)["symbol"][0])
            payload_by_symbol = {
                "AAA": [
                    {
                        "publishedDate": "2024-01-01",
                        "newGrade": "Buy",
                        "previousGrade": "Hold",
                        "gradingCompany": "Broker A",
                        "action": "upgrade",
                    }
                ],
                "BBB": [
                    {
                        "symbol": "BBB",
                        "publishedDate": "2024-01-02",
                        "newGrade": "Neutral",
                        "previousGrade": "Sell",
                        "gradingCompany": "Broker B",
                        "action": "upgrade",
                    }
                ],
            }
            return _FakeResponse(json.dumps(payload_by_symbol[requested_symbols[-1]]).encode("utf-8"))

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "fmp.csv"
            fetcher = MarketDataFetcher(
                Config(path=Path("config.json"), raw={"api": {"fmp_api_key": "fmp-key"}}),
                urlopen=fake_urlopen,
            )

            fetcher.fetch_fmp_upgrades(output_path, [" aaa ", "AAA", "", "bbb"])

            with output_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(requested_symbols, ["AAA", "BBB"])
        self.assertEqual(rows[0]["symbol"], "AAA")
        self.assertEqual(rows[1]["symbol"], "BBB")

    def test_fetch_all_runs_only_configured_sources(self) -> None:
        calls: list[str] = []

        def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
            calls.append(url)
            parsed = urlparse(url)
            if "stlouisfed" in parsed.netloc:
                payload = json.dumps({"observations": [{"date": "2024-01-01", "value": "4.10"}]}).encode("utf-8")
            elif "financialmodelingprep" in parsed.netloc:
                payload = json.dumps([{"symbol": "AAA", "publishedDate": "2024-01-01"}]).encode("utf-8")
            else:
                payload = b"DATE,CLOSE\n2024-01-01,20.1\n"
            return _FakeResponse(payload)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = Config(
                path=root / "config.json",
                raw={
                    "paths": {
                        "fred_dgs10": str(root / "fred.csv"),
                        "cboe_vix": str(root / "vix.csv"),
                        "fmp_grades": str(root / "fmp.csv"),
                    },
                    "api": {
                        "fred_api_key": "fred-key",
                        "fmp_api_key": "fmp-key",
                        "fmp_symbols": ["AAA"],
                        "cboe_vix_csv_url": "https://example.com/vix.csv",
                        "request_timeout_seconds": 12,
                    },
                },
            )

            outputs = MarketDataFetcher(config, urlopen=fake_urlopen).fetch_all()

        self.assertEqual(len(outputs), 3)
        self.assertEqual(len(calls), 3)
        self.assertTrue(any("stlouisfed" in url for url in calls))
        self.assertTrue(any("financialmodelingprep" in url for url in calls))
        self.assertTrue(any("example.com" in url for url in calls))

    def test_fetch_fred_series_rejects_non_object_json_payload(self) -> None:
        def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
            return _FakeResponse(json.dumps(["unexpected"]).encode("utf-8"))

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "fred.csv"
            fetcher = MarketDataFetcher(
                Config(path=Path("config.json"), raw={"api": {"fred_api_key": "fred-key"}}),
                urlopen=fake_urlopen,
            )

            with self.assertRaisesRegex(ValueError, "FRED response must be a JSON object"):
                fetcher.fetch_fred_series("DGS10", output_path)


if __name__ == "__main__":
    unittest.main()
