from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.main import build_parser
from quant_research.validation import DataValidator


class ValidationTest(unittest.TestCase):
    def test_validate_command_is_registered(self) -> None:
        parser = build_parser()

        args = parser.parse_args(["validate", "--config", "config/sample_config.json"])

        self.assertEqual(args.command, "validate")

    def test_validator_writes_expected_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_dir = root / "output"
            config = Config(
                path=root / "config.json",
                raw={
                    "paths": {
                        "output_dir": str(output_dir),
                        "compustat_quarterly": str(root / "compustat_quarterly.csv"),
                        "crsp_daily": str(root / "crsp_daily.csv"),
                        "ccm_link": str(root / "ccm_link.csv"),
                        "ibes_link": str(root / "ibes_link.csv"),
                        "ibes_summary": str(root / "ibes_summary.csv"),
                        "ibes_surprise": str(root / "ibes_surprise.csv"),
                        "kpss_patent": str(root / "kpss_patent.csv"),
                        "ff_factors": str(root / "ff_factors.csv"),
                        "fred_dgs10": str(root / "fred_dgs10.csv"),
                        "cboe_vix": str(root / "cboe_vix.csv"),
                        "fmp_grades": str(root / "fmp_grades.csv"),
                    },
                    "strategy": {
                        "use_rdq": True,
                        "report_lag_days": 45,
                        "start_date": "2024-01-01",
                        "end_date": "2024-12-31",
                        "min_price": 5.0,
                        "min_market_cap": 100000000.0,
                        "beta_lookback_days": 10,
                        "min_beta_observations": 2,
                        "vix_de_risk_level": 30.0,
                    },
                },
            )

            self._write_csv(
                root / "compustat_quarterly.csv",
                [
                    {
                        "gvkey": "001",
                        "datadate": "2024-03-31",
                        "rdq": "2024-05-15",
                        "sic": "3571",
                        "gsector": "45",
                        "atq": "200",
                        "ltq": "50",
                        "ceqq": "150",
                        "saleq": "100",
                        "ibq": "10",
                        "oancfy": "20",
                    },
                    {
                        "gvkey": "002",
                        "datadate": "2024-03-31",
                        "rdq": "2024-05-15",
                        "sic": "2834",
                        "gsector": "35",
                        "atq": "200",
                        "ltq": "50",
                        "ceqq": "150",
                        "saleq": "100",
                        "ibq": "10",
                        "oancfy": "20",
                    },
                    {
                        "gvkey": "003",
                        "datadate": "2024-03-31",
                        "rdq": "2024-05-15",
                        "sic": "7370",
                        "gsector": "45",
                        "atq": "200",
                        "ltq": "50",
                        "ceqq": "150",
                        "saleq": "100",
                        "ibq": "10",
                        "oancfy": "20",
                    },
                    {
                        "gvkey": "004",
                        "datadate": "2024-03-31",
                        "rdq": "2024-05-15",
                        "sic": "1311",
                        "gsector": "10",
                        "atq": "200",
                        "ltq": "50",
                        "ceqq": "150",
                        "saleq": "100",
                        "ibq": "10",
                        "oancfy": "20",
                    },
                    {
                        "gvkey": "005",
                        "datadate": "2024-03-31",
                        "rdq": "2024-05-15",
                        "sic": "6021",
                        "gsector": "40",
                        "atq": "200",
                        "ltq": "50",
                        "ceqq": "150",
                        "saleq": "100",
                        "ibq": "10",
                        "oancfy": "20",
                    },
                ],
            )
            self._write_csv(
                root / "ccm_link.csv",
                [
                    {"gvkey": "001", "permno": "10001", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                    {"gvkey": "002", "permno": "10002", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                    {"gvkey": "004", "permno": "10004", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                    {"gvkey": "005", "permno": "10005", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                ],
            )
            self._write_csv(
                root / "crsp_daily.csv",
                [
                    {"permno": "10001", "date": "2024-05-28", "ret": "0.01", "dlret": "0.00", "prc": "10", "shrout": "20000", "vol": "1000"},
                    {"permno": "10001", "date": "2024-05-29", "ret": "0.02", "dlret": "0.00", "prc": "10", "shrout": "20000", "vol": "1000"},
                    {"permno": "10001", "date": "2024-05-30", "ret": "0.03", "dlret": "0.00", "prc": "10", "shrout": "20000", "vol": "1000"},
                    {"permno": "10002", "date": "2024-05-30", "ret": "0.01", "dlret": "0.00", "prc": "4", "shrout": "30000", "vol": "1000"},
                    {"permno": "10005", "date": "2024-05-30", "ret": "0.01", "dlret": "0.00", "prc": "10", "shrout": "100", "vol": "1000"},
                ],
            )
            self._write_csv(
                root / "ibes_link.csv",
                [
                    {"ticker": "AAA", "permno": "10001", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                    {"ticker": "BBB", "permno": "10002", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                    {"ticker": "CCC", "permno": "10005", "linkdt": "2024-01-01", "linkenddt": "2024-12-31"},
                ],
            )
            self._write_csv(
                root / "ibes_summary.csv",
                [
                    {"ticker": "AAA", "statpers": "2024-04-30", "measure": "EPS", "meanest": "1.0", "stdev": "0.1", "numest": "5"},
                    {"ticker": "AAA", "statpers": "2024-05-20", "measure": "EPS", "meanest": "1.1", "stdev": "0.2", "numest": "5"},
                ],
            )
            self._write_csv(
                root / "ibes_surprise.csv",
                [
                    {"ticker": "AAA", "statpers": "2024-05-20", "actual": "1.2", "surprise": "0.1", "surpct": "5.0"},
                ],
            )
            self._write_csv(
                root / "kpss_patent.csv",
                [
                    {"gvkey": "001", "issue_date": "2024-02-01", "patent_count": "10", "citation_count": "20"},
                ],
            )
            self._write_csv(
                root / "ff_factors.csv",
                [
                    {"date": "2024-05-28", "mktrf": "1.0", "rf": "0.0"},
                    {"date": "2024-05-29", "mktrf": "2.0", "rf": "0.0"},
                    {"date": "2024-05-30", "mktrf": "3.0", "rf": "0.0"},
                ],
            )
            self._write_csv(
                root / "fred_dgs10.csv",
                [
                    {"date": "2024-03-01", "value": "4.0"},
                    {"date": "2024-05-30", "value": "3.0"},
                ],
            )
            self._write_csv(
                root / "cboe_vix.csv",
                [
                    {"DATE": "2024-05-30", "CLOSE": "20.0"},
                ],
            )
            self._write_csv(
                root / "fmp_grades.csv",
                [
                    {"symbol": "AAA", "publishedDate": "2024-05-10", "newGrade": "buy", "previousGrade": "hold", "action": "upgrade"},
                ],
            )

            outputs = DataValidator(config).run()

            self.assertEqual(len(outputs), 3)
            for output in outputs:
                self.assertTrue(output.exists())

            summary = json.loads((output_dir / "validation_summary.json").read_text(encoding="utf-8"))
            with (output_dir / "validation_summary.csv").open("r", encoding="utf-8", newline="") as handle:
                summary_rows = list(csv.DictReader(handle))
            with (output_dir / "validation_rebalances.csv").open("r", encoding="utf-8", newline="") as handle:
                rebalance_rows = list(csv.DictReader(handle))

        self.assertEqual(summary["reports_in_window"], 5)
        self.assertEqual(summary["linked_reports"], 4)
        self.assertAlmostEqual(summary["link_match_rate"], 0.8, places=8)
        self.assertEqual(summary["missing_link_count"], 1)
        self.assertEqual(summary["priced_reports"], 3)
        self.assertAlmostEqual(summary["price_snapshot_rate"], 0.75, places=8)
        self.assertEqual(summary["missing_price_history_count"], 1)
        self.assertEqual(summary["final_universe_count"], 1)
        self.assertEqual(summary["price_filter_drop_count"], 1)
        self.assertEqual(summary["market_cap_filter_drop_count"], 1)
        self.assertEqual(summary["beta_estimated_count"], 1)
        self.assertAlmostEqual(summary["beta_coverage_rate"], 1.0, places=8)
        self.assertAlmostEqual(summary["factor_missing_rates"]["surprise"], 2.0 / 3.0, places=8)
        self.assertEqual(len(summary_rows), 1)
        self.assertIn("surprise_missing_rate", summary_rows[0])
        self.assertEqual(len(rebalance_rows), 1)
        self.assertEqual(rebalance_rows[0]["rebalance_date"], "2024-05-31")
        self.assertEqual(rebalance_rows[0]["report_count"], "5")
        self.assertEqual(rebalance_rows[0]["final_universe_count"], "1")
        self.assertEqual(rebalance_rows[0]["beta_estimated_count"], "1")

    def _write_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)


if __name__ == "__main__":
    unittest.main()
