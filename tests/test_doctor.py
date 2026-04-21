from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from quant_research.doctor import ConfigDoctor
from quant_research.config import Config
from quant_research.main import main


class DoctorTest(unittest.TestCase):
    def test_doctor_passes_sample_config(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            config = Config.load(repo_root / "config" / "sample_config.json").with_path_overrides(
                output_dir=Path(tmp) / "doctor"
            )

            outputs, summary = ConfigDoctor(config).run()

            self.assertEqual(summary["status"], "pass")
            self.assertEqual(summary["fail_count"], 0)
            self.assertTrue(outputs[0].exists())
            self.assertTrue(outputs[1].exists())
            self.assertTrue(outputs[2].exists())

    def test_doctor_reports_missing_required_file(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            config_path = temp_root / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "paths": {
                            "output_dir": str(temp_root / "output"),
                            "compustat_quarterly": str(temp_root / "missing.csv"),
                            "crsp_daily": str(repo_root / "data" / "sample_crsp_daily.csv"),
                            "ccm_link": str(repo_root / "data" / "sample_ccm_link.csv"),
                            "ibes_link": str(repo_root / "data" / "sample_ibes_link.csv"),
                            "ibes_summary": str(repo_root / "data" / "sample_ibes_summary.csv"),
                            "ibes_surprise": str(repo_root / "data" / "sample_ibes_surprise.csv"),
                            "kpss_patent": str(repo_root / "data" / "sample_kpss_patent.csv"),
                            "ff_factors": str(repo_root / "data" / "sample_ff_factors.csv"),
                            "fred_dgs10": str(repo_root / "data" / "sample_fred_dgs10.csv"),
                            "cboe_vix": str(repo_root / "data" / "sample_cboe_vix.csv"),
                            "fmp_grades": str(repo_root / "data" / "sample_fmp_grades.csv"),
                        },
                        "strategy": {"holding_count": 1, "start_date": "2024-01-01", "end_date": "2024-12-31"},
                    }
                ),
                encoding="utf-8",
            )

            outputs, summary = ConfigDoctor(Config.load(config_path)).run()

            self.assertEqual(summary["status"], "fail")
            self.assertEqual(summary["fail_count"], 1)
            self.assertTrue(any(check["name"] == "compustat_quarterly" for check in summary["checks"]))
            self.assertTrue(outputs[0].exists())

    def test_doctor_reports_missing_required_csv_column(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            bad_crsp_path = temp_root / "bad_crsp.csv"
            bad_crsp_path.write_text(
                "permno,date,prc,shrout\n10001,2025-01-31,100,2000\n",
                encoding="utf-8",
            )
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(temp_root / "doctor-output")
            raw["paths"]["crsp_daily"] = str(bad_crsp_path)

            _outputs, summary = ConfigDoctor(Config(path=sample.path, raw=raw)).run()

        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check["category"] == "input_schema"
                and check["name"] == "crsp_daily"
                and check["status"] == "fail"
                and "ret" in str(check["message"])
                for check in summary["checks"]
            )
        )

    def test_doctor_reports_unparseable_sample_row(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            bad_factors_path = temp_root / "bad_factors.csv"
            bad_factors_path.write_text(
                "date,mktrf,smb,hml,umd,rf\nnot-a-date,abc,0.0,0.0,0.0,0.01\n",
                encoding="utf-8",
            )
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(temp_root / "doctor-output")
            raw["paths"]["ff_factors"] = str(bad_factors_path)

            _outputs, summary = ConfigDoctor(Config(path=sample.path, raw=raw)).run()

        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check["category"] == "input_sample"
                and check["name"] == "ff_factors"
                and check["status"] == "fail"
                and "not-a-date" in str(check["message"])
                and "abc" in str(check["message"])
                for check in summary["checks"]
            )
        )

    def test_doctor_reports_required_header_only_csv(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            empty_fred_path = temp_root / "empty_fred.csv"
            empty_fred_path.write_text("date,value\n", encoding="utf-8")
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(temp_root / "doctor-output")
            raw["paths"]["fred_dgs10"] = str(empty_fred_path)

            _outputs, summary = ConfigDoctor(Config(path=sample.path, raw=raw)).run()

        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check["category"] == "input_sample"
                and check["name"] == "fred_dgs10"
                and check["status"] == "fail"
                and "no data rows" in str(check["message"])
                for check in summary["checks"]
            )
        )

    def test_doctor_warns_on_high_required_column_blank_rate(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            sparse_compustat_path = temp_root / "sparse_compustat.csv"
            sparse_compustat_path.write_text(
                "\n".join(
                    [
                        "gvkey,datadate,rdq,sic,atq,ltq,ceqq,saleq,ibq,oancfy",
                        "001001,2025-01-15,2025-02-10,3571,500,200,300,250,,25",
                        "001001,2025-04-15,2025-05-10,3571,520,210,310,255,,26",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(temp_root / "doctor-output")
            raw["paths"]["compustat_quarterly"] = str(sparse_compustat_path)

            outputs, summary = ConfigDoctor(Config(path=sample.path, raw=raw)).run()

            csv_report = outputs[1].read_text(encoding="utf-8")

        self.assertEqual(summary["status"], "warn")
        self.assertIn("max_blank_rate", csv_report)
        self.assertTrue(
            any(
                check["category"] == "input_profile"
                and check["name"] == "compustat_quarterly"
                and check["status"] == "warn"
                and "ibq=100.0%" in str(check["message"])
                for check in summary["checks"]
            )
        )

    def test_doctor_profile_row_limit_truncates_profile(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            config = Config.load(repo_root / "config" / "sample_config.json").with_path_overrides(
                output_dir=Path(tmp) / "doctor-output"
            )

            _outputs, summary = ConfigDoctor(config, profile_row_limit=1).run()

        self.assertEqual(summary["profile_row_limit"], 1)
        self.assertTrue(
            any(
                check["category"] == "input_profile"
                and check["name"] == "crsp_daily"
                and check["profiled_rows"] == 1
                and check["profile_truncated"]
                for check in summary["checks"]
            )
        )

    def test_doctor_reports_invalid_strategy_without_throwing(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(Path(tmp) / "doctor-output")
            raw["strategy"]["holding_count"] = "many"
            config = Config(path=sample.path, raw=raw)

            _outputs, summary = ConfigDoctor(config).run()

        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check["name"] == "holding_count" and check["status"] == "fail"
                for check in summary["checks"]
            )
        )

    def test_doctor_reports_out_of_range_strategy_value(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(Path(tmp) / "doctor-output")
            raw["strategy"]["feature_winsor_quantile"] = 0.8

            _outputs, summary = ConfigDoctor(Config(path=sample.path, raw=raw)).run()

        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check["name"] == "feature_winsor_quantile" and check["status"] == "fail"
                for check in summary["checks"]
            )
        )

    def test_doctor_warns_on_unsupported_strategy_value(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(Path(tmp) / "doctor-output")
            raw["strategy"]["benchmark_mode"] = "mystery"

            _outputs, summary = ConfigDoctor(Config(path=sample.path, raw=raw)).run()

        self.assertEqual(summary["status"], "warn")
        self.assertTrue(
            any(
                check["name"] == "benchmark_mode" and check["status"] == "warn"
                for check in summary["checks"]
            )
        )

    def test_cli_doctor_writes_report_and_manifest(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "doctor-output"
            stdout = io.StringIO()
            argv = [
                "quant-research",
                "doctor",
                "--config",
                str(repo_root / "config" / "sample_config.json"),
                "--output-dir",
                str(output_dir),
            ]

            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    main()

            self.assertTrue((output_dir / "doctor_report.json").exists())
            self.assertTrue((output_dir / "doctor_report.csv").exists())
            self.assertTrue((output_dir / "doctor_report.html").exists())
            self.assertTrue((output_dir / "run_manifest.json").exists())
            self.assertIn("pass input_path.compustat_quarterly", stdout.getvalue())

    def test_doctor_html_report_summarizes_checks(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            config = Config.load(repo_root / "config" / "sample_config.json").with_path_overrides(
                output_dir=Path(tmp) / "doctor-output"
            )

            outputs, _summary = ConfigDoctor(config).run()

            html_report = outputs[2].read_text(encoding="utf-8")

        self.assertIn("<title>Doctor Report</title>", html_report)
        self.assertIn('data-filter="fail"', html_report)
        self.assertIn('data-status="pass"', html_report)
        self.assertIn("input_sample", html_report)
        self.assertIn("input_profile", html_report)
        self.assertIn("compustat_quarterly", html_report)

    def test_cli_doctor_json_outputs_parseable_payload(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "doctor-output"
            stdout = io.StringIO()
            argv = [
                "quant-research",
                "doctor",
                "--config",
                str(repo_root / "config" / "sample_config.json"),
                "--output-dir",
                str(output_dir),
                "--json",
                "--profile-row-limit",
                "1",
            ]

            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    main()

            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "pass")
            self.assertEqual(payload["exit_code"], 0)
            self.assertEqual(payload["profile_row_limit"], 1)
            self.assertEqual(payload["outputs"][0], str(output_dir / "doctor_report.json"))
            self.assertIn(str(output_dir / "doctor_report.html"), payload["outputs"])
            self.assertEqual(payload["manifest_path"], str(output_dir / "run_manifest.json"))

    def test_cli_doctor_strict_treats_warnings_as_failure(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            temp_root = Path(tmp)
            sample = Config.load(repo_root / "config" / "sample_config.json")
            raw = json.loads(json.dumps(sample.raw))
            raw["paths"]["output_dir"] = str(temp_root / "doctor-output")
            raw["paths"]["broker_fills"] = str(temp_root / "missing_optional.csv")
            config_path = temp_root / "config.json"
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            stdout = io.StringIO()
            argv = [
                "quant-research",
                "doctor",
                "--config",
                str(config_path),
                "--strict",
                "--json",
            ]

            with patch.object(sys, "argv", argv):
                with redirect_stdout(stdout):
                    with self.assertRaises(SystemExit) as raised:
                        main()

            payload = json.loads(stdout.getvalue())
            self.assertEqual(raised.exception.code, 1)
            self.assertEqual(payload["status"], "warn")
            self.assertEqual(payload["exit_code"], 1)
            self.assertTrue(payload["strict"])


if __name__ == "__main__":
    unittest.main()
