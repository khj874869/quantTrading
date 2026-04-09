from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

from quant_research.cache import PreparedDataCache
from quant_research.config import Config


class CacheTest(unittest.TestCase):
    def test_cache_hit_and_invalidation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for dirname in ("data", "output"):
                (root / dirname).mkdir()

            self._write_dataset(root)
            config_path = root / "config.json"
            config_path.write_text(
                """
                {
                  "paths": {
                    "output_dir": "output",
                    "compustat_quarterly": "data/sample_compustat_quarterly.csv",
                    "crsp_daily": "data/sample_crsp_daily.csv",
                    "ccm_link": "data/sample_ccm_link.csv",
                    "ibes_link": "data/sample_ibes_link.csv",
                    "ibes_summary": "data/sample_ibes_summary.csv",
                    "ibes_surprise": "data/sample_ibes_surprise.csv",
                    "kpss_patent": "data/sample_kpss_patent.csv",
                    "ff_factors": "data/sample_ff_factors.csv",
                    "fred_dgs10": "data/sample_fred_dgs10.csv",
                    "cboe_vix": "data/sample_cboe_vix.csv",
                    "fmp_grades": "data/sample_fmp_grades.csv"
                  },
                  "cache": {
                    "enabled": true,
                    "cache_dir": "output/cache"
                  },
                  "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "use_rdq": true,
                    "report_lag_days": 45,
                    "beta_lookback_days": 126,
                    "min_beta_observations": 60
                  }
                }
                """,
                encoding="utf-8",
            )

            config = Config.load(config_path)
            first = PreparedDataCache(config).load_or_build()
            second = PreparedDataCache(config).load_or_build()
            self.assertFalse(first.cache_hit)
            self.assertTrue(second.cache_hit)
            self.assertFalse(first.source_cache_hit)
            self.assertFalse(first.feature_cache_hit)
            self.assertFalse(first.prepared_cache_hit)
            self.assertTrue(second.source_cache_hit)
            self.assertTrue(second.feature_cache_hit)
            self.assertTrue(second.prepared_cache_hit)
            self.assertIn("load_sources_total_seconds", first.profile)
            self.assertIn("build_feature_panel_total_seconds", first.profile)
            self.assertIn("finalize_prepared_total_seconds", first.profile)

            sample_file = root / "data" / "sample_fmp_grades.csv"
            sample_file.write_text(
                sample_file.read_text(encoding="utf-8") + "AAA,2025-02-01,Buy,Hold,Broker X,Upgrade\n",
                encoding="utf-8",
            )
            third = PreparedDataCache(config).load_or_build()
            self.assertFalse(third.cache_hit)

    def test_strategy_only_change_keeps_cache_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for dirname in ("data", "output"):
                (root / dirname).mkdir()
            self._write_dataset(root)

            base_config = {
                "paths": {
                    "output_dir": "output",
                    "compustat_quarterly": "data/sample_compustat_quarterly.csv",
                    "crsp_daily": "data/sample_crsp_daily.csv",
                    "ccm_link": "data/sample_ccm_link.csv",
                    "ibes_link": "data/sample_ibes_link.csv",
                    "ibes_summary": "data/sample_ibes_summary.csv",
                    "ibes_surprise": "data/sample_ibes_surprise.csv",
                    "kpss_patent": "data/sample_kpss_patent.csv",
                    "ff_factors": "data/sample_ff_factors.csv",
                    "fred_dgs10": "data/sample_fred_dgs10.csv",
                    "cboe_vix": "data/sample_cboe_vix.csv",
                    "fmp_grades": "data/sample_fmp_grades.csv"
                },
                "cache": {"enabled": True, "cache_dir": "output/cache"},
                "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "use_rdq": True,
                    "report_lag_days": 45,
                    "beta_lookback_days": 126,
                    "min_beta_observations": 60,
                    "transaction_cost_bps": 10.0
                }
            }
            config_a_path = root / "config_a.json"
            config_a_path.write_text(json.dumps(base_config), encoding="utf-8")
            config_b = dict(base_config)
            config_b["strategy"] = dict(base_config["strategy"])
            config_b["strategy"]["transaction_cost_bps"] = 99.0
            config_b_path = root / "config_b.json"
            config_b_path.write_text(json.dumps(config_b), encoding="utf-8")

            first = PreparedDataCache(Config.load(config_a_path)).load_or_build()
            second = PreparedDataCache(Config.load(config_b_path)).load_or_build()
            self.assertFalse(first.cache_hit)
            self.assertTrue(second.cache_hit)
            self.assertTrue(second.source_cache_hit)
            self.assertTrue(second.feature_cache_hit)
            self.assertTrue(second.prepared_cache_hit)

    def test_feature_stage_change_reuses_source_cache_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for dirname in ("data", "output"):
                (root / dirname).mkdir()
            self._write_dataset(root)

            base_config = {
                "paths": {
                    "output_dir": "output",
                    "compustat_quarterly": "data/sample_compustat_quarterly.csv",
                    "crsp_daily": "data/sample_crsp_daily.csv",
                    "ccm_link": "data/sample_ccm_link.csv",
                    "ibes_link": "data/sample_ibes_link.csv",
                    "ibes_summary": "data/sample_ibes_summary.csv",
                    "ibes_surprise": "data/sample_ibes_surprise.csv",
                    "kpss_patent": "data/sample_kpss_patent.csv",
                    "ff_factors": "data/sample_ff_factors.csv",
                    "fred_dgs10": "data/sample_fred_dgs10.csv",
                    "cboe_vix": "data/sample_cboe_vix.csv",
                    "fmp_grades": "data/sample_fmp_grades.csv"
                },
                "cache": {"enabled": True, "cache_dir": "output/cache"},
                "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "use_rdq": True,
                    "report_lag_days": 45,
                    "beta_lookback_days": 126,
                    "min_beta_observations": 60
                }
            }
            config_a_path = root / "config_a.json"
            config_a_path.write_text(json.dumps(base_config), encoding="utf-8")
            config_b = dict(base_config)
            config_b["strategy"] = dict(base_config["strategy"])
            config_b["strategy"]["report_lag_days"] = 60
            config_b_path = root / "config_b.json"
            config_b_path.write_text(json.dumps(config_b), encoding="utf-8")

            first = PreparedDataCache(Config.load(config_a_path)).load_or_build()
            second = PreparedDataCache(Config.load(config_b_path)).load_or_build()
            self.assertFalse(first.cache_hit)
            self.assertFalse(second.cache_hit)
            self.assertTrue(second.source_cache_hit)
            self.assertFalse(second.feature_cache_hit)
            self.assertFalse(second.prepared_cache_hit)

    def test_beta_stage_change_reuses_source_and_feature_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for dirname in ("data", "output"):
                (root / dirname).mkdir()
            self._write_dataset(root)

            base_config = {
                "paths": {
                    "output_dir": "output",
                    "compustat_quarterly": "data/sample_compustat_quarterly.csv",
                    "crsp_daily": "data/sample_crsp_daily.csv",
                    "ccm_link": "data/sample_ccm_link.csv",
                    "ibes_link": "data/sample_ibes_link.csv",
                    "ibes_summary": "data/sample_ibes_summary.csv",
                    "ibes_surprise": "data/sample_ibes_surprise.csv",
                    "kpss_patent": "data/sample_kpss_patent.csv",
                    "ff_factors": "data/sample_ff_factors.csv",
                    "fred_dgs10": "data/sample_fred_dgs10.csv",
                    "cboe_vix": "data/sample_cboe_vix.csv",
                    "fmp_grades": "data/sample_fmp_grades.csv"
                },
                "cache": {"enabled": True, "cache_dir": "output/cache"},
                "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "use_rdq": True,
                    "report_lag_days": 45,
                    "beta_lookback_days": 126,
                    "min_beta_observations": 60
                }
            }
            config_a_path = root / "config_a.json"
            config_a_path.write_text(json.dumps(base_config), encoding="utf-8")
            config_b = dict(base_config)
            config_b["strategy"] = dict(base_config["strategy"])
            config_b["strategy"]["beta_lookback_days"] = 252
            config_b_path = root / "config_b.json"
            config_b_path.write_text(json.dumps(config_b), encoding="utf-8")

            first = PreparedDataCache(Config.load(config_a_path)).load_or_build()
            second = PreparedDataCache(Config.load(config_b_path)).load_or_build()
            self.assertFalse(first.cache_hit)
            self.assertFalse(second.cache_hit)
            self.assertTrue(second.source_cache_hit)
            self.assertTrue(second.feature_cache_hit)
            self.assertFalse(second.prepared_cache_hit)

    def test_risk_penalty_change_reuses_source_and_feature_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for dirname in ("data", "output"):
                (root / dirname).mkdir()
            self._write_dataset(root)

            base_config = {
                "paths": {
                    "output_dir": "output",
                    "compustat_quarterly": "data/sample_compustat_quarterly.csv",
                    "crsp_daily": "data/sample_crsp_daily.csv",
                    "ccm_link": "data/sample_ccm_link.csv",
                    "ibes_link": "data/sample_ibes_link.csv",
                    "ibes_summary": "data/sample_ibes_summary.csv",
                    "ibes_surprise": "data/sample_ibes_surprise.csv",
                    "kpss_patent": "data/sample_kpss_patent.csv",
                    "ff_factors": "data/sample_ff_factors.csv",
                    "fred_dgs10": "data/sample_fred_dgs10.csv",
                    "cboe_vix": "data/sample_cboe_vix.csv",
                    "fmp_grades": "data/sample_fmp_grades.csv"
                },
                "cache": {"enabled": True, "cache_dir": "output/cache"},
                "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "use_rdq": True,
                    "report_lag_days": 45,
                    "beta_lookback_days": 126,
                    "min_beta_observations": 60,
                    "risk_penalty_downside_beta_weight": 0.10,
                    "risk_penalty_idio_vol_weight": 0.05
                }
            }
            config_a_path = root / "config_a.json"
            config_a_path.write_text(json.dumps(base_config), encoding="utf-8")
            config_b = dict(base_config)
            config_b["strategy"] = dict(base_config["strategy"])
            config_b["strategy"]["risk_penalty_downside_beta_weight"] = 0.25
            config_b_path = root / "config_b.json"
            config_b_path.write_text(json.dumps(config_b), encoding="utf-8")

            first = PreparedDataCache(Config.load(config_a_path)).load_or_build()
            second = PreparedDataCache(Config.load(config_b_path)).load_or_build()
            self.assertFalse(first.cache_hit)
            self.assertFalse(second.cache_hit)
            self.assertTrue(second.source_cache_hit)
            self.assertTrue(second.feature_cache_hit)
            self.assertFalse(second.prepared_cache_hit)

    def test_regime_penalty_change_reuses_source_and_feature_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for dirname in ("data", "output"):
                (root / dirname).mkdir()
            self._write_dataset(root)

            base_config = {
                "paths": {
                    "output_dir": "output",
                    "compustat_quarterly": "data/sample_compustat_quarterly.csv",
                    "crsp_daily": "data/sample_crsp_daily.csv",
                    "ccm_link": "data/sample_ccm_link.csv",
                    "ibes_link": "data/sample_ibes_link.csv",
                    "ibes_summary": "data/sample_ibes_summary.csv",
                    "ibes_surprise": "data/sample_ibes_surprise.csv",
                    "kpss_patent": "data/sample_kpss_patent.csv",
                    "ff_factors": "data/sample_ff_factors.csv",
                    "fred_dgs10": "data/sample_fred_dgs10.csv",
                    "cboe_vix": "data/sample_cboe_vix.csv",
                    "fmp_grades": "data/sample_fmp_grades.csv"
                },
                "cache": {"enabled": True, "cache_dir": "output/cache"},
                "strategy": {
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "use_rdq": True,
                    "report_lag_days": 45,
                    "beta_lookback_days": 126,
                    "min_beta_observations": 60,
                    "risk_penalty_downside_beta_weight": 0.10,
                    "risk_penalty_idio_vol_weight": 0.05,
                    "regime_risk_scaling_enabled": True,
                    "regime_vix_penalty_multiplier": 1.5
                }
            }
            config_a_path = root / "config_a.json"
            config_a_path.write_text(json.dumps(base_config), encoding="utf-8")
            config_b = dict(base_config)
            config_b["strategy"] = dict(base_config["strategy"])
            config_b["strategy"]["regime_vix_penalty_multiplier"] = 2.0
            config_b_path = root / "config_b.json"
            config_b_path.write_text(json.dumps(config_b), encoding="utf-8")

            first = PreparedDataCache(Config.load(config_a_path)).load_or_build()
            second = PreparedDataCache(Config.load(config_b_path)).load_or_build()
            self.assertFalse(first.cache_hit)
            self.assertFalse(second.cache_hit)
            self.assertTrue(second.source_cache_hit)
            self.assertTrue(second.feature_cache_hit)
            self.assertFalse(second.prepared_cache_hit)

    def _write_dataset(self, root: Path) -> None:
        (root / "data" / "sample_compustat_quarterly.csv").write_text(
            "gvkey,datadate,rdq,sic,atq,ltq,ceqq,saleq,ibq,oancfy\n"
            "001001,2025-01-15,2025-02-10,3571,500,200,300,250,20,25\n"
            "001002,2025-01-15,2025-02-11,2834,450,250,200,200,10,12\n"
            "001001,2024-01-15,2024-02-12,3571,450,180,270,230,18,22\n"
            "001002,2024-01-15,2024-02-13,2834,430,245,185,195,9,10\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_crsp_daily.csv").write_text(
            "permno,date,ret,dlret,prc,shrout\n"
            "10001,2025-01-31,0,0,100,2000\n"
            "10002,2025-01-31,0,0,80,2000\n"
            "10001,2025-02-28,0,0,101,2000\n"
            "10002,2025-02-28,0,0,79.84,2000\n"
            "10001,2025-03-31,0,0,102.00,2000\n"
            "10002,2025-03-31,0,0,80.00,2000\n"
            "10001,2025-04-01,0.0100,0,103.02,2000\n"
            "10002,2025-04-01,-0.0020,0,79.84,2000\n"
            "10001,2025-04-02,-0.0050,0,102.50,2000\n"
            "10002,2025-04-02,0.0040,0,80.16,2000\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_ccm_link.csv").write_text(
            "gvkey,permno,linkdt,linkenddt\n"
            "001001,10001,2020-01-01,2030-12-31\n"
            "001002,10002,2020-01-01,2030-12-31\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_ibes_link.csv").write_text(
            "ticker,permno,linkdt,linkenddt\n"
            "AAA,10001,2020-01-01,2030-12-31\n"
            "BBB,10002,2020-01-01,2030-12-31\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_ibes_summary.csv").write_text(
            "ticker,statpers,fpedats,meanest,stdev,numest,measure\n"
            "AAA,2024-12-15,2025-03-31,2.00,0.20,10,EPS\n"
            "AAA,2025-01-20,2025-03-31,2.20,0.18,11,EPS\n"
            "BBB,2024-12-15,2025-03-31,1.50,0.25,9,EPS\n"
            "BBB,2025-01-20,2025-03-31,1.40,0.30,8,EPS\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_ibes_surprise.csv").write_text(
            "ticker,statpers,fpedats,actual,surprise,surpct\n"
            "AAA,2025-01-25,2024-12-31,2.30,0.15,7.0\n"
            "BBB,2025-01-25,2024-12-31,1.35,-0.10,-6.5\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_kpss_patent.csv").write_text(
            "gvkey,issue_date,patent_count,citation_count\n"
            "001001,2024-10-01,4,20\n"
            "001002,2024-10-01,1,3\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_ff_factors.csv").write_text(
            "date,mktrf,smb,hml,umd,rf\n"
            "2025-04-01,0.15,0.02,0.01,0.03,0.01\n"
            "2025-04-02,-0.05,0.01,0.00,-0.02,0.01\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_fred_dgs10.csv").write_text(
            "date,value\n"
            "2024-11-29,4.50\n"
            "2025-01-31,4.20\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_cboe_vix.csv").write_text(
            "DATE,OPEN,HIGH,LOW,CLOSE\n"
            "2025-01-31,18.0,19.0,17.5,18.5\n"
            "2025-02-28,18.5,19.5,18.0,19.0\n"
            "2025-03-31,19.0,20.0,18.5,19.5\n"
            "2025-04-01,19.5,20.5,18.7,19.2\n"
            "2025-04-02,18.8,19.8,18.2,18.9\n",
            encoding="utf-8",
        )
        (root / "data" / "sample_fmp_grades.csv").write_text(
            "symbol,publishedDate,newGrade,previousGrade,gradingCompany,action\n"
            "AAA,2025-01-10,Buy,Hold,Broker A,Upgrade\n"
            "AAA,2025-01-20,Strong Buy,Buy,Broker B,Upgrade\n"
            "BBB,2025-01-18,Hold,Buy,Broker C,Downgrade\n",
            encoding="utf-8",
        )
