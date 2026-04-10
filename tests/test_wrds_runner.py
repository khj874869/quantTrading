from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quant_research.config import Config
from quant_research.wrds_runner import WRDSConfigError, WRDSExportRunner


class WRDSRunnerTest(unittest.TestCase):
    def _config(self, root: Path) -> Config:
        config_path = root / "config.json"
        config_path.write_text(
            """
            {
              "paths": {
                "compustat_quarterly": "data/compustat.csv",
                "ccm_link": "data/ccm.csv",
                "crsp_daily": "data/crsp.csv",
                "ibes_link": "data/ibes_link.csv",
                "ibes_summary": "data/ibes_summary.csv",
                "ibes_surprise": "data/ibes_surprise.csv",
                "kpss_patent": "data/patent.csv",
                "ff_factors": "data/ff.csv"
              },
              "wrds": {
                "placeholders": {
                  "IBES_SUMMARY_TABLE": "ibes.statsum_epsus"
                },
                "exports": [
                  {
                    "name": "ibes_summary",
                    "sql": "sql/template.sql",
                    "output": "data/ibes_summary.csv"
                  }
                ]
              }
            }
            """,
            encoding="utf-8",
        )
        return Config.load(config_path)

    def test_render_sql_replaces_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sql").mkdir()
            (root / "data").mkdir()
            (root / "sql" / "template.sql").write_text("select * from <IBES_SUMMARY_TABLE>;", encoding="utf-8")
            runner = WRDSExportRunner(self._config(root))
            sql = runner._render_sql("select * from <IBES_SUMMARY_TABLE>;", runner._placeholders())
            self.assertEqual(sql, "select * from ibes.statsum_epsus;")

    def test_render_sql_raises_on_missing_placeholder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "sql").mkdir()
            (root / "data").mkdir()
            (root / "sql" / "template.sql").write_text("select * from <MISSING_TABLE>;", encoding="utf-8")
            runner = WRDSExportRunner(self._config(root))
            with self.assertRaises(WRDSConfigError):
                runner._render_sql("select * from <MISSING_TABLE>;", runner._placeholders())


if __name__ == "__main__":
    unittest.main()
   