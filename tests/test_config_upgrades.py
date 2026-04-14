from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from quant_research.config import Config


class ConfigUpgradeTest(unittest.TestCase):
    def test_load_accepts_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            config_path.write_text('{"paths": {"output_dir": "output"}}', encoding="utf-8-sig")

            config = Config.load(config_path)

        self.assertEqual(config.paths["output_dir"], "output")

    def test_resolve_path_expands_environment_variables(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "env-data"
            data_dir.mkdir()
            config_path = root / "config.json"
            config_path.write_text(json.dumps({"paths": {"data_dir": "%QUANT_TEST_DATA%"}}), encoding="utf-8")

            with patch.dict(os.environ, {"QUANT_TEST_DATA": str(data_dir)}, clear=False):
                config = Config.load(config_path)
                self.assertEqual(config.resolve("data_dir"), data_dir.resolve())


if __name__ == "__main__":
    unittest.main()
