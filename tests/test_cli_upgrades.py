from __future__ import annotations

import io
import os
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from quant_research import __version__
from quant_research.main import build_parser, _resolve_config_path


class CliUpgradeTest(unittest.TestCase):
    def test_version_flag_prints_package_version(self) -> None:
        parser = build_parser()
        stdout = io.StringIO()

        with self.assertRaises(SystemExit) as raised:
            with redirect_stdout(stdout):
                parser.parse_args(["--version"])

        self.assertEqual(raised.exception.code, 0)
        self.assertEqual(stdout.getvalue().strip(), f"quant-research {__version__}")

    def test_config_can_come_from_environment(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["validate"])

        with patch.dict(os.environ, {"QUANT_RESEARCH_CONFIG": "config/sample_config.json"}, clear=False):
            resolved = _resolve_config_path(parser, args)

        self.assertEqual(resolved, "config/sample_config.json")


if __name__ == "__main__":
    unittest.main()
