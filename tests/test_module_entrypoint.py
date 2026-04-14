from __future__ import annotations

import unittest
from unittest.mock import patch

import quant_research.__main__ as module_entrypoint


class ModuleEntrypointTest(unittest.TestCase):
    def test_module_entrypoint_delegates_to_cli(self) -> None:
        with patch.object(module_entrypoint, "cli_main") as cli_main:
            module_entrypoint.main()

        cli_main.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
