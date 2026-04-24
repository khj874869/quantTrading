from __future__ import annotations

import tarfile
import unittest
import zipfile
from pathlib import Path


class DistributionArtifactsTest(unittest.TestCase):
    def test_manifest_includes_commercial_docs(self) -> None:
        manifest_path = Path(__file__).resolve().parents[1] / "MANIFEST.in"
        self.assertTrue(manifest_path.exists())
        manifest_text = manifest_path.read_text(encoding="utf-8")

        for expected_line in [
            "include LICENSE.txt",
            "include SUPPORT.md",
            "include SECURITY.md",
            "include CHANGELOG.md",
            "recursive-include docs *.md",
        ]:
            self.assertIn(expected_line, manifest_text)

    def test_built_sdist_contains_required_policy_docs(self) -> None:
        dist_dir = Path(__file__).resolve().parents[1] / "dist"
        archive_paths = sorted(dist_dir.glob("quant-research-stack-*.tar.gz"))
        if not archive_paths:
            self.skipTest("sdist has not been built")
        archive_path = archive_paths[-1]

        with tarfile.open(archive_path, "r:gz") as archive:
            names = set(archive.getnames())

        expected_suffixes = [
            "LICENSE.txt",
            "SUPPORT.md",
            "SECURITY.md",
            "CHANGELOG.md",
            "docs/commercial_readiness.md",
            "docs/data_sources_and_compliance.md",
            "docs/operations_runbook.md",
            "docs/config_reference.md",
            "docs/output_reference.md",
        ]
        for suffix in expected_suffixes:
            self.assertTrue(any(name.endswith(suffix) for name in names), suffix)

    def test_built_wheel_contains_license_and_typed_marker(self) -> None:
        dist_dir = Path(__file__).resolve().parents[1] / "dist"
        archive_paths = sorted(dist_dir.glob("quant_research_stack-*-py3-none-any.whl"))
        if not archive_paths:
            self.skipTest("wheel has not been built")
        archive_path = archive_paths[-1]

        with zipfile.ZipFile(archive_path) as archive:
            names = set(archive.namelist())

        self.assertIn("quant_research/py.typed", names)
        self.assertTrue(
            any(name.endswith(".dist-info/LICENSE.txt") for name in names),
            "wheel is missing LICENSE.txt",
        )


if __name__ == "__main__":
    unittest.main()
