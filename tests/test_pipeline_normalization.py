from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str((Path(__file__).resolve().parents[1] / "src")))

from quant_research.config import Config
from quant_research.pipeline import DataPipeline
from quant_research.utils import normalize_cross_section


class PipelineNormalizationTest(unittest.TestCase):
    def test_normalize_cross_section_robust_softens_outlier_impact(self) -> None:
        values = [1.0, 1.1, 1.2, 20.0]

        standard_scores = normalize_cross_section(values, method="standard", winsor_quantile=0.0)
        robust_scores = normalize_cross_section(values, method="robust", winsor_quantile=0.05)

        self.assertGreater(abs(standard_scores[-1]), abs(robust_scores[-1]))
        self.assertLess(abs(robust_scores[-1]), 2.5)
    def test_pipeline_uses_feature_normalization_settings(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        values = [0.5, 0.6, 0.7, 10.0]

        pipeline.feature_zscore_method = "standard"
        pipeline.feature_winsor_quantile = 0.0
        standard_scores = pipeline._normalize_feature_values(values)
    
        pipeline.feature_zscore_method = "robust"
        pipeline.feature_winsor_quantile = 0.05
        robust_scores = pipeline._normalize_feature_values(values)

        self.assertGreater(abs(standard_scores[-1]), abs(robust_scores[-1]))

    def test_pipeline_uses_risk_normalization_settings(self) -> None:
        pipeline = DataPipeline(Config.load(Path("config/sample_config.json")))
        values = [0.1, 0.12, 0.13, 1.5]

        pipeline.risk_zscore_method = "standard"
        pipeline.risk_winsor_quantile = 0.0
        standard_scores = pipeline._normalize_risk_values(values)

        pipeline.risk_zscore_method = "robust"
        pipeline.risk_winsor_quantile = 0.05
        robust_scores = pipeline._normalize_risk_values(values)

        self.assertGreater(abs(standard_scores[-1]), abs(robust_scores[-1]))


if __name__ == "__main__":
    unittest.main()


