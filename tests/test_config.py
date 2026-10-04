import tempfile
import unittest
from pathlib import Path

from coindcx_futures.config import load_settings


class ConfigTests(unittest.TestCase):
    def test_defaults_are_paper_and_live_limits_are_unset(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = load_settings(Path(directory) / "missing.toml")
        self.assertEqual(settings.execution_mode, "paper")
        self.assertFalse(settings.risk_limits.configured())
        self.assertEqual(settings.candidate_count_per_side, 3)

    def test_example_config_is_valid_and_isolated_margin(self):
        example = Path(__file__).parents[1] / "config.example.toml"
        settings = load_settings(example)
        self.assertEqual(settings.margin_currency, "INR")
        self.assertEqual(settings.margin_type, "isolated")
        self.assertFalse(settings.risk_limits.configured())


if __name__ == "__main__":
    unittest.main()

