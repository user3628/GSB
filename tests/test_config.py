import unittest

from ragwb.config import validate_config
from ragwb.errors import E_CONFIG_INVALID, WorkbenchError
from tests.helpers import load_config, make_workspace


class ConfigValidationTest(unittest.TestCase):
    def setUp(self):
        self.config = load_config(make_workspace())

    def test_valid_config_passes(self):
        self.assertIs(validate_config(self.config), self.config)

    def test_missing_section_raises_config_invalid(self):
        config = dict(self.config)
        del config["budget"]
        with self.assertRaises(WorkbenchError) as caught:
            validate_config(config)
        self.assertEqual(caught.exception.code, E_CONFIG_INVALID)
        self.assertIn("budget.global_evidence_evaluations",
                      caught.exception.context["missing"])

    def test_non_positive_budget_rejected(self):
        config = dict(self.config)
        config["budget"] = {"global_evidence_evaluations": 0}
        with self.assertRaises(WorkbenchError) as caught:
            validate_config(config)
        self.assertEqual(caught.exception.code, E_CONFIG_INVALID)


if __name__ == "__main__":
    unittest.main()
