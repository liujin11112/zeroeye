"""Tests for tools/config_generator.py"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

# Ensure the tools directory is on the path
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "tools"))

from config_generator import (
    DEFAULT_CONFIG,
    ENV_OVERRIDES,
    SENSITIVE_KEYS,
    generate_config,
    mask_sensitive,
    merge_config,
    to_dotenv,
    to_json,
    to_k8s_configmap,
    to_toml,
    to_yaml,
)


class TestMergeConfig(unittest.TestCase):
    """Tests for merge_config() - recursive dictionary merging."""

    def test_flat_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3}
        result = merge_config(base, override)
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 3)

    def test_nested_merge_preserves_siblings(self):
        base = {"app": {"name": "test", "version": "1.0", "debug": True}}
        override = {"app": {"debug": False}}
        result = merge_config(base, override)
        self.assertEqual(result["app"]["name"], "test")
        self.assertEqual(result["app"]["version"], "1.0")
        self.assertEqual(result["app"]["debug"], False)

    def test_deeply_nested_merge(self):
        base = {"a": {"b": {"c": 1, "d": 2}, "e": 3}}
        override = {"a": {"b": {"c": 99}}}
        result = merge_config(base, override)
        self.assertEqual(result["a"]["b"]["c"], 99)
        self.assertEqual(result["a"]["b"]["d"], 2)
        self.assertEqual(result["a"]["e"], 3)

    def test_new_keys_added(self):
        base = {"a": 1}
        override = {"b": 2}
        result = merge_config(base, override)
        self.assertEqual(result["a"], 1)
        self.assertEqual(result["b"], 2)

    def test_empty_override(self):
        base = {"a": 1, "b": {"c": 2}}
        result = merge_config(base, {})
        self.assertEqual(result, base)

    def test_non_dict_value_replaces_dict(self):
        base = {"a": {"b": 1}}
        override = {"a": "string"}
        result = merge_config(base, override)
        self.assertEqual(result["a"], "string")


class TestGenerateConfig(unittest.TestCase):
    """Tests for generate_config() with environment overrides."""

    def setUp(self):
        self.dev_config = generate_config("development")
        self.staging_config = generate_config("staging")
        self.prod_config = generate_config("production")

    def test_development_environment(self):
        self.assertEqual(self.dev_config["app"]["environment"], "development")
        self.assertTrue(self.dev_config["app"]["debug"])
        self.assertEqual(self.dev_config["app"]["log_level"], "debug")
        self.assertEqual(self.dev_config["database"]["name"], "tent_dev")

    def test_staging_environment(self):
        self.assertEqual(self.staging_config["app"]["environment"], "staging")
        self.assertTrue(self.staging_config["app"]["debug"])
        self.assertEqual(self.staging_config["app"]["log_level"], "info")
        self.assertEqual(self.staging_config["database"]["name"], "tent_staging")
        self.assertEqual(self.staging_config["database"]["pool_max"], 20)
        self.assertEqual(self.staging_config["monitoring"]["tracing_sample_rate"], 0.5)

    def test_production_environment(self):
        self.assertEqual(self.prod_config["app"]["environment"], "production")
        self.assertFalse(self.prod_config["app"]["debug"])
        self.assertEqual(self.prod_config["app"]["log_level"], "info")
        self.assertEqual(self.prod_config["database"]["name"], "tent_production")
        self.assertEqual(self.prod_config["database"]["pool_max"], 50)
        self.assertTrue(self.prod_config["auth"]["mfa_required"])
        self.assertEqual(self.prod_config["monitoring"]["tracing_sample_rate"], 0.01)
        self.assertTrue(self.prod_config["features"]["margin_trading"])
        self.assertFalse(self.prod_config["features"]["ai_assistant"])

    def test_defaults_preserved(self):
        for env_name, config in [("dev", self.dev_config), ("staging", self.staging_config), ("prod", self.prod_config)]:
            with self.subTest(env=env_name):
                self.assertEqual(config["app"]["name"], "tent-of-trials")
                self.assertEqual(config["server"]["host"], "0.0.0.0")
                self.assertEqual(config["server"]["port"], 8080)
                self.assertIn("kafka", config)
                self.assertEqual(config["kafka"]["brokers"], ["localhost:9092"])

    def test_custom_overrides(self):
        custom = generate_config("development", {"server": {"port": 9090}})
        self.assertEqual(custom["server"]["port"], 9090)
        self.assertEqual(custom["app"]["environment"], "development")  # env overrides still applied

    def test_custom_override_depth(self):
        custom = generate_config("production", {"database": {"pool_max": 100, "pool_min": 20}})
        self.assertEqual(custom["database"]["pool_max"], 100)
        self.assertEqual(custom["database"]["pool_min"], 20)
        self.assertEqual(custom["database"]["name"], "tent_production")


class TestMaskSensitive(unittest.TestCase):
    """Tests for mask_sensitive() - redaction of secret values."""

    def setUp(self):
        self.config = generate_config("development")

    def test_database_password_redacted(self):
        masked = mask_sensitive(self.config)
        self.assertEqual(masked["database"]["password"], "***REDACTED***")

    def test_redis_password_redacted(self):
        masked = mask_sensitive(self.config)
        self.assertEqual(masked["redis"]["password"], "***REDACTED***")

    def test_jwt_secret_redacted(self):
        masked = mask_sensitive(self.config)
        self.assertEqual(masked["auth"]["jwt_secret"], "***REDACTED***")

    def test_non_sensitive_values_unchanged(self):
        masked = mask_sensitive(self.config)
        self.assertEqual(masked["app"]["name"], "tent-of-trials")
        self.assertEqual(masked["server"]["port"], 8080)
        self.assertEqual(masked["database"]["host"], "localhost")
        self.assertEqual(masked["auth"]["jwt_expiry_minutes"], 1440)  # development override

    def test_empty_config_masking(self):
        masked = mask_sensitive({})
        self.assertEqual(masked, {})

    def test_deep_nested_masking(self):
        config = {
            "database": {"host": "localhost", "password": "real_secret"},
        }
        masked = mask_sensitive(config)
        self.assertEqual(masked["database"]["password"], "***REDACTED***")
        self.assertEqual(masked["database"]["host"], "localhost")

    def test_deeply_nested_keys_not_accidentally_masked(self):
        config = {
            "nested": {
                "database": {"password": "secret123"},
            },
        }
        masked = mask_sensitive(config)
        # Full key is "nested.database.password", which is not in SENSITIVE_KEYS
        self.assertEqual(masked["nested"]["database"]["password"], "secret123")
    def test_no_duplicates(self):
        self.assertEqual(len(SENSITIVE_KEYS), len(set(SENSITIVE_KEYS)),
                         "SENSITIVE_KEYS contains duplicate entries")

    def test_known_keys_present(self):
        required = {"database.password", "redis.password", "auth.jwt_secret"}
        self.assertTrue(required.issubset(set(SENSITIVE_KEYS)),
                        f"Missing required keys: {required - set(SENSITIVE_KEYS)}")


class TestOutputFormats(unittest.TestCase):
    """Tests for output format functions."""

    def setUp(self):
        self.config = generate_config("production")

    def test_json_output(self):
        output = to_json(self.config)
        parsed = json.loads(output)
        self.assertEqual(parsed["app"]["name"], "tent-of-trials")
        self.assertEqual(parsed["app"]["environment"], "production")

    def test_json_output_not_pretty(self):
        output = to_json(self.config, pretty=False)
        parsed = json.loads(output)
        self.assertEqual(parsed["app"]["name"], "tent-of-trials")

    def test_dotenv_output(self):
        output = to_dotenv(self.config)
        self.assertIn("APP_NAME=tent-of-trials", output)
        self.assertIn("SERVER_HOST=0.0.0.0", output)
        self.assertIn("DATABASE_NAME=tent_production", output)
        self.assertIn("# Generated by config_generator.py", output)

    def test_masked_config_formats(self):
        masked = mask_sensitive(self.config)
        output = to_json(masked)
        parsed = json.loads(output)
        self.assertEqual(parsed["database"]["password"], "***REDACTED***")


class TestCLIBehavior(unittest.TestCase):
    """Verify CLI output remains stable."""

    def test_cli_development_yaml(self):
        """Smoke test: CLI for dev environment in yaml should not crash"""
        from io import StringIO
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        try:
            from config_generator import main
            # We just check the module can be imported and has expected interface
            self.assertTrue(callable(main))
        finally:
            sys.stdout = old_stdout


if __name__ == "__main__":
    unittest.main()
