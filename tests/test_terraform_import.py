"""Tests for Terraform resource name validation"""
import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))
from terraform_import import validate_resource_name

class TestValidateResourceName(unittest.TestCase):
    def test_valid_standard(self):
        for n in ["my_resource", "MyResource", "resource_1", "test"]:
            v, m = validate_resource_name(n)
            self.assertTrue(v, f"{n}: {m}")
    def test_hyphen_rejected(self):
        v, m = validate_resource_name("my-resource")
        self.assertFalse(v); self.assertIn("hyphen", m.lower())
    def test_empty_rejected(self):
        v, m = validate_resource_name(""); self.assertFalse(v)
    def test_invalid_chars(self):
        for n in ["123abc", "my.resource", "my resource"]:
            self.assertFalse(validate_resource_name(n)[0])
if __name__ == "__main__":
    unittest.main()
