import tempfile
import unittest
from pathlib import Path

from scripts.publish import read_configuration


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.policies = self.root / "policies.yaml"
        self.kong = self.root / "kong.yaml"
        self.policies.write_text("""namespace: demo
policies:
  - id: main
    rules:
      - id: total
        limit_by: ip
        limit: 10
        window_seconds: 60
""")
        self.kong.write_text("""plugins:
  - name: distributed-rate-limit
    config:
      namespace: demo
      policy_id: main
""")

    def test_reference_and_policy_namespace(self):
        documents = read_configuration(self.policies, self.kong)
        self.assertEqual(documents[0]["namespace"], "demo")
        self.assertEqual(documents[0]["id"], "main")

    def test_missing_reference_and_reused_binding_are_errors(self):
        self.kong.write_text("""plugins:
  - name: distributed-rate-limit
    config: {namespace: demo, policy_id: missing}
""")
        with self.assertRaisesRegex(ValueError, "reference"):
            read_configuration(self.policies, self.kong)
        self.kong.write_text("""services:
  - name: one
    plugins:
      - name: distributed-rate-limit
        config: {namespace: demo, policy_id: main}
  - name: two
    plugins:
      - name: distributed-rate-limit
        config: {namespace: demo, policy_id: main}
""")
        with self.assertRaisesRegex(ValueError, "reused"):
            read_configuration(self.policies, self.kong)

    def test_duplicate_yaml_keys_and_unknown_root_fields_are_errors(self):
        self.policies.write_text("namespace: demo\nnamespace: surprise\npolicies: []")
        with self.assertRaisesRegex(ValueError, "duplicate"):
            read_configuration(self.policies, self.kong)
        self.policies.write_text("namespace: demo\npolicies: []\npolciies: []")
        with self.assertRaisesRegex(ValueError, "unknown"):
            read_configuration(self.policies, self.kong)
