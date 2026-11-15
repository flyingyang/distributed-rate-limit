import json
import unittest
from unittest.mock import Mock, patch

from scripts.ha import Topology, command, controlled_handoff, declared_image


class TopologyToolsTests(unittest.TestCase):
    @patch("scripts.ha.compose")
    def test_maintenance_requires_one_declared_image_without_temporary_override(self, compose):
        config = {"services": {node: {"image": "redis:7.4.3-alpine"}
                               for node in ("redis", "redis-b", "redis-c")}}
        compose.return_value = json.dumps(config)
        self.assertEqual(declared_image(), "redis:7.4.3-alpine")
        with self.assertRaises(ValueError):
            declared_image("redis:7.4.2-alpine")
        config["services"]["redis-b"]["image"] = "redis:7.4.2-alpine"
        compose.return_value = json.dumps(config)
        with self.assertRaises(RuntimeError):
            declared_image()

    @patch("scripts.ha.command")
    @patch("scripts.ha.info")
    def test_handoff_demotes_promoted_peer_before_releasing_old_writes(self, info, command):
        state = {"promoted": False, "demoted": False}
        topology = Mock()
        topology.addresses = {"redis": "172.30.85.20",
                              "redis-b": "172.30.85.21", "redis-c": "172.30.85.22"}
        topology.primary.side_effect = lambda: "redis-b" if state["demoted"] else "redis"
        topology.healthy.side_effect = lambda: "redis-b" if state["demoted"] else False
        info.return_value = {"master_repl_offset": "12", "slave_repl_offset": "12"}

        def native(node, *args):
            if args[0] == "SENTINEL" and args[1] == "failover":
                state["promoted"] = True
            elif args[0] == "ROLE":
                if node == "redis":
                    return ["slave" if state["demoted"] else "master"]
                return ["master" if node == "redis-b" and state["promoted"] else "slave"]
            elif args[0] == "REPLICAOF":
                self.assertTrue(state["promoted"])
                self.assertEqual(args[1:], ("172.30.85.21", 6379))
                state["demoted"] = True
            elif args[:2] == ("CLIENT", "UNPAUSE"):
                self.assertTrue(state["demoted"], "must not release a writable old primary")
            return "OK"

        command.side_effect = native
        self.assertEqual(controlled_handoff(topology, "redis"), "redis-b")

    @patch("scripts.ha.compose")
    def test_info_has_known_raw_output_but_other_invalid_json_is_an_error(self, compose):
        compose.return_value = "# Replication\nrole:master\nmaster_repl_offset:12"
        self.assertEqual(command("redis", "INFO", "replication"), compose.return_value)
        compose.return_value = "this is not a Redis JSON reply"
        with self.assertRaises(ValueError):
            command("redis", "GET", "key")

    @patch("scripts.ha.command")
    @patch("scripts.ha.info")
    def test_bootstrap_hostname_is_primary_identity_but_foreign_host_is_not(self, info, command):
        topology = object.__new__(Topology)
        topology.unavailable = set()
        topology.addresses = {"redis": "172.30.85.20",
                              "redis-b": "172.30.85.21", "redis-c": "172.30.85.22"}
        topology.primary = Mock(return_value="redis")
        replica = {"role": "slave", "master_link_status": "up",
                   "master_host": "redis", "master_port": "6379", "slave_repl_offset": "12"}
        info.side_effect = lambda node: {"master_repl_offset": "12"} if node == "redis" else replica
        command.side_effect = lambda node, *args: (
            ["172.30.85.20", "6379"] if args[1] == "get-master-addr-by-name"
            else "OK 3 usable Sentinels")
        self.assertEqual(topology.healthy(), "redis")
        replica["master_host"] = "unrelated"
        self.assertFalse(topology.healthy())
