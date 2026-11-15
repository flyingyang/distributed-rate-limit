import unittest
from unittest.mock import Mock, patch

import redis

from scripts.redis_connection import RedisTarget, validate_connection


CONFIG = {
    "redis_mode": "sentinel",
    "sentinel_master": "limiter",
    "sentinel_nodes": [{"host": "sentinel-a", "port": 26379},
                       {"host": "sentinel-b", "port": 26379}],
}


class ConnectionTests(unittest.TestCase):
    def test_invalid_discovery_never_falls_back_to_direct_host(self):
        for config in (
            {"redis_mode": "sentinel", "sentinel_nodes": CONFIG["sentinel_nodes"]},
            {**CONFIG, "sentinel_nodes": []},
            {**CONFIG, "sentinel_nodes": [{"host": "s", "port": 0}]},
            {"redis_mode": "direct", "sentinel_master": "limiter"},
        ):
            with self.subTest(config=config), self.assertRaises(ValueError):
                validate_connection(config)

    @patch("scripts.redis_connection.redis.Redis")
    @patch("scripts.redis_connection.Sentinel")
    def test_role_validation_discards_stale_endpoint(self, sentinel, client):
        sentinel.return_value.discover_master.side_effect = [
            ("old", 6379), ("new", 6379),
        ]
        old, new = Mock(), Mock()
        old.role.return_value = ["slave", "new", 6379, "connected", 0]
        new.role.return_value = ["master", 0, []]
        client.side_effect = [old, new]
        target = RedisTarget(CONFIG)
        self.assertIs(target.connect(), new)
        old.close.assert_called_once()
        self.assertEqual(sentinel.return_value.discover_master.call_count, 2)

    @patch("scripts.redis_connection.redis.Redis")
    @patch("scripts.redis_connection.Sentinel")
    def test_only_readonly_preexecution_rejection_can_replay_eval(self, sentinel, client):
        sentinel.return_value.discover_master.side_effect = [
            ("old", 6379), ("new", 6379),
        ]
        old, new = Mock(), Mock()
        old.role.return_value = new.role.return_value = ["master", 0, []]
        old.eval.side_effect = redis.ReadOnlyError("You can't write against a read only replica.")
        new.eval.return_value = ["allow"]
        client.side_effect = [old, new]
        target = RedisTarget(CONFIG)
        self.assertEqual(target.eval("script", 0), ["allow"])
        old.close.assert_called_once()
        self.assertEqual(old.eval.call_count + new.eval.call_count, 2)

    @patch("scripts.redis_connection.redis.Redis")
    @patch("scripts.redis_connection.Sentinel")
    def test_ambiguous_eval_is_never_replayed(self, sentinel, client):
        sentinel.return_value.discover_master.return_value = ("primary", 6379)
        for error in (redis.TimeoutError("lost reply"), redis.ConnectionError("EOF")):
            with self.subTest(error=error):
                connection = Mock()
                connection.role.return_value = ["master", 0, []]
                connection.eval.side_effect = error
                client.return_value = connection
                target = RedisTarget(CONFIG)
                with self.assertRaises(type(error)):
                    target.eval("script", 0)
                self.assertEqual(connection.eval.call_count, 1)
                connection.close.assert_called_once()
