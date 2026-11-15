"""Destructive only to the demo namespace. Requires three actual Kong gateways."""

import concurrent.futures
import copy
import json
import os
import socket
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from scripts.publish import read_configuration
from tests.support import ROOT, cleanup, prefix, publish_raw, redis_client, state


URLS = os.environ.get("KONG_URLS", "").split(",")
CONSUMERS = {
    "alice": "11111111-1111-4111-8111-111111111111",
    "bob": "22222222-2222-4222-8222-222222222222",
    "carol": "33333333-3333-4333-8333-333333333333",
    "dave": "44444444-4444-4444-8444-444444444444",
}


def request(url, path, consumer=None, headers=None, timeout=3):
    combined = dict(headers or {})
    if consumer:
        combined["apikey"] = consumer + "-demo-key"
    try:
        response = urlopen(Request(url + path, headers=combined), timeout=timeout)
    except HTTPError as error:
        response = error
    with response:
        return response.status, dict(response.headers), response.read()


@unittest.skipUnless(os.environ.get("KONG_URLS"), "requires running demo gateways")
class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.documents = read_configuration(
            ROOT / "config" / "policies.yaml", ROOT / "config" / "kong.yaml"
        )
        if any(doc["namespace"] != "demo" for doc in self.documents):
            self.fail("HTTP tests may only reset the demo namespace")
        self.by_id = {doc["id"]: doc for doc in self.documents}
        for document in self.documents:
            cleanup(document)
            self.assertEqual(publish_raw(document)[0], "active")

    def histories(self, policy_id="public-api"):
        client = redis_client()
        document = self.by_id[policy_id]
        return {key: client.zrange(key, 0, -1, withscores=True) for key in
                client.scan_iter(match=prefix(document) + "pool:*")}

    def count(self, rule_id, identity, policy_id="public-api"):
        key = prefix(self.by_id[policy_id]) + "pool:" + rule_id + ":" + identity.encode().hex()
        return redis_client().zcard(key)

    def update(self, document):
        time.sleep(0.11)
        result = publish_raw(document)
        self.assertIn(result[0], {"active", "preparing"}, result)
        return result

    def test_three_gateways_share_all_pools_under_concurrency(self):
        consumers = list(CONSUMERS)

        def send(index):
            return request(URLS[index % 3], "/orders/" + str(index),
                           consumers[index % 4])[0]

        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            statuses = list(pool.map(send, range(80)))
        self.assertLess(time.monotonic() - start, 3, "test exceeded the rolling window")
        self.assertEqual(statuses.count(200), 12, statuses)
        self.assertEqual(statuses.count(429), 68, statuses)
        self.assertEqual(self.count("orders-total", "/orders"), 12)
        for name, identity in CONSUMERS.items():
            subtotal = self.count("client-orders", identity)
            self.assertLessEqual(subtotal, 4, name)
            self.assertEqual(self.count("client-total", identity), subtotal)

    def test_query_prefix_boundaries_and_total_across_routes(self):
        for index, path in enumerate((
            "/orders?page=1", "/orders/123", "/orders?page=2", "/orders/456",
        )):
            self.assertEqual(request(URLS[index % 3], path, "alice")[0], 200)
        status, headers, _ = request(URLS[1], "/orders", "alice")
        self.assertEqual(status, 429)
        self.assertGreaterEqual(int(headers["Retry-After"]), 1)
        self.assertEqual(request(URLS[0], "/orders-backup", "alice")[0], 200)
        self.assertEqual(self.count("orders-total", "/orders"), 4)
        for _ in range(3):
            self.assertEqual(request(URLS[2], "/products", "alice")[0], 200)
        self.assertEqual(request(URLS[0], "/products", "alice")[0], 429)
        self.assertEqual(self.count("client-total", CONSUMERS["alice"]), 8)

    def test_auth_missing_consumer_and_rejection_do_not_consume(self):
        self.assertEqual(request(URLS[0], "/orders")[0], 401)
        self.assertEqual(self.histories(), {})
        self.assertEqual(request(URLS[1], "/private")[0], 403)
        self.assertEqual(self.histories("identity-api"), {})
        for _ in range(4):
            self.assertEqual(request(URLS[0], "/orders", "alice")[0], 200)
        before = self.histories()
        self.assertEqual(request(URLS[2], "/orders", "alice")[0], 429)
        self.assertEqual(self.histories(), before)
        # Both zero matches and identity failures exercise cached revisions.
        self.assertEqual(request(URLS[0], "/free")[0], 200)
        changed = copy.deepcopy(self.by_id["identity-api"])
        changed["rules"][0]["limit"] = 1
        self.update(changed)
        self.assertEqual(request(URLS[0], "/free")[0], 200)
        self.assertEqual(request(URLS[1], "/private")[0], 403)

    def test_independent_binding_does_not_share_same_rule_id(self):
        for consumer in CONSUMERS:
            for _ in range(4):
                request(URLS[0], "/orders", consumer)
        self.assertEqual(self.count("orders-total", "/orders"), 12)
        for gateway in URLS:
            self.assertEqual(request(gateway, "/isolated")[0], 200)
        self.assertEqual(request(URLS[0], "/isolated")[0], 429)
        self.assertEqual(self.count("orders-total", "/isolated", "isolated-api"), 3)
        self.assertEqual(self.count("orders-total", "/orders"), 12)

    def test_untrusted_forwarding_headers_cannot_split_ip_quota(self):
        for index in range(2):
            self.assertEqual(request(URLS[index], "/ip", headers={
                "X-Forwarded-For": f"198.51.100.{index + 1}",
            })[0], 200)
        self.assertEqual(request(URLS[2], "/ip", headers={
            "X-Forwarded-For": "203.0.113.99",
        })[0], 429)
        self.assertEqual(len(self.histories("ip-api")), 1)

    def test_trusted_proxy_resolves_client_and_sanitizes_header(self):
        proxy = os.environ.get("PROXY_URL")
        self.assertIsNotNone(proxy, "start the integration proxy")
        for index in range(2):
            self.assertEqual(request(proxy, "/ip", headers={
                "X-Forwarded-For": f"198.51.100.{index + 1}",
            })[0], 200)
        self.assertEqual(request(proxy, "/ip")[0], 429)
        self.assertEqual(len(self.histories("ip-api")), 1)
        # Direct traffic has the same actual source IP despite going through the
        # controlled proxy above; trusting it must not charge the proxy address.
        self.assertEqual(request(URLS[0], "/ip")[0], 429)
        self.assertEqual(len(self.histories("ip-api")), 1)

    def test_live_limits_refresh_and_preserve_history_without_503(self):
        for _ in range(3):
            self.assertEqual(request(URLS[0], "/orders", "alice")[0], 200)
        changed = copy.deepcopy(self.by_id["public-api"])
        changed["rules"][2]["limit"] = 2
        self.update(changed)
        for gateway in URLS:
            self.assertEqual(request(gateway, "/orders", "alice")[0], 429)
        self.assertEqual(self.count("client-orders", CONSUMERS["alice"]), 3)
        changed["rules"][2]["limit"] = 5
        self.update(changed)
        self.assertEqual(request(URLS[1], "/orders", "alice")[0], 200)
        self.assertEqual(request(URLS[2], "/orders", "alice")[0], 200)
        self.assertEqual(request(URLS[0], "/orders", "alice")[0], 429)

    def test_window_warmup_keeps_traffic_serving_and_gates_stale_cleanup(self):
        changed = copy.deepcopy(self.by_id["isolated-api"])
        changed["rules"][0]["window_seconds"] = 4
        changed["rules"][0]["limit"] = 2
        self.assertEqual(request(URLS[0], "/isolated")[0], 200)
        self.assertEqual(self.update(changed)[0], "preparing")
        self.assertEqual(request(URLS[1], "/isolated")[0], 200)
        time.sleep(3.05)
        self.assertEqual(request(URLS[2], "/isolated")[0], 200)
        self.assertEqual(publish_raw(changed)[0], "preparing")
        client = redis_client()
        seconds, micros = client.time()
        remaining = float(state(changed)["ready_at"]) - seconds - micros / 1_000_000
        time.sleep(max(0, remaining) + 0.02)
        self.assertEqual(publish_raw(changed)[0], "active")
        self.assertEqual(request(URLS[0], "/isolated")[0], 200)
        self.assertEqual(request(URLS[1], "/isolated")[0], 429)

    def test_upstream_errors_timeouts_and_disconnects_are_not_refunded(self):
        self.assertEqual(request(URLS[0], "/fail", "alice")[0], 502)
        self.assertEqual(request(URLS[1], "/slow", "alice")[0], 504)
        with self.assertRaises((TimeoutError, socket.timeout)):
            request(URLS[2], "/slow", "alice", timeout=0.05)
        time.sleep(0.1)
        self.assertEqual(self.count("client-total", CONSUMERS["alice"]), 3)
        self.assertEqual(self.count("orders-total", "/orders"), 0)
        status, headers, body = request(URLS[0], "/products", "alice")
        self.assertEqual(status, 200)
        self.assertNotIn("Retry-After", headers)
        self.assertEqual(json.loads(body)["path"], "/products")

    def test_missing_snapshot_is_503_and_recovery_keeps_prior_history(self):
        self.assertEqual(request(URLS[0], "/orders", "alice")[0], 200)
        document = self.by_id["public-api"]
        client = redis_client()
        key = prefix(document) + "snapshot:" + state(document)["active"]
        content = client.get(key)
        client.delete(key)
        try:
            for gateway in URLS:
                status, headers, _ = request(gateway, "/orders", "alice")
                self.assertEqual(status, 503)
                self.assertNotIn("Retry-After", headers)
            self.assertEqual(self.count("client-orders", CONSUMERS["alice"]), 1)
        finally:
            client.set(key, content)
        self.assertEqual(request(URLS[1], "/orders", "alice")[0], 200)
        self.assertEqual(self.count("client-orders", CONSUMERS["alice"]), 2)
