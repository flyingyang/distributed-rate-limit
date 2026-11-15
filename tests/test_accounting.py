import concurrent.futures
import json
import unittest

from tests.support import (
    admit, cleanup, policy, prefix, redis_client, rule, seed,
)


class AccountingTests(unittest.TestCase):
    def make_policy(self, rules):
        document = policy(rules)
        self.addCleanup(cleanup, document)
        return document, seed(document)

    def histories(self, document):
        client = redis_client()
        return {key: client.zcard(key) for key in
                client.scan_iter(match=prefix(document) + "pool:*")}

    def test_concurrent_admission_is_unique_and_bounded(self):
        doc, revision = self.make_policy([rule(limit=25, window=10)])
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            results = list(pool.map(lambda _: admit(doc, revision), range(80)))
        self.assertEqual(sum(result[0] == "allow" for result in results), 25)
        self.assertEqual(sum(result[0] == "reject" for result in results), 55)
        self.assertEqual(list(self.histories(doc).values()), [25])

    def test_rejection_is_all_or_nothing_and_missing_identity_does_not_charge(self):
        doc, revision = self.make_policy([
            rule("path", "path", 2, 10, path="/orders", path_match="prefix"),
            rule("consumer", "consumer", 1, 10),
            rule("ip", "ip", 10, 10),
        ])
        self.assertEqual(admit(doc, revision)[0], "allow")
        before = self.histories(doc)
        self.assertEqual(admit(doc, revision)[0], "reject")
        self.assertEqual(self.histories(doc), before)
        self.assertEqual(admit(doc, revision, consumer="")[0], "forbidden")
        self.assertEqual(self.histories(doc), before)
        self.assertEqual(admit(doc, revision, consumer="bob")[0], "allow")
        self.assertEqual(admit(doc, revision, consumer="carol")[0], "reject")
        self.assertEqual(sorted(self.histories(doc).values()), [1, 1, 2, 2])

    def test_stale_no_match_and_identity_errors_gate_before_mutation(self):
        doc, revision = self.make_policy([
            rule("private", "consumer", 1, 10, path="/private", path_match="prefix")
        ])
        self.assertEqual(admit(doc, "stale", path="/public", consumer="")[0], "stale")
        self.assertEqual(admit(doc, "stale", path="/private", consumer="")[0], "stale")
        self.assertEqual(admit(doc, revision, path="/public", consumer="")[0], "allow")
        self.assertEqual(self.histories(doc), {})

    def test_retry_after_uses_lowered_limit_rank_and_max_wait(self):
        doc, revision = self.make_policy([
            rule("ip", "ip", 2, 10), rule("client", "consumer", 1, 20),
        ])
        client = redis_client()
        seconds, micros = client.time()
        now = seconds + micros / 1_000_000
        ip_key = prefix(doc) + "pool:ip:" + "127.0.0.1".encode().hex()
        consumer_key = prefix(doc) + "pool:client:" + b"alice".hex()
        client.zadd(ip_key, {"a": now - 9, "b": now - 8, "c": now - 3})
        client.zadd(consumer_key, {"x": now - 12})
        result = admit(doc, revision)
        # IP needs its second record to expire (~2s); Consumer needs ~8s.
        self.assertEqual(result[0], "reject")
        self.assertEqual(int(result[1]), 8)
        client.delete(consumer_key)
        result = admit(doc, revision)
        self.assertEqual(int(result[1]), 2)

    def test_bad_pool_type_fails_before_other_pool_writes(self):
        doc, revision = self.make_policy([
            rule("first", "ip"), rule("second", "consumer"),
        ])
        client = redis_client()
        client.set(prefix(doc) + "pool:second:" + b"alice".hex(), "wrong-type")
        self.assertEqual(admit(doc, revision)[0], "error")
        self.assertFalse(client.exists(
            prefix(doc) + "pool:first:" + "127.0.0.1".encode().hex()
        ))

    def test_retained_old_records_do_not_count_and_expiration_is_exact(self):
        doc = policy([dict(rule(limit=1, window=2), retain_seconds=10)])
        self.addCleanup(cleanup, doc)
        revision = seed(doc)
        client = redis_client()
        seconds, micros = client.time()
        now = seconds + micros / 1_000_000
        key = prefix(doc) + "pool:total:" + "127.0.0.1".encode().hex()
        client.zadd(key, {"old": now - 5})
        self.assertEqual(admit(doc, revision)[0], "allow")
        self.assertEqual(client.zcard(key), 2)
        self.assertGreaterEqual(client.pttl(key), 10_000)
