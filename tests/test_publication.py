import concurrent.futures
import copy
import time
import unittest

from tests.support import (
    admit, cleanup, policy, prefix, publish_raw, redis_client, rule, state,
)


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.document = policy([rule(limit=3, window=1)])
        self.addCleanup(cleanup, self.document)

    def activate(self):
        result = publish_raw(self.document)
        self.assertEqual(result[0], "active", result)
        return result[1]

    def test_status_after_primary_change_cannot_claim_preparation_is_ready(self):
        self.activate()
        target = copy.deepcopy(self.document)
        target["rules"][0]["window_seconds"] = 2
        time.sleep(0.11)
        self.assertEqual(publish_raw(target)[0], "preparing")
        client = redis_client()
        key = prefix(target) + "state"
        client.hset(key, mapping={
            "ready_at": str(sum_time(client) - 0.1),
            "run_id": "previous-primary",
        })
        before = state(target)
        result = publish_raw(target, "status")
        self.assertEqual(result[0], "rewarming")
        self.assertGreaterEqual(float(result[2]), 2000)
        self.assertEqual(state(target), before)
        self.assertEqual(publish_raw(target)[0], "preparing")
        self.assertGreater(float(state(target)["ready_at"]), sum_time(client))

    def test_identical_and_limit_updates_preserve_consumption(self):
        revision = self.activate()
        self.assertEqual(admit(self.document, revision)[0], "allow")
        self.assertEqual(admit(self.document, revision)[0], "allow")
        self.assertEqual(publish_raw(self.document)[1], revision)
        lower = copy.deepcopy(self.document)
        lower["rules"][0]["limit"] = 1
        time.sleep(0.11)
        updated = publish_raw(lower)
        self.assertEqual(updated[0], "active")
        self.assertEqual(admit(lower, revision)[0], "stale")
        self.assertEqual(admit(lower, updated[1])[0], "reject")
        lower["rules"][0]["limit"] = 4
        time.sleep(0.11)
        increased = publish_raw(lower)
        self.assertEqual(admit(lower, increased[1])[0], "allow")

    def test_invalid_and_structural_changes_leave_active_policy_untouched(self):
        revision = self.activate()
        changed = copy.deepcopy(self.document)
        changed["rules"][0]["limit_by"] = "consumer"
        self.assertEqual(publish_raw(changed)[0], "error")
        self.assertEqual(state(self.document)["active"], revision)
        changed["rules"][0]["limit"] = 0
        self.assertEqual(publish_raw(changed)[0], "error")
        self.assertEqual(state(self.document)["active"], revision)

    def test_window_preparation_keeps_old_policy_and_is_resumable(self):
        revision = self.activate()
        target = copy.deepcopy(self.document)
        target["rules"][0]["window_seconds"] = 2
        target["rules"][0]["limit"] = 2
        time.sleep(0.11)
        started = publish_raw(target)
        self.assertEqual(started[0], "preparing")
        preparation_revision = state(target)["active"]
        self.assertEqual(admit(target, revision)[0], "stale")
        self.assertEqual(admit(target, preparation_revision)[0], "allow")
        ready_at = state(target)["ready_at"]
        self.assertEqual(publish_raw(target)[0], "preparing")
        self.assertEqual(state(target)["ready_at"], ready_at)
        competing = copy.deepcopy(target)
        competing["rules"][0]["limit"] = 8
        self.assertEqual(publish_raw(competing)[0], "error")
        time.sleep(1.15)
        # Old one-second policy still allows; retained admission remains in the log.
        self.assertEqual(admit(target, preparation_revision)[0], "allow")
        client = redis_client()
        key = next(client.scan_iter(match=prefix(target) + "pool:*"))
        self.assertEqual(client.zcard(key), 2)
        remaining = max(0, float(ready_at) - sum_time(client))
        time.sleep(remaining + 0.03)
        activated = publish_raw(target)
        self.assertEqual(activated[0], "active")
        # Both old-policy admissions still fall inside the new two-second window.
        # The earliest one is now just expired, so one new admission is available.
        self.assertEqual(admit(target, activated[1])[0], "allow")
        self.assertEqual(admit(target, activated[1])[0], "reject")

    def test_shorter_window_and_cancellation_preserve_history(self):
        self.document["rules"][0]["window_seconds"] = 2
        revision = self.activate()
        self.assertEqual(admit(self.document, revision)[0], "allow")
        target = copy.deepcopy(self.document)
        target["rules"][0]["window_seconds"] = 1
        time.sleep(0.11)
        shorter = publish_raw(target)
        self.assertEqual(shorter[0], "active")
        target["rules"][0]["window_seconds"] = 3
        time.sleep(0.11)
        self.assertEqual(publish_raw(target)[0], "preparing")
        self.assertEqual(publish_raw(target, "cancel")[0], "cancelled")
        current = state(target)["active"]
        self.assertEqual(admit(target, current)[0], "allow")
        self.assertEqual(publish_raw(target)[0], "busy")
        time.sleep(0.11)
        self.assertEqual(publish_raw(target)[0], "preparing")

    def test_concurrent_updates_are_serialized_and_status_is_truthful(self):
        self.activate()
        targets = []
        for limit in (5, 6):
            target = copy.deepcopy(self.document)
            target["rules"][0]["limit"] = limit
            targets.append(target)
        time.sleep(0.11)
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(publish_raw, targets))
        self.assertEqual(sorted(result[0] for result in results), ["active", "busy"])
        statuses = [publish_raw(doc, "status")[0] for doc in targets]
        self.assertEqual(sorted(statuses), ["active", "different"])

    def test_lost_preparation_incarnation_requires_full_new_warmup(self):
        self.activate()
        target = copy.deepcopy(self.document)
        target["rules"][0]["window_seconds"] = 2
        time.sleep(0.11)
        self.assertEqual(publish_raw(target)[0], "preparing")
        client = redis_client()
        client.hset(prefix(target) + "state", mapping={
            "ready_at": str(sum_time(client) - 1),
            "run_id": "a-different-redis-incarnation",
        })
        result = publish_raw(target)
        self.assertEqual(result[0], "preparing")
        self.assertGreaterEqual(float(result[2]), 1900)

    def test_invalid_preparation_metadata_never_reports_ready_or_activates(self):
        self.activate()
        target = copy.deepcopy(self.document)
        target["rules"][0]["window_seconds"] = 3
        time.sleep(0.11)
        self.assertEqual(publish_raw(target)[0], "preparing")
        prepared = state(target)
        client = redis_client()
        key = prefix(target) + "state"
        for field, value in (
            ("ready_at", "nan"),
            ("ready_at", "inf"),
            ("ready_at", "-inf"),
            ("ready_at", "0"),
            ("run_id", None),
            ("run_id", ""),
        ):
            with self.subTest(field=field, value=value):
                client.hset(key, mapping=prepared)
                if value is None:
                    client.hdel(key, field)
                else:
                    client.hset(key, field, value)
                damaged = state(target)
                for operation in ("status", "apply"):
                    self.assertEqual(publish_raw(target, operation)[0], "error")
                    self.assertEqual(state(target), damaged)
        self.assertEqual(publish_raw(target, "cancel")[0], "cancelled")
        self.assertEqual(state(target)["active"], prepared["active"])
        self.assertEqual(admit(target, prepared["active"])[0], "allow")


def sum_time(client):
    seconds, micros = client.time()
    return seconds + micros / 1_000_000
