import unittest

from tests.support import core_script, redis_client


class MatchingTests(unittest.TestCase):
    """Run the same independent Lua core used by Kong, inside real Redis."""

    def test_path_boundaries_and_shared_pool(self):
        script = core_script() + """
local rule = {id="orders", limit_by="path", path="/orders", path_match="prefix"}
assert(core.matches(rule, "/orders"))
assert(core.matches(rule, "/orders?page=2"))
assert(core.matches(rule, "/orders/123"))
assert(not core.matches(rule, "/orders-backup"))
rule.path_match = "exact"
assert(core.matches(rule, "/orders"))
assert(not core.matches(rule, "/orders/123"))
rule.path = "/"
rule.path_match = "prefix"
assert(core.matches(rule, "/anything"))
rule.path = "/orders/"
assert(core.matches(rule, "/orders/123"))
assert(not core.matches(rule, "/orders"))
local policy = {rules={
  {id="orders", limit_by="path", path="/orders", path_match="prefix"},
  {id="total", limit_by="consumer"},
  {id="subtotal", limit_by="consumer", path="/orders", path_match="exact"},
  {id="ip", limit_by="ip"}
}}
local a, missing = core.pools(policy, "/orders/123", "alice", "127.0.0.1")
assert(not missing and #a == 3)
local b = core.pools(policy, "/orders/456", "bob", "127.0.0.2")
assert(a[1].identity == b[1].identity)
assert(a[2].identity ~= b[2].identity)
local _, absent = core.pools(policy, "/orders", "", "127.0.0.1")
assert(absent)
local public = {rules={{id="filtered", limit_by="consumer",
  path="/private", path_match="prefix"}}}
local skipped, required = core.pools(public, "/public", "", "127.0.0.1")
assert(#skipped == 0 and not required)
return "ok"
"""
        self.assertEqual(redis_client().eval(script, 0), "ok")

    def test_refresh_is_internal_but_ambiguous_mutations_are_never_retried(self):
        script = core_script() + """
local storage = {loads=0, admissions=0, invalidations=0}
function storage:load()
  self.loads = self.loads + 1
  return {rules={}}, tostring(self.loads)
end
function storage:invalidate() self.invalidations = self.invalidations + 1 end
function storage:admit(revision, facts)
  assert(facts.path == "/public")
  self.admissions = self.admissions + 1
  if self.admissions == 1 then return {"stale", "2"} end
  return {"allow"}
end
assert(core.decide(storage, {path="/public"})[1] == "allow")
assert(storage.loads == 2 and storage.invalidations == 1)
storage.admissions = 0
function storage:admit()
  self.admissions = self.admissions + 1
  return nil, "response lost after possible mutation"
end
local result, err = core.decide(storage, {path="/public"})
assert(not result and err and storage.admissions == 1)
function storage:admit() return {"allow", "unexpected"} end
assert(not core.decide(storage, {path="/public"}))
function storage:admit() return {"stale", "new"} end
assert(not core.decide(storage, {path="/public"}, 3))
return "ok"
"""
        self.assertEqual(redis_client().eval(script, 0), "ok")

    def test_validation_rejects_ambiguous_configuration(self):
        script = core_script() + """
local good = {namespace="test", id="main", rules={
  {id="orders", limit_by="path", path="/orders", path_match="prefix",
   limit=10, window_seconds=60}
}}
assert(core.validate(good))
good.rules[1].path = "/orders//items"
assert(not core.validate(good))
good.rules[1].path = "/orders"
good.rules[1].path_match = "regex"
assert(not core.validate(good))
good.rules[1].path_match = "prefix"
good.rules[2] = good.rules[1]
assert(not core.validate(good))
good.rules[2] = nil
good.rules[1].limit = 1.5
assert(not core.validate(good))
good.rules[1].limit = 10
good.rules[1].limti = 20
assert(not core.validate(good))
return "ok"
"""
        self.assertEqual(redis_client().eval(script, 0), "ok")
