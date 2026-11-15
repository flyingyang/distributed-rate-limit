import unittest

from tests.support import LUA_DIR, redis_client


class PrimaryDiscoveryTests(unittest.TestCase):
    def test_discovery_skips_stale_role_and_never_falls_back(self):
        source = (LUA_DIR / "primary.lua").read_text()
        script = """
local closed = 0
local endpoints = {}
local connections = {}
local function client(address, role, failure)
  return {
    set_timeouts=function() end,
    connect=function(self, host) endpoints[#endpoints+1]=host; return true end,
    sentinel=function() return address end,
    role=function() return role end,
    select=function() return true end,
    close=function() closed=closed+1 end,
    auth=function() return true end,
    set_keepalive=function() return true end,
  }
end
local fake = {new=function() return table.remove(connections, 1) end}
local require = function(name) assert(name=="resty.redis"); return fake end
local ngx = {null=false, now=function() return 1 end,
             md5=function(value) return value end}
local primary = (function()
""" + source + """
end)()
local conf = {
 redis_mode="sentinel", sentinel_master="limiter",
 sentinel_nodes={{host="s1",port=26379},{host="s2",port=26379}},
 redis_host="must-not-use", redis_port=6379, redis_database=0,
 redis_connect_timeout_ms=100, redis_timeout_ms=100,
 redis_ssl=false, redis_ssl_verify=true,
}
connections = {
 client({"old","6379"}), client(nil, {"slave"}),
 client({"new","6379"}), client(nil, {"master"}),
}
local selected, pool, err = primary.connect(conf, 2)
assert(selected and pool and not err)
assert(endpoints[1]=="s1" and endpoints[2]=="old")
assert(endpoints[3]=="s2" and endpoints[4]=="new")
assert(closed >= 1)
connections = {
 client(nil, {"slave"}), client(false), client(false),
}
local absent = primary.connect(conf, 2)
assert(not absent)
for _, host in ipairs(endpoints) do assert(host~="must-not-use") end
return "ok"
"""
        self.assertEqual(redis_client().eval(script, 0), "ok")
