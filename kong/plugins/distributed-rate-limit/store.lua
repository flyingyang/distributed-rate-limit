local cjson = require "cjson.safe"
local core = require "kong.plugins.distributed-rate-limit.core"
local primary = require "kong.plugins.distributed-rate-limit.primary"

local store = {}
local cache = {}
local binding_cache_keys = {}
local directory = debug.getinfo(1, "S").source:sub(2):match("^(.*[/\\])")

local function source(name)
  local file = assert(io.open(directory .. name, "r"))
  local content = file:read("*a")
  file:close()
  return content
end

local admission_script = "local core = (function()\n" .. source("core.lua")
  .. "\nend)()\n" .. source("admission.lua")

local connection = {}
connection.__index = connection

local function cache_key(prefix, pool)
  local key = pool .. ":" .. prefix
  local previous = binding_cache_keys[prefix]
  if previous and previous ~= key then cache[previous] = nil end
  binding_cache_keys[prefix] = key
  return key
end

function store.connect(conf)
  local deadline = ngx.now() + conf.refresh_budget_ms / 1000
  local client, pool, err = primary.connect(conf, deadline)
  if not client then return nil, err end
  local prefix = "drl:" .. conf.namespace .. ":" .. conf.policy_id .. ":"
  return setmetatable({
    client=client, prefix=prefix, cache_key=cache_key(prefix, pool),
    deadline=deadline, conf=conf,
  }, connection)
end

function connection:load()
  if ngx.now() > self.deadline then return nil, nil, "policy refresh budget exhausted" end
  local cached = cache[self.cache_key]
  if cached then return cached.policy, cached.revision end
  local revision, err = self.client:hget(self.prefix .. "state", "active")
  if not revision or revision == ngx.null then
    return nil, nil, "active policy unavailable: " .. tostring(err)
  end
  local content
  content, err = self.client:get(self.prefix .. "snapshot:" .. revision)
  if not content or content == ngx.null then
    return nil, nil, "active snapshot unavailable: " .. tostring(err)
  end
  local policy
  policy, err = cjson.decode(content)
  if not policy then return nil, nil, "invalid snapshot JSON: " .. tostring(err) end
  local valid
  valid, err = core.validate(policy, true)
  if not valid then return nil, nil, err end
  if self.prefix ~= "drl:" .. policy.namespace .. ":" .. policy.id .. ":" then
    return nil, nil, "snapshot binding mismatch"
  end
  cache[self.cache_key] = {policy=policy, revision=revision}
  return policy, revision
end

function connection:admit(revision, facts)
  if ngx.now() > self.deadline then return nil, "policy refresh budget exhausted" end
  local result, err = self.client:eval(admission_script, 1, self.prefix .. "state",
    revision, facts.path, facts.consumer or "", facts.ip)
  if not result and err and err:match("^READONLY ")
    and not self.recovered and self.conf.redis_mode == "sentinel" then
    -- Redis rejected EVAL before execution. Reconnect, then let the core reload
    -- the new primary's policy before any new admission attempt.
    self.recovered = true
    self.client:close()
    self:invalidate()
    primary.invalidate(self.conf)
    local client, pool
    client, pool, err = primary.connect(self.conf, self.deadline)
    if not client then return nil, err end
    self.client = client
    self.cache_key = cache_key(self.prefix, pool)
    return {"stale", "primary-changed"}
  end
  if not result then return nil, "Redis admission failed: " .. tostring(err) end
  if type(result) ~= "table" or #result < 1 then
    return nil, "unconfirmed Redis admission result"
  end
  return result
end

function connection:invalidate()
  cache[self.cache_key] = nil
end

function connection:close(healthy)
  if not healthy then
    self.client:close()
    primary.invalidate(self.conf)
    self:invalidate()
    return
  end
  local ok, err = self.client:set_keepalive(10000, 100)
  if not ok then
    ngx.log(ngx.ERR, "distributed-rate-limit: Redis keepalive failed: ", err)
    self.client:close()
  end
end

return store
