local state_key, operation, encoded = KEYS[1], ARGV[1], ARGV[2]
local prefix = state_key:sub(1, -6)
if #encoded > 65536 then return {"error", "policy exceeds 64 KiB"} end
local decoded, desired = pcall(cjson.decode, encoded)
if not decoded then return {"error", "invalid policy JSON"} end
local valid, err = core.validate(desired)
if not valid then return {"error", err} end
if prefix ~= "drl:" .. desired.namespace .. ":" .. desired.id .. ":" then
  return {"error", "policy binding mismatch"}
end
if operation == "validate" then return {"valid"} end
if operation ~= "apply" and operation ~= "status" and operation ~= "cancel" then
  return {"error", "unknown publication operation"}
end

local function key_type(key)
  return redis.call("TYPE", key).ok
end

local kind = key_type(state_key)
if kind ~= "none" and kind ~= "hash" then
  return {"error", "invalid publication state type"}
end
local active_revision = redis.call("HGET", state_key, "active")
local active
if active_revision then
  local snapshot_key = prefix .. "snapshot:" .. active_revision
  if key_type(snapshot_key) ~= "string" then
    return {"error", "active snapshot is missing; repair state, do not reset quota"}
  end
  decoded, active = pcall(cjson.decode, redis.call("GET", snapshot_key))
  if not decoded then return {"error", "invalid active snapshot JSON"} end
  valid, err = core.validate(active, true)
  if not valid then return {"error", err} end
  if active.namespace ~= desired.namespace or active.id ~= desired.id then
    return {"error", "active snapshot binding mismatch"}
  end
end

local clock = redis.call("TIME")
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local desired_revision = redis.sha1hex(encoded)
local pending = redis.call("HGET", state_key, "target")
local ready_at = tonumber(redis.call("HGET", state_key, "ready_at"))
local preparation_run_id = redis.call("HGET", state_key, "run_id")

if operation == "cancel" then
  redis.call("HDEL", state_key, "target", "ready_at", "run_id")
  return {"cancelled", active_revision or ""}
end
if pending and pending ~= desired_revision then
  return {"error", "different preparation in progress; resume or cancel it"}
end
if active and not core.same_structure(active, desired) then
  return {"error", "live structural changes are unsupported"}
end
-- Lua treats NaN as truthy; it must not bypass the readiness comparison.
if pending and (not active or not ready_at or ready_at ~= ready_at
  or ready_at <= 0 or ready_at >= math.huge
  or not preparation_run_id or preparation_run_id == "") then
  return {"error", "preparation metadata is invalid or incomplete"}
end

local run_id = redis.call("INFO", "server"):match("run_id:([^\r\n]+)")
if not run_id then return {"error", "Redis incarnation is unavailable"} end

if operation == "status" then
  if pending then
    if preparation_run_id ~= run_id then
      local full_window = 0
      for _, rule in ipairs(desired.rules) do
        full_window = math.max(full_window, rule.window_seconds)
      end
      return {"rewarming", pending, tostring(full_window * 1000)}
    end
    return {"preparing", pending, tostring(math.max(0, math.ceil((ready_at - now) * 1000)))}
  end
  if not active then return {"missing"} end
  return {core.same_limits(active, desired) and "active" or "different", active_revision}
end
if not pending and active and core.same_limits(active, desired) then
  return {"active", active_revision}
end

local old_by_id = {}
if active then
  for _, rule in ipairs(active.rules) do old_by_id[rule.id] = rule end
end
local warmup = 0
for _, rule in ipairs(desired.rules) do
  local previous = old_by_id[rule.id]
  if previous then
    rule.retain_seconds = math.max(
      previous.retain_seconds or previous.window_seconds, rule.window_seconds
    )
    if rule.window_seconds > previous.window_seconds then
      warmup = math.max(warmup, rule.window_seconds)
    end
  else
    rule.retain_seconds = rule.window_seconds
  end
end

local function save_snapshot(policy)
  local content = cjson.encode(policy)
  local revision = redis.sha1hex(content)
  local key = prefix .. "snapshot:" .. revision
  local snapshot_type = key_type(key)
  if snapshot_type ~= "none" and snapshot_type ~= "string" then
    return nil, "invalid snapshot key type"
  end
  local existing = redis.call("GET", key)
  if existing and existing ~= content then return nil, "immutable snapshot conflict" end
  redis.call("SET", key, content, "NX")
  return revision
end

if pending then
  if preparation_run_id ~= run_id then
    ready_at = now + warmup
    redis.call("HSET", state_key, "ready_at", string.format("%.6f", ready_at),
      "run_id", run_id)
  end
  if now < ready_at then
    return {"preparing", pending, tostring(math.ceil((ready_at - now) * 1000))}
  end
else
  local last_update = tonumber(redis.call("HGET", state_key, "last_update")) or 0
  if now - last_update < 0.1 then return {"busy", "minimum update interval is 100ms"} end
  if warmup > 0 then
    for _, rule in ipairs(active.rules) do
      rule.retain_seconds = math.max(
        rule.retain_seconds or rule.window_seconds,
        -- Structure is stable; locate by ID, never assume authored rule order.
        (function()
          for _, wanted in ipairs(desired.rules) do
            if wanted.id == rule.id then return wanted.window_seconds end
          end
        end)()
      )
    end
    local preparation_revision, save_error = save_snapshot(active)
    if not preparation_revision then return {"error", save_error} end
    ready_at = now + warmup
    redis.call("HSET", state_key, "active", preparation_revision,
      "target", desired_revision, "ready_at", string.format("%.6f", ready_at),
      "run_id", run_id, "last_update", string.format("%.6f", now))
    return {"preparing", desired_revision, tostring(warmup * 1000)}
  end
end

local revision, save_error = save_snapshot(desired)
if not revision then return {"error", save_error} end
redis.call("HSET", state_key, "active", revision,
  "last_update", string.format("%.6f", now))
redis.call("HDEL", state_key, "target", "ready_at", "run_id")
return {"active", revision}
