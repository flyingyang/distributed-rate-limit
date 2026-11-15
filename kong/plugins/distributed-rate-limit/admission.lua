local state_key = KEYS[1]
local expected_revision, path, consumer, ip = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local prefix = state_key:sub(1, -6)

local function key_type(key)
  return redis.call("TYPE", key).ok
end

local state_type = key_type(state_key)
if state_type ~= "hash" then return {"error", "active policy is unavailable"} end
local revision = redis.call("HGET", state_key, "active")
if not revision then return {"error", "active revision is missing"} end
-- Even zero matches or missing identity must pass this gate before any writes.
if revision ~= expected_revision then return {"stale", revision} end

local snapshot_key = prefix .. "snapshot:" .. revision
if key_type(snapshot_key) ~= "string" then
  return {"error", "active snapshot is unavailable"}
end
local decoded, policy = pcall(cjson.decode, redis.call("GET", snapshot_key))
if not decoded then return {"error", "invalid active snapshot JSON"} end
local valid, err = core.validate(policy, true)
if not valid then return {"error", err} end
if prefix ~= "drl:" .. policy.namespace .. ":" .. policy.id .. ":" then
  return {"error", "snapshot binding mismatch"}
end
if type(path) ~= "string" or #path > 8192 or path:sub(1, 1) ~= "/"
  or type(consumer) ~= "string" or #consumer > 128
  or type(ip) ~= "string" or #ip == 0 or #ip > 64 then
  return {"error", "invalid request facts"}
end
local pools, missing_consumer = core.pools(policy, path, consumer, ip)
if missing_consumer then return {"forbidden", "Consumer identity is required"} end
if #pools == 0 then return {"allow"} end

local sequence_key = prefix .. "sequence"
local sequence_type = key_type(sequence_key)
if sequence_type ~= "none" and sequence_type ~= "string" then
  return {"error", "invalid sequence key type"}
end
local sequence = redis.call("GET", sequence_key)
if sequence and (not sequence:match("^%d+$") or #sequence > 15) then
  return {"error", "invalid or exhausted admission sequence"}
end
for _, pool in ipairs(pools) do
  pool.key = prefix .. "pool:" .. pool.rule.id .. ":" .. core.hex(pool.identity)
  local kind = key_type(pool.key)
  if kind ~= "none" and kind ~= "zset" then
    return {"error", "invalid quota pool type"}
  end
end

local clock = redis.call("TIME")
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local score = clock[1] .. "." .. string.format("%06d", tonumber(clock[2]))
local retry_after = 0
for _, pool in ipairs(pools) do
  local rule = pool.rule
  local lower = "(" .. string.format("%.6f", now - rule.window_seconds)
  local count = redis.call("ZCOUNT", pool.key, lower, "+inf")
  if count >= rule.limit then
    local rank = count - rule.limit
    local record = redis.call(
      "ZRANGEBYSCORE", pool.key, lower, "+inf", "WITHSCORES", "LIMIT", rank, 1
    )
    if #record ~= 2 then return {"error", "inconsistent quota history"} end
    retry_after = math.max(retry_after,
      math.ceil(tonumber(record[2]) + rule.window_seconds - now))
  end
end
if retry_after > 0 then return {"reject", tostring(math.max(1, retry_after))} end

-- Redis scripts do not roll back runtime errors; all known inputs/types were
-- checked above. An unexpected write failure is ambiguous and must fail closed.
redis.call("INCR", sequence_key)
local member = redis.call("GET", sequence_key)
for _, pool in ipairs(pools) do
  local retention = pool.rule.retain_seconds or pool.rule.window_seconds
  redis.call("ZREMRANGEBYSCORE", pool.key, "-inf",
    string.format("%.6f", now - retention))
  redis.call("ZADD", pool.key, score, member)
  redis.call("PEXPIRE", pool.key, retention * 1000 + 1000)
end
return {"allow"}
