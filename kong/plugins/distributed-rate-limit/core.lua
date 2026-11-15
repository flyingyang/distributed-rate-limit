local core = {}

local function identifier(value)
  return type(value) == "string" and #value >= 1 and #value <= 64
    and value:match("^[%w_-]+$") ~= nil
end

local function integer(value, maximum)
  return type(value) == "number" and value >= 1 and value <= maximum
    and value == math.floor(value)
end

local function only_fields(value, allowed)
  for key in pairs(value) do
    if not allowed[key] then
      return nil, "unknown field: " .. tostring(key)
    end
  end
  return true
end

local function canonical_path(path)
  if type(path) ~= "string" or #path == 0 or #path > 1024
    or path:sub(1, 1) ~= "/" or path:find("[%c%%?#\\]")
    or path:find("//", 1, true) then
    return false
  end
  for segment in path:gmatch("[^/]+") do
    if segment == "." or segment == ".." then
      return false
    end
  end
  return true
end

-- This validator runs unchanged in Kong and in Redis publication scripts.
function core.validate(policy, internal)
  if type(policy) ~= "table" then
    return nil, "policy must be an object"
  end
  local valid, err = only_fields(policy, {namespace=true, id=true, rules=true})
  if not valid then return nil, err end
  if not identifier(policy.namespace) or not identifier(policy.id) then
    return nil, "invalid namespace or policy id"
  end
  if type(policy.rules) ~= "table" or #policy.rules < 1 or #policy.rules > 32 then
    return nil, "rules must contain 1-32 entries"
  end
  local seen = {}
  for index, rule in pairs(policy.rules) do
    if type(index) ~= "number" or index < 1 or index > #policy.rules
      or index ~= math.floor(index) or type(rule) ~= "table" then
      return nil, "rules must be an array of objects"
    end
    valid, err = only_fields(rule, {
      id=true, limit_by=true, path=true, path_match=true,
      limit=true, window_seconds=true, retain_seconds=internal == true,
    })
    if not valid then return nil, err end
    if not identifier(rule.id) or seen[rule.id] then
      return nil, "invalid or duplicate rule id"
    end
    seen[rule.id] = true
    if rule.limit_by ~= "path" and rule.limit_by ~= "consumer"
      and rule.limit_by ~= "ip" then
      return nil, "limit_by must be path, consumer or ip"
    end
    if not integer(rule.limit, 10000) or not integer(rule.window_seconds, 3600) then
      return nil, "limit must be 1-10000; window_seconds must be 1-3600 integers"
    end
    if rule.retain_seconds ~= nil and (
      not integer(rule.retain_seconds, 3600)
      or rule.retain_seconds < rule.window_seconds
    ) then
      return nil, "invalid retention horizon"
    end
    if rule.limit_by == "ip" and (rule.path ~= nil or rule.path_match ~= nil) then
      return nil, "IP path filters are unsupported"
    end
    if rule.limit_by == "path" or rule.path ~= nil or rule.path_match ~= nil then
      if not canonical_path(rule.path) then
        return nil, "path must be canonical absolute text without escapes or dot segments"
      end
      if rule.path_match ~= "exact" and rule.path_match ~= "prefix" then
        return nil, "path_match must be exact or prefix"
      end
    end
  end
  return true
end

function core.matches(rule, path)
  if not rule.path then return true end
  path = path:match("^[^?]*")
  if path == rule.path then return true end
  if rule.path_match ~= "prefix" then return false end
  if rule.path == "/" then return true end
  local prefix = rule.path
  if prefix:sub(-1) ~= "/" then prefix = prefix .. "/" end
  return path:sub(1, #prefix) == prefix
end

function core.hex(value)
  return (value:gsub(".", function(byte)
    return string.format("%02x", string.byte(byte))
  end))
end

function core.pools(policy, path, consumer, ip)
  local pools, missing_consumer = {}, false
  for _, rule in ipairs(policy.rules) do
    if core.matches(rule, path) then
      local identity
      if rule.limit_by == "path" then
        identity = rule.path
      elseif rule.limit_by == "consumer" then
        identity = consumer
        if not identity or identity == "" then missing_consumer = true end
      else
        identity = ip
      end
      pools[#pools + 1] = {rule=rule, identity=identity}
    end
  end
  return pools, missing_consumer
end

function core.same_structure(left, right)
  if #left.rules ~= #right.rules then return false end
  local old = {}
  for _, rule in ipairs(left.rules) do old[rule.id] = rule end
  for _, rule in ipairs(right.rules) do
    local previous = old[rule.id]
    if not previous or previous.limit_by ~= rule.limit_by
      or previous.path ~= rule.path or previous.path_match ~= rule.path_match then
      return false
    end
  end
  return left.namespace == right.namespace and left.id == right.id
end

function core.same_limits(left, right)
  if not core.same_structure(left, right) then return false end
  local old = {}
  for _, rule in ipairs(left.rules) do old[rule.id] = rule end
  for _, rule in ipairs(right.rules) do
    if old[rule.id].limit ~= rule.limit
      or old[rule.id].window_seconds ~= rule.window_seconds then
      return false
    end
  end
  return true
end

function core.decide(storage, facts, attempts)
  for _ = 1, attempts or 8 do
    local snapshot, revision, err = storage:load()
    if not snapshot then return nil, err end
    local result
    result, err = storage:admit(revision, facts)
    if not result then return nil, err end
    if type(result) ~= "table" then return nil, "unconfirmed admission result" end
    if result[1] ~= "stale" then
      if result[1] == "allow" and #result == 1 then return result end
      if result[1] == "forbidden" and #result == 2
        and type(result[2]) == "string" then return result end
      if result[1] == "reject" and #result == 2 and tonumber(result[2])
        and tonumber(result[2]) >= 1
        and tonumber(result[2]) == math.floor(tonumber(result[2])) then
        return result
      end
      return nil, result[1] == "error" and type(result[2]) == "string" and result[2]
        or "unconfirmed admission result"
    end
    if #result ~= 2 or type(result[2]) ~= "string" or #result[2] == 0 then
      return nil, "unconfirmed revision result"
    end
    storage:invalidate()
  end
  return nil, "policy refresh budget exhausted"
end

return core
