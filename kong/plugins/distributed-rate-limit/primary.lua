local redis = require "resty.redis"
local primary = {}
local addresses, generations = {}, {}

local function settings_id(conf)
  local parts = {
    conf.redis_mode or "direct", conf.redis_host or "", conf.redis_port or 6379,
    conf.redis_database or 0, conf.redis_username or "", conf.redis_password or "",
    tostring(conf.redis_ssl), tostring(conf.redis_ssl_verify),
    conf.sentinel_master or "", conf.sentinel_username or "",
    conf.sentinel_password or "", tostring(conf.sentinel_ssl),
  }
  for _, node in ipairs(conf.sentinel_nodes or {}) do
    parts[#parts + 1] = node.host .. ":" .. node.port
  end
  for index, value in ipairs(parts) do
    local text = tostring(value)
    parts[index] = #text .. ":" .. text
  end
  return ngx.md5(table.concat(parts))
end

function primary.invalidate(conf)
  local id = settings_id(conf)
  addresses[id] = nil
  generations[id] = (generations[id] or 0) + 1
end

local function dial(conf, host, port, deadline, sentinel, pool)
  local remaining = math.floor((deadline - ngx.now()) * 1000)
  if remaining <= 0 then return nil, "Redis discovery budget exhausted" end
  local client, err = redis:new()
  if not client then return nil, "Redis client creation failed: " .. tostring(err) end
  client:set_timeouts(
    math.min(conf.redis_connect_timeout_ms, remaining),
    math.min(conf.redis_timeout_ms, remaining),
    math.min(conf.redis_timeout_ms, remaining)
  )
  local ssl
  if sentinel then ssl = conf.sentinel_ssl else ssl = conf.redis_ssl end
  local ok
  ok, err = client:connect(host, port, {
    pool=pool, pool_size=100, backlog=100, ssl=ssl == true,
    ssl_verify=sentinel or conf.redis_ssl_verify, server_name=host,
  })
  if not ok then client:close(); return nil, "Redis connection failed: " .. tostring(err) end
  local password, username
  if sentinel then
    password, username = conf.sentinel_password, conf.sentinel_username
  else
    password, username = conf.redis_password, conf.redis_username
  end
  if password then
    if username then ok, err = client:auth(username, password)
    else ok, err = client:auth(password) end
    if not ok then
      client:close()
      return nil, "Redis authentication failed: " .. tostring(err)
    end
  end
  return client
end

local function writable(conf, endpoint, deadline, id)
  local pool = id .. ":" .. endpoint[1] .. ":" .. endpoint[2]
    .. ":" .. (generations[id] or 0)
  local client, err = dial(conf, endpoint[1], endpoint[2], deadline, false, pool)
  if not client then return nil, nil, err end
  local role
  role, err = client:role()
  if not role or role[1] ~= "master" then
    client:close()
    return nil, nil, "Redis endpoint is not a writable primary: " .. tostring(err)
  end
  local ok
  ok, err = client:select(conf.redis_database)
  if not ok then
    client:close()
    return nil, nil, "Redis database selection failed: " .. tostring(err)
  end
  return client, pool
end

function primary.connect(conf, deadline)
  local id = settings_id(conf)
  if conf.redis_mode ~= "sentinel" then
    return writable(conf, {conf.redis_host, conf.redis_port}, deadline, id)
  end
  local cached = addresses[id]
  if cached and cached.expires > ngx.now() then
    local client, pool = writable(conf, cached.endpoint, deadline, id)
    if client then return client, pool end
    primary.invalidate(conf)
  end
  local last_error = "Sentinel does not know the configured primary"
  for _, node in ipairs(conf.sentinel_nodes) do
    local sentinel, err = dial(conf, node.host, node.port, deadline, true, "sentinel:" .. id)
    if sentinel then
      local endpoint
      endpoint, err = sentinel:sentinel("get-master-addr-by-name", conf.sentinel_master)
      sentinel:close()
      if type(endpoint) == "table" and type(endpoint[1]) == "string"
        and tonumber(endpoint[2]) and tonumber(endpoint[2]) >= 1
        and tonumber(endpoint[2]) <= 65535 then
        endpoint[2] = tonumber(endpoint[2])
        local previous = addresses[id]
        if previous and (previous.endpoint[1] ~= endpoint[1]
          or previous.endpoint[2] ~= endpoint[2]) then
          primary.invalidate(conf)
        end
        local client, pool
        client, pool, err = writable(conf, endpoint, deadline, id)
        if client then
          addresses[id] = {endpoint=endpoint, expires=ngx.now() + 0.5}
          return client, pool
        end
      end
    end
    last_error = err or last_error
    if ngx.now() >= deadline then break end
  end
  primary.invalidate(conf)
  return nil, nil, "Sentinel primary discovery failed: " .. tostring(last_error)
end

return primary
