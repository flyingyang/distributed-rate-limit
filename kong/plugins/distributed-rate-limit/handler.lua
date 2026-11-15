local core = require "kong.plugins.distributed-rate-limit.core"
local store = require "kong.plugins.distributed-rate-limit.store"

local handler = {
  PRIORITY = 900,
  VERSION = "1.0.0",
}

function handler:access(conf)
  local storage, err = store.connect(conf)
  if not storage then
    kong.log.err("distributed-rate-limit: ", err)
    return kong.response.exit(503, {message="Rate limiter unavailable"})
  end
  local consumer = kong.client.get_consumer()
  local result
  result, err = core.decide(storage, {
    path=kong.request.get_path(),
    consumer=consumer and consumer.id,
    ip=kong.client.get_forwarded_ip(),
  })
  storage:close(result ~= nil)
  if not result then
    kong.log.err("distributed-rate-limit: ", err)
    return kong.response.exit(503, {message="Rate limiter unavailable"})
  end
  if result[1] == "forbidden" then
    kong.log.notice("distributed-rate-limit: applicable rule requires a Kong Consumer")
    return kong.response.exit(403, {message="Authenticated Consumer required"})
  end
  if result[1] == "reject" then
    return kong.response.exit(429, {message="Rate limit exceeded"}, {
      ["Retry-After"]=result[2],
    })
  end
end

return handler
