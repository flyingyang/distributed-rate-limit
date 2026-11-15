local typedefs = require "kong.db.schema.typedefs"

local function identifier(value)
  if #value > 64 or not value:match("^[%w_-]+$") then
    return nil, "must contain 1-64 letters, digits, '_' or '-'"
  end
  return true
end

local function discovery(config)
  if config.redis_mode == "sentinel" then
    if not config.sentinel_master or not config.sentinel_nodes or #config.sentinel_nodes == 0 then
      return nil, "Sentinel mode requires sentinel_master and sentinel_nodes"
    end
  elseif config.sentinel_master or (config.sentinel_nodes and #config.sentinel_nodes > 0) then
    return nil, "Sentinel settings require redis_mode=sentinel"
  end
  return true
end

return {
  name = "distributed-rate-limit",
  fields = {
    {consumer = typedefs.no_consumer},
    {protocols = typedefs.protocols_http},
    {config = {
      type = "record",
      custom_validator = discovery,
      fields = {
        {redis_mode = {type="string", default="direct", one_of={"direct", "sentinel"}}},
        {sentinel_master = {type="string", custom_validator=identifier}},
        {sentinel_nodes = {
          type="array", len_min=1, len_max=5,
          elements={type="record", fields={
            {host={type="string", required=true}},
            {port=typedefs.port({default=26379})},
          }},
        }},
        {sentinel_username = {type="string"}},
        {sentinel_password = {type="string", referenceable=true}},
        {sentinel_ssl = {type="boolean", default=false}},
        {namespace = {type="string", required=true, custom_validator=identifier}},
        {policy_id = {type="string", required=true, custom_validator=identifier}},
        {redis_host = {type="string", required=true, default="redis"}},
        {redis_port = typedefs.port({default=6379})},
        {redis_database = {type="integer", default=0, between={0, 15}}},
        {redis_username = {type="string"}},
        {redis_password = {type="string", referenceable=true}},
        {redis_ssl = {type="boolean", default=false}},
        {redis_ssl_verify = {type="boolean", default=true}},
        {redis_connect_timeout_ms = {type="integer", default=200, between={1, 2000}}},
        {redis_timeout_ms = {type="integer", default=200, between={1, 2000}}},
        {refresh_budget_ms = {type="integer", default=1000, between={100, 5000}}},
      },
    }},
  },
}
