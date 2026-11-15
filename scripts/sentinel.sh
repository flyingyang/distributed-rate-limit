#!/bin/sh
set -eu
config=/data/sentinel.conf
if [ ! -f "$config" ]; then
  {
    printf '%s\n' 'bind 0.0.0.0' 'protected-mode no' 'port 26379' 'dir /data'
    printf '%s\n' 'sentinel resolve-hostnames yes'
    printf 'sentinel announce-ip %s\n' "$SENTINEL_ANNOUNCE_IP"
    printf '%s\n' 'sentinel monitor limiter redis 6379 2'
    printf '%s\n' 'sentinel down-after-milliseconds limiter 2000'
    printf '%s\n' 'sentinel failover-timeout limiter 10000'
    printf '%s\n' 'sentinel parallel-syncs limiter 1'
  } > "$config.new"
  mv "$config.new" "$config"
fi
exec /usr/local/bin/docker-entrypoint.sh redis-server "$config" --sentinel
