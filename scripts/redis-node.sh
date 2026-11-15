#!/bin/sh
set -eu
config=/data/redis.conf
if [ ! -f "$config" ]; then
  {
    printf '%s\n' 'bind 0.0.0.0' 'protected-mode no' 'port 6379' 'dir /data'
    printf '%s\n' 'appendonly yes' 'appendfsync everysec' 'maxmemory-policy noeviction'
    printf 'replica-announce-ip %s\n' "$REDIS_ANNOUNCE_IP"
    if [ -n "${REDIS_BOOTSTRAP_PRIMARY:-}" ]; then
      printf 'replicaof %s 6379\n' "$REDIS_BOOTSTRAP_PRIMARY"
    fi
  } > "$config.new"
  mv "$config.new" "$config"
fi
# Sentinel's CONFIG REWRITE persists role changes into this same file.
exec /usr/local/bin/docker-entrypoint.sh redis-server "$config"
