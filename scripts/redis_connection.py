"""Explicit primary discovery; transport failures never replay a mutation."""

import os
import time

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
from redis.sentinel import Sentinel


def validate_connection(config):
    mode = config.get("redis_mode", "direct")
    if mode not in {"direct", "sentinel"}:
        raise ValueError("redis_mode must be direct or sentinel")
    if mode == "direct":
        if config.get("sentinel_master") or config.get("sentinel_nodes"):
            raise ValueError("Sentinel settings require redis_mode: sentinel")
        return
    master = config.get("sentinel_master")
    nodes = config.get("sentinel_nodes")
    if not isinstance(master, str) or not master.strip():
        raise ValueError("sentinel_master is required")
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 5:
        raise ValueError("sentinel_nodes must contain 1-5 endpoints")
    for node in nodes:
        if not isinstance(node, dict) or set(node) != {"host", "port"}:
            raise ValueError("Sentinel endpoint requires only host and port")
        if not isinstance(node["host"], str) or not node["host"].strip():
            raise ValueError("Sentinel host is required")
        port = node["port"]
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("invalid Sentinel port")


class RedisTarget:
    def __init__(self, config, database=None, url=None):
        validate_connection(config)
        self.config = config
        self.database = config.get("redis_database", 0) if database is None else database
        self.url = url
        self.client = None
        self.address = None
        self.sentinel = None
        if config.get("redis_mode") == "sentinel" and not url:
            sentinel_options = {
                "socket_connect_timeout": 0.2, "socket_timeout": 0.2,
                "decode_responses": True, "retry": Retry(NoBackoff(), 0),
                "username": config.get("sentinel_username"),
                "password": config.get("sentinel_password"),
            }
            if config.get("sentinel_ssl"):
                sentinel_options.update(ssl=True, ssl_cert_reqs="required")
            self.sentinel = Sentinel(
                [(node["host"], node["port"]) for node in config["sentinel_nodes"]],
                sentinel_kwargs=sentinel_options,
            )

    def invalidate(self):
        if self.client is not None:
            self.client.close()
        self.client = None
        self.address = None

    def connect(self, deadline=None):
        deadline = deadline or time.monotonic() + 3
        error = None
        for _ in range(2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                address = (
                    self.sentinel.discover_master(self.config["sentinel_master"])
                    if self.sentinel else
                    (self.config.get("redis_host", "redis"),
                     self.config.get("redis_port", 6379))
                )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise redis.ConnectionError("discovery budget exhausted")
                if address != self.address:
                    self.invalidate()
                if self.client is None:
                    options = {
                        "decode_responses": True,
                        "socket_connect_timeout": min(0.2, remaining),
                        "socket_timeout": min(0.5, remaining),
                        "retry": Retry(NoBackoff(), 0), "retry_on_timeout": False,
                    }
                    if self.url:
                        self.client = redis.Redis.from_url(self.url, **options)
                    else:
                        options.update(
                            host=address[0], port=address[1], db=self.database,
                            username=self.config.get("redis_username"),
                            password=self.config.get("redis_password"),
                        )
                        if self.config.get("redis_ssl"):
                            options.update(
                                ssl=True, ssl_cert_reqs=(
                                    "required" if self.config.get("redis_ssl_verify", True)
                                    else "none"
                                ),
                            )
                        self.client = redis.Redis(**options)
                    self.address = address
                role = self.client.role()
                if role[0] != "master":
                    raise redis.ConnectionError("discovered Redis is not a primary")
                return self.client
            except (redis.ConnectionError, redis.TimeoutError) as exc:
                error = exc
                self.invalidate()
        raise redis.ConnectionError(f"primary discovery failed: {error or 'budget exhausted'}")

    def eval(self, *args):
        deadline = time.monotonic() + 3
        for attempt in range(2):
            client = self.connect(deadline)
            try:
                return client.eval(*args)
            except redis.ReadOnlyError:
                # A top-level READONLY rejects EVAL before execution.
                self.invalidate()
                if attempt or time.monotonic() >= deadline:
                    raise
            except (redis.ConnectionError, redis.TimeoutError):
                # The server may have executed the script; never repeat it.
                self.invalidate()
                raise
        raise redis.ConnectionError("primary recovery budget exhausted")


def target_for(config, database=None):
    override = os.environ.get("REDIS_URL")
    if override and config.get("redis_mode") == "sentinel":
        raise ValueError("REDIS_URL direct override conflicts with a Sentinel binding")
    return RedisTarget(config, database, url=override)
