import json
import os
import uuid
from pathlib import Path

import redis

from scripts.publish import read_bindings
from scripts.redis_connection import target_for

ROOT = Path(__file__).resolve().parents[1]
LUA_DIR = ROOT / "kong" / "plugins" / "distributed-rate-limit"


def redis_client():
    if os.environ.get("REDIS_URL"):
        # Explicit direct mode is retained for isolated old-topology tests.
        return target_for({"redis_mode": "direct"}).connect()
    binding = read_bindings(ROOT / "config" / "kong.yaml")[("demo", "public-api")]
    return target_for(binding, int(os.environ.get("REDIS_DATABASE", "15"))).connect()


def script(name):
    return core_script() + (LUA_DIR / name).read_text()


def core_script():
    return "local core = (function()\n" + (
        LUA_DIR / "core.lua"
    ).read_text() + "\nend)()\n"


def seed(document):
    """Bootstrap accounting tests independently of the publication implementation."""
    client = redis_client()
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"))
    revision = client.eval("return redis.sha1hex(ARGV[1])", 0, encoded)
    client.set(prefix(document) + "snapshot:" + revision, encoded)
    client.hset(prefix(document) + "state", mapping={"active": revision})
    return revision


def policy(rules, namespace=None):
    return {"namespace": namespace or "test-" + uuid.uuid4().hex, "id": "main",
            "rules": rules}


def rule(id="total", by="ip", limit=3, window=2, **extra):
    return dict(id=id, limit_by=by, limit=limit, window_seconds=window, **extra)


def prefix(document):
    return "drl:" + document["namespace"] + ":" + document["id"] + ":"


def admit(document, revision, path="/orders", consumer="alice", ip="127.0.0.1"):
    return redis_client().eval(
        script("admission.lua"), 1, prefix(document) + "state",
        revision, path, consumer, ip,
    )


def publish_raw(document, operation="apply"):
    return redis_client().eval(
        script("publication.lua"), 1, prefix(document) + "state",
        operation, json.dumps(document, separators=(",", ":"), sort_keys=True),
    )


def state(document):
    return redis_client().hgetall(prefix(document) + "state")


def cleanup(document):
    client = redis_client()
    keys = list(client.scan_iter(match=prefix(document) + "*"))
    if keys:
        client.delete(*keys)
