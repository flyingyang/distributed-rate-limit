"""Publish operator-authored policies; Redis owns validation and activation."""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import redis
import yaml

if __package__ in (None, ""):
    from redis_connection import target_for
else:
    from .redis_connection import target_for


ROOT = Path(__file__).resolve().parents[1]
LUA_DIR = ROOT / "kong" / "plugins" / "distributed-rate-limit"
IDENTIFIER = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class UniqueKeyLoader(yaml.SafeLoader):
    pass


def unique_mapping(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, (str, int, float, bool)):
            raise ValueError("YAML mapping keys must be scalar")
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping
)


def load_yaml(path):
    with Path(path).open(encoding="utf-8") as stream:
        return yaml.load(stream, Loader=UniqueKeyLoader)


def identifier(value, field):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must contain 1-64 letters, digits, '_' or '-'")
    return value


def read_configuration(policies_path, kong_path):
    authored = load_yaml(policies_path)
    if not isinstance(authored, dict):
        raise ValueError("policy document must be a mapping")
    if set(authored) - {"namespace", "policies"}:
        raise ValueError("unknown policy document field")
    namespace = identifier(authored.get("namespace"), "namespace")
    entries = authored.get("policies")
    if not isinstance(entries, list) or not entries:
        raise ValueError("policies must be a nonempty list")
    documents = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"id", "rules"}:
            raise ValueError("each policy requires only id and rules")
        policy_id = identifier(entry["id"], "policy id")
        if policy_id in documents:
            raise ValueError(f"duplicate policy id: {policy_id}")
        documents[policy_id] = dict(namespace=namespace, **entry)

    references = set()

    def visit(node):
        if isinstance(node, dict):
            for plugin in node.get("plugins", []):
                if plugin.get("name") != "distributed-rate-limit":
                    continue
                binding = plugin.get("config", {})
                reference = (binding.get("namespace"), binding.get("policy_id"))
                if reference in references:
                    raise ValueError(f"binding reference reused: {reference}")
                if reference[0] != namespace or reference[1] not in documents:
                    raise ValueError(f"unknown policy reference: {reference}")
                references.add(reference)
            for key, value in node.items():
                if key != "plugins":
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(load_yaml(kong_path))
    for policy_id in documents:
        if (namespace, policy_id) not in references:
            raise ValueError(f"policy has no binding reference: {policy_id}")
    return list(documents.values())


def read_bindings(kong_path):
    bindings = {}

    def visit(node):
        if isinstance(node, dict):
            for plugin in node.get("plugins", []):
                if plugin.get("name") == "distributed-rate-limit":
                    config = plugin["config"]
                    bindings[(config["namespace"], config["policy_id"])] = config
            for key, value in node.items():
                if key != "plugins":
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(load_yaml(kong_path))
    return bindings


class Publisher:
    def __init__(self, client):
        self.client = client
        self.script = "local core = (function()\n" + (
            LUA_DIR / "core.lua"
        ).read_text(encoding="utf-8") + "\nend)()\n" + (
            LUA_DIR / "publication.lua"
        ).read_text(encoding="utf-8")

    def execute(self, document, operation="apply"):
        key = f"drl:{document['namespace']}:{document['id']}:state"
        encoded = json.dumps(document, sort_keys=True, separators=(",", ":"),
                             allow_nan=False)
        # EVAL deliberately has no automatic retry after an ambiguous write.
        try:
            result = self.client.eval(self.script, 1, key, operation, encoded)
        except (redis.ConnectionError, redis.TimeoutError) as exc:
            if operation in {"validate", "status"}:
                raise RuntimeError("Redis read failed") from exc
            # Reconcile by reading state, never by retrying the mutation.
            try:
                current = self.client.eval(self.script, 1, key, "status", encoded)
            except redis.RedisError as read_error:
                raise RuntimeError(
                    "publication outcome unknown; restore Redis and run status"
                ) from read_error
            if current[0] == "active":
                return current
            raise RuntimeError(
                f"publication response lost; reconciled status: {current[0]}; "
                "run status or resume apply"
            ) from exc
        if not isinstance(result, list) or not result:
            raise RuntimeError("invalid publication response")
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["validate", "apply", "status", "cancel"])
    parser.add_argument("--policies", type=Path, default=ROOT / "config" / "policies.yaml")
    parser.add_argument("--kong", type=Path, default=ROOT / "config" / "kong.yaml")
    parser.add_argument("--policy", help="operate on one referenced policy")
    parser.add_argument("--wait", action="store_true", help="resume preparation until active")
    args = parser.parse_args(argv)
    try:
        documents = read_configuration(args.policies, args.kong)
        if args.policy:
            documents = [doc for doc in documents if doc["id"] == args.policy]
            if not documents:
                raise ValueError("selected policy does not exist")
        bindings = read_bindings(args.kong)
        publishers = {
            (doc["namespace"], doc["id"]): Publisher(target_for(
                bindings[(doc["namespace"], doc["id"])]
            )) for doc in documents
        }
        failures = False
        for document in documents:
            publisher = publishers[(document["namespace"], document["id"])]
            # Validate every selected document before any selected mutation.
            result = publisher.execute(document, "validate")
            if result[0] != "valid":
                raise ValueError(f"{document['id']}: {' '.join(map(str, result))}")
        for document in documents:
            publisher = publishers[(document["namespace"], document["id"])]
            while True:
                result = publisher.execute(document, args.operation)
                print(json.dumps({"policy": document["id"], "status": result[0],
                                  "details": result[1:]}), flush=True)
                if result[0] == "error":
                    failures = True
                if not (args.wait and args.operation == "apply" and
                        result[0] in {"preparing", "busy"}):
                    break
                time.sleep(0.25)
        return 1 if failures else 0
    except (ValueError, TypeError, yaml.YAMLError, OSError, redis.RedisError,
            RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted; old policy remains usable; run status or resume apply",
              file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
