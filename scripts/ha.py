"""Demo Redis failover/maintenance, using only native Redis and Compose commands."""

import argparse
import json
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.yaml")]
MEMBERS = ("redis", "redis-b", "redis-c")
SENTINELS = ("sentinel-a", "sentinel-b", "sentinel-c")
PORTS = (8001, 8002, 8003)


def compose(*args, timeout=30, env=None):
    result = subprocess.run(
        COMPOSE + list(args), check=True, capture_output=True, text=True,
        timeout=timeout, env=env,
    )
    return result.stdout.strip()


def command(node, *args):
    port = 26379 if node in SENTINELS else 6379
    output = compose(
        "exec", "-T", node, "redis-cli", "-p", str(port), "--json",
        *map(str, args), timeout=3,
    )
    # redis-cli deliberately renders INFO as raw text, even with --json.
    if args and str(args[0]).upper() == "INFO":
        if not output.startswith("# "):
            raise RuntimeError(f"{node}: invalid INFO output")
        return output
    result = json.loads(output)
    if isinstance(result, dict) and "error" in result:
        raise RuntimeError(f"{node}: {result['error']}")
    return result


def info(node, section="replication"):
    content = command(node, "INFO", section)
    return dict(line.split(":", 1) for line in content.splitlines()
                if line and not line.startswith("#") and ":" in line)


def wait(description, condition, timeout=30):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = condition()
            if result:
                return result
        except (RuntimeError, ValueError, subprocess.SubprocessError) as error:
            last_error = error
        time.sleep(0.25)
    raise RuntimeError(f"{description} did not complete in {timeout}s: {last_error}")


class Topology:
    def __init__(self):
        self.unavailable = set()
        self.addresses = {}
        for node in MEMBERS:
            container = compose("ps", "-q", node)
            if not container or len(container.splitlines()) != 1:
                raise RuntimeError(f"expected one running {node}")
            output = subprocess.run(
                ["docker", "inspect", container], check=True,
                capture_output=True, text=True, timeout=3,
            )
            networks = json.loads(output.stdout)[0]["NetworkSettings"]["Networks"]
            self.addresses[node] = next(
                network["IPAddress"] for network in networks.values()
                if network["IPAddress"]
            )

    def primary(self):
        for sentinel in SENTINELS:
            if sentinel in self.unavailable:
                continue
            try:
                endpoint = command(sentinel, "SENTINEL", "get-master-addr-by-name", "limiter")
                node = next((name for name, address in self.addresses.items()
                             if endpoint and len(endpoint) == 2
                             and endpoint[0] == address and str(endpoint[1]) == "6379"), None)
                if node and node not in self.unavailable and command(node, "ROLE")[0] == "master":
                    return node
            except (RuntimeError, ValueError, subprocess.SubprocessError):
                continue
        raise RuntimeError("no reachable Sentinel-discovered primary")

    def healthy(self):
        primary = self.primary()
        if len(self.unavailable) != 0:
            return False
        offset = int(info(primary)["master_repl_offset"])
        for replica in MEMBERS:
            if replica == primary:
                continue
            state = info(replica)
            if (state.get("role") != "slave" or state.get("master_link_status") != "up"
                    or state.get("master_host") not in (primary, self.addresses[primary])
                    or state.get("master_port") != "6379"
                    or int(state.get("slave_repl_offset", -1)) < offset):
                return False
        for sentinel in SENTINELS:
            endpoint = command(sentinel, "SENTINEL", "get-master-addr-by-name", "limiter")
            if (not endpoint or endpoint[0] != self.addresses[primary]
                    or str(endpoint[1]) != "6379"):
                return False
            quorum = command(sentinel, "SENTINEL", "ckquorum", "limiter")
            if not isinstance(quorum, str) or not quorum.startswith("OK"):
                return False
        return primary

    def pause(self, node):
        self.unavailable.add(node)
        compose("pause", node)

    def restore(self):
        # Restore only members this invocation paused; never reset Redis roles.
        errors = []
        for node in tuple(self.unavailable):
            try:
                container = compose("ps", "-q", node)
                inspected = subprocess.run(
                    ["docker", "inspect", container], check=True,
                    capture_output=True, text=True, timeout=3,
                )
                if json.loads(inspected.stdout)[0]["State"]["Paused"]:
                    compose("unpause", node)
                self.unavailable.remove(node)
            except subprocess.SubprocessError as error:
                errors.append(f"{node}: {error}")
        if errors:
            raise RuntimeError("unpause failed: " + "; ".join(errors))
        # A resumed Sentinel can enter its native TILT interval after a long
        # process pause. Cleanup waits for convergence, not an arbitrary sleep.
        wait("Sentinel convergence and replica synchronization", self.healthy, timeout=60)
        compose("up", "-d", "--wait", "--no-recreate", *MEMBERS, *SENTINELS)


def response(port, path="/free", consumer=None):
    headers = {"apikey": consumer + "-demo-key"} if consumer else {}
    try:
        result = urlopen(Request(f"http://127.0.0.1:{port}{path}", headers=headers), timeout=2)
    except HTTPError as error:
        result = error
    with result:
        return result.status


class Traffic:
    def __init__(self):
        self.results = []
        self.stop = threading.Event()
        self.threads = []

    def __enter__(self):
        def run(port):
            while not self.stop.is_set():
                start = time.monotonic()
                try:
                    status = response(port)
                except (OSError, URLError):
                    status = "transport-error"
                self.results.append((port, start, time.monotonic() - start, status))
                self.stop.wait(0.05)

        for port in PORTS:
            thread = threading.Thread(target=run, args=(port,))
            thread.start()
            self.threads.append(thread)
        return self

    def __exit__(self, *args):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=3)

    def summary(self):
        results = list(self.results)
        counts = {}
        for _, _, _, status in results:
            counts[str(status)] = counts.get(str(status), 0) + 1
        if any(status not in (200, 503) for _, _, _, status in results):
            raise RuntimeError(f"unexpected continuous HTTP outcomes: {counts}")
        if not all(any(port == target and status == 200 for port, _, _, status in results)
                   for target in PORTS):
            raise RuntimeError("not all three gateways served successful traffic")
        return {"responses": counts, "max_latency_seconds":
                round(max((latency for _, _, latency, _ in results), default=0), 3)}


def publication(node, document, operation="apply"):
    directory = ROOT / "kong" / "plugins" / "distributed-rate-limit"
    script = "local core = (function()\n" + (directory / "core.lua").read_text()
    script += "\nend)()\n" + (directory / "publication.lua").read_text()
    return command(node, "EVAL", script, 1, "drl:demo:isolated-api:state",
                   operation, json.dumps(document, sort_keys=True, separators=(",", ":")))


def current_policy(node):
    revision = command(node, "HGET", "drl:demo:isolated-api:state", "active")
    document = json.loads(command(node, "GET", "drl:demo:isolated-api:snapshot:" + revision))
    for rule in document["rules"]:
        rule.pop("retain_seconds", None)
    return document


def restore_policy(topology, document):
    primary = topology.primary()
    result = publication(primary, document, "cancel")
    if result[0] != "cancelled":
        raise RuntimeError(f"cancel failed: {result}")

    def apply():
        result = publication(topology.primary(), document)
        if result[0] not in {"active", "busy", "preparing"}:
            raise RuntimeError(f"policy restore failed: {result}")
        return result[0] == "active"

    wait("original demo policy restoration", apply)


def verify():
    topology = Topology()
    original = None
    try:
        primary = wait("initial topology", topology.healthy)
        if any(response(port) != 200 for port in PORTS):
            raise RuntimeError("all gateways must be ready")
        original = current_policy(primary)
        target = json.loads(json.dumps(original))
        target["rules"][0]["window_seconds"] += 1
        time.sleep(0.11)
        prepared = publication(primary, target)
        if prepared[0] != "preparing":
            raise RuntimeError(f"preparation did not start: {prepared}")
        wait("preparation replication", topology.healthy)
        # Losing one Sentinel leaves two available to authorize promotion.
        topology.pause("sentinel-c")
        with Traffic() as traffic:
            time.sleep(0.25)
            switched_at = time.monotonic()
            topology.pause(primary)
            new_primary = wait(
                "automatic primary failover",
                lambda: (node if (node := topology.primary()) != primary else False),
            )
            wait("all gateways automatically reconnect",
                 lambda: all(response(port) == 200 for port in PORTS),
                 timeout=max(0.01, 30 - (time.monotonic() - switched_at)))
            recovery = round(time.monotonic() - switched_at, 3)
            if recovery > 30:
                raise RuntimeError(f"automatic HTTP recovery exceeded 30s: {recovery}")
            status = publication(new_primary, target, "status")
            if status[0] != "rewarming":
                raise RuntimeError(f"changed-primary status is unsafe: {status}")
            resumed = publication(new_primary, target)
            if resumed[0] != "preparing" or float(resumed[2]) < target["rules"][0]["window_seconds"] * 1000:
                raise RuntimeError(f"full new-window warmup did not restart: {resumed}")
            print(json.dumps({"automatic_failover": {"old": primary, "new": new_primary,
                              "recovery_seconds": recovery, **traffic.summary()}}), flush=True)
        topology.restore()
        wait("CLI discovery after failover", lambda: json.loads(compose(
            "run", "--rm", "--no-deps", "publisher", "python", "scripts/publish.py",
            "status", "--policy", "public-api", timeout=10
        ))["status"] == "active")
        # Entire datastore outage is different from a recoverable single failure.
        for node in MEMBERS:
            topology.pause(node)
        outcomes = [response(port) for port in PORTS]
        if outcomes != [503, 503, 503]:
            raise RuntimeError(f"full outage did not fail closed: {outcomes}")
        print(json.dumps({"full_outage": outcomes}), flush=True)
    finally:
        original_error = sys.exc_info()[1]
        try:
            topology.restore()
            if original:
                restore_policy(topology, original)
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as cleanup_error:
            if original_error is None:
                raise
            print(f"cleanup also failed: {cleanup_error}", file=sys.stderr)
    outcomes = [response(port) for port in PORTS]
    if outcomes != [200, 200, 200]:
        raise RuntimeError(f"post-outage recovery failed: {outcomes}")
    print(json.dumps({"recovered": outcomes, "primary": topology.primary()}), flush=True)


def controlled_handoff(topology, old_primary):
    if topology.primary() != old_primary:
        raise RuntimeError("primary changed before planned handoff")
    quorum = command("sentinel-a", "SENTINEL", "ckquorum", "limiter")
    if not isinstance(quorum, str) or not quorum.startswith("OK"):
        raise RuntimeError("Sentinel quorum is unavailable")
    paused, requested, selected = False, False, None
    pause_started = time.monotonic()
    deadline = pause_started + 8
    try:
        paused = True
        command(old_primary, "CLIENT", "PAUSE", 10000, "WRITE")
        offset = int(info(old_primary)["master_repl_offset"])
        wait("all eligible replicas caught up", lambda: all(
            int(info(replica).get("slave_repl_offset", -1)) >= offset
            for replica in MEMBERS if replica != old_primary
        ), timeout=min(5, max(0.01, deadline - time.monotonic())))
        requested = True
        command("sentinel-a", "SENTINEL", "failover", "limiter")

        def promoted():
            candidates = [node for node in MEMBERS if node != old_primary
                          and command(node, "ROLE")[0] == "master"]
            return candidates[0] if len(candidates) == 1 else False

        selected = wait("Sentinel-selected replica promotion", promoted,
                        timeout=max(0.01, deadline - time.monotonic()))
        # Sentinel uses MULTI/EXEC for reconfiguration, which WRITE pause blocks.
        # It still selects/promotes the new primary; this non-transactional admin
        # command safely demotes the old one before releasing queued admissions.
        command(old_primary, "REPLICAOF", topology.addresses[selected], 6379)
        wait("old primary demotion", lambda: command(old_primary, "ROLE")[0] == "slave",
             timeout=max(0.01, deadline - time.monotonic()))
        if time.monotonic() - pause_started >= 10:
            raise RuntimeError("write pause expired before a confirmed safe handoff")
        command(old_primary, "CLIENT", "UNPAUSE")
        paused = False
        wait("Sentinel convergence after handoff", lambda:
             topology.primary() == selected and topology.healthy())
        return selected
    finally:
        original_error = sys.exc_info()[1]
        if paused:
            try:
                role = command(old_primary, "ROLE")[0]
                if not requested or role == "slave":
                    command(old_primary, "CLIENT", "UNPAUSE")
                else:
                    # Do not deliberately reopen writes on an old primary after
                    # an uncertain promotion. The native pause is bounded.
                    print("handoff incomplete: inspect Redis roles; old-primary "
                          "write pause expires automatically after 10s", file=sys.stderr)
            except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as cleanup_error:
                if original_error is None:
                    raise
                print(f"cleanup also failed: {cleanup_error}", file=sys.stderr)


def declared_image(requested=None):
    config = json.loads(compose("config", "--format", "json"))
    images = {config["services"][node]["image"] for node in MEMBERS}
    if len(images) != 1:
        raise RuntimeError("declare the same desired image for all Redis members")
    image = images.pop()
    if requested is not None and requested != image:
        raise ValueError("--image must match the declared Compose image; "
                         "edit compose.yaml before controlled maintenance")
    return image


def maintain(requested=None):
    image = declared_image(requested)
    topology = Topology()
    primary = wait("initial topology", topology.healthy)
    marker = "drl:demo:maintenance:" + uuid.uuid4().hex
    value = uuid.uuid4().hex
    old_primary = primary

    def upgrade(node):
        compose("up", "-d", "--no-deps", "--force-recreate", node,
                timeout=60)
        wait(f"{node} rejoin", topology.healthy)
        print(json.dumps({"upgraded": node, "image": info(node, "server")["redis_version"]}), flush=True)

    try:
        with Traffic() as traffic:
            for replica in MEMBERS:
                if replica != primary:
                    upgrade(replica)
            command(primary, "SET", marker, value)
            new_primary = controlled_handoff(topology, primary)
            if command(new_primary, "GET", marker) != value:
                raise RuntimeError("synchronized pre-handoff marker was lost")
            upgrade(old_primary)
            wait("gateways recover from handoff", lambda: all(
                response(port) == 200 for port in PORTS
            ))
            print(json.dumps({"planned_handoff": {"old": old_primary, "new": new_primary,
                              **traffic.summary()}}), flush=True)
    finally:
        original_error = sys.exc_info()[1]
        try:
            command(topology.primary(), "DEL", marker)
        except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as cleanup_error:
            if original_error is None:
                raise
            print(f"cleanup also failed: {cleanup_error}", file=sys.stderr)
    wait("final topology", topology.healthy)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["verify", "maintain", "status"])
    parser.add_argument("--image", help="optional assertion of the declared Redis image")
    args = parser.parse_args()
    if args.operation == "verify":
        verify()
    elif args.operation == "maintain":
        maintain(args.image)
    else:
        topology = Topology()
        print(json.dumps({"primary": wait("topology", topology.healthy)}))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as error:
        raise SystemExit(f"error: {error}")
