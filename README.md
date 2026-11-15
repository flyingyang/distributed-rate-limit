# Distributed rate limiting for Kong

A readable Kong take-home implementation: three gateways share exact rolling
HTTP request quotas through one Redis primary. No separate limiter service.
The Redis HA extension uses two replicas and three Sentinels. Kong and the
publisher discover the current primary automatically; the limiter is independent
of the ingress product. Automatic failover and controlled patch maintenance have
been exercised with continuous traffic through the real gateways.

```text
Kong authentication -> adapter/core -> Redis revision gate + all-rule admission
                                    -> upstream, or 403 / 429 / 503
Policy YAML -> Python CLI -> immutable snapshot -> preparation -> activation
Sentinel -> current primary discovery -> Kong / publisher reconnect
```

## Reproduce

Prerequisites: Linux containers, Docker Engine and Docker Compose with `up --wait`
support, internet access for pinned images, and free localhost ports 8001-8003.
The full verification script also needs host Python 3.8+ (standard library only).
On Windows, development used Ubuntu WSL2 with Docker Engine inside WSL,
not Docker Desktop.

From a Linux shell in this repository. For Redis image upgrades, follow
[the maintenance procedure](#redis-availability-and-maintenance) before running up:

```sh
docker compose --profile integration up --build -d --wait
curl -i -H 'apikey: alice-demo-key' http://localhost:8001/orders
curl -i -H 'apikey: alice-demo-key' http://localhost:8002/orders/123
curl -i -H 'apikey: alice-demo-key' http://localhost:8003/orders?page=2
```

With otherwise unused demo quotas, four Alice order requests are admitted per
rolling three seconds. The next returns 429 with integer `Retry-After`.
Alice/Bob/Carol/Dave demonstration credentials are
`<username>-demo-key`. They are **public demo credentials**, not production secrets.
All three gateways use the same explicit Consumer UUIDs and policy bindings.

PowerShell equivalent for startup:

```powershell
wsl -d Ubuntu --cd 'D:\code\distributed-rate-limit' --exec sudo -n docker compose --profile integration up --build -d --wait
curl.exe -i -H "apikey: alice-demo-key" http://localhost:8001/orders
```

Change the checkout path as needed. If WSL stops, start it and rerun Compose up.
Redis data, Sentinel configuration and rewritten roles persist across recreation;
member names are not permanent primary roles. `docker compose down` stops this
project; `down -v` deliberately discards history and election state.
Redis/Sentinel addresses are internal, without NAT port remapping. If the demo's
`172.30.85.0/24` network overlaps your environment, update the subnet, member
addresses and trusted proxy address together. Single-host Compose does not
provide independent-host or availability-zone redundancy.

## Rules and updates

`config/kong.yaml` owns routing, authentication, bindings and Redis connections.
`config/policies.yaml` is the only operator-authored limit/window/rule source.
Every policy must be referenced exactly once in the deployment document; that
document is replicated across gateways. Separate bindings must use distinct IDs.

| Dimension | Pool identity | Matching |
| --- | --- | --- |
| `path` | Configured path, shared by all matching requests | Required `exact` or segment-boundary `prefix` |
| `consumer` | Authenticated Kong Consumer ID | Whole binding or optional explicit path filter |
| `ip` | Kong's trusted forwarded-IP result | Whole binding |

All applicable ceilings must allow before any quota is consumed. Queries are
excluded; `/orders/123` shares the `/orders` prefix pool, `/orders-backup` does not.
See [identity and path contracts](docs/design.md#4-rules-identity-and-configuration-isolation)
for overlap, trailing-slash, canonical-path and trusted-IP details.

Edit limits/windows in the policy YAML, then:

```sh
docker compose run --rm --no-deps publisher python scripts/publish.py validate
docker compose run --rm --no-deps publisher python scripts/publish.py apply --wait
docker compose run --rm --no-deps publisher python scripts/publish.py status
```

Use `--policy public-api` to select one binding, or `cancel` to stop a pending
preparation. When warmup is required, apply without `--wait` reports `preparing`
and exits; run the same apply later to resume. Redis replies are printed as JSON
lines with `valid`, `active`, `preparing`, `rewarming`, `busy`, `different`,
`missing`, `cancelled` or `error` status. Exceptions are reported on stderr;
errors exit nonzero.
`busy` means a confirmed nonmutating 100ms publication throttle, not activation.
An identical apply is idempotent. Validation requires reachable Redis but does
not mutate it.

Lowering limits retains all consumption and can immediately cause legitimate 429.
Shortening windows activates with existing history. Increasing a window prepares
longer retention while the old limit/window continues serving, then conservatively
waits a full new window before activation. Interrupted CLI preparation is resumable;
there is no background controller. Canceling leaves old rules and enlarged
retention in place. A Redis incarnation change makes status report `rewarming`;
resume restarts a full preparation window rather than trusting the old deadline.

Rule membership, dimensions, paths, namespace, binding and authentication/scope
changes are **not lossless live migrations**. Structural policy updates are rejected.
Cross-binding publication is not one global transaction: validation runs first,
but successful bindings can activate before another binding fails.

The CLI also runs with Python 3.12 and `pip install -r requirements.txt`, provided
the Sentinel-announced addresses are reachable. It reads connection settings from
each referenced binding, not a separately maintained primary address.

For standalone Redis, explicitly choose `redis_mode: direct`; `REDIS_URL` is
available as a direct-mode CLI/test override. Using it with Sentinel bindings is
an error, not a hidden fallback. Configure separate Sentinel/Redis credentials
and TLS fields for protected services; never commit secrets.

## Redis availability and maintenance

Sentinel elects replacements; Kong and the publisher discover the primary and
reconnect without IP edits or gateway restarts. The plugin owns primary-only
access and safe, bounded recovery, not Redis election/replication.

```sh
python3 scripts/ha.py status
python3 scripts/outage.py
python3 scripts/ha.py maintain
```

These host commands need Docker permissions (use `sudo -n python3 ...` in the
described WSL installation). The outage check pauses one Sentinel and the current
primary, verifies automatic recovery/rewarmup/rejoin, then tests full-store outage.
These commands operate only on the demo Compose project and its demo state.

Maintenance replaces replicas individually, catches them up during a write pause,
lets Sentinel promote, confirms old-primary demotion before unpausing, then
replaces the old member. See [the native handoff design](docs/design.md#7-deployment-configuration-and-implementation).
Do not run a publisher concurrently. Roles and volumes are retained; the desired
Redis image is declared in Compose, currently 7.4.3.
For a future patch, edit the shared Redis image in `compose.yaml`, then run
maintenance **before ordinary up or check.sh**; do not upgrade all members at once.
Optional `--image` is an assertion that must match Compose, not a temporary override.
An incomplete handoff reports failure rather than intentionally reopening writes
on an old primary after uncertain promotion; its native 10s pause is bounded.
Inspect roles before resuming maintenance.

## Verification

Run the complete reproducible suite from a Linux shell:

```sh
sh scripts/check.sh
```

PowerShell + this WSL installation:

```powershell
wsl -d Ubuntu --cd 'D:\code\distributed-rate-limit' --exec sudo -n sh scripts/check.sh
```

This runs the shared Lua core against **real Redis**, concurrent accounting and
publication tests, actual HTTP requests through **three Kong instances**, a
controlled trusted proxy, automatic Redis failover and controlled Redis member
replacement using the declared image. Core tests use isolated random namespaces
in Redis DB 15. HTTP tests deliberately reset only the `demo`
namespace in DB 0; do not point them at a production namespace. The script restores
the example limits afterward, preserving the final test history.

Individual suites, after starting the stack:

```sh
docker compose run --rm tests python -m unittest tests.test_core tests.test_accounting tests.test_publication tests.test_cli tests.test_connection tests.test_primary tests.test_ha_tools -v
docker compose run --rm -e REDIS_DATABASE=0 -e KONG_URLS=http://kong-a:8000,http://kong-b:8000,http://kong-c:8000 -e PROXY_URL=http://proxy:8080 tests python -m unittest tests.test_http -v
python3 scripts/outage.py
```

The latest complete run on 2026-11-15 exited 0: **29 core/Redis/CLI/tool tests and
10 HTTP tests passed**, along with automatic failover, full-store recovery and
controlled replacement. All four demo policies were restored active.
HA traffic uses `/free` (revision checks, no matching quota rules); it verifies
recovery, not fault-time quota accuracy. Detailed results, the real patch-upgrade
evidence and unverified boundaries are in [design section 8](docs/design.md#8-verification).

## Repository map

```text
kong/plugins/distributed-rate-limit/  # Kong adapter, independent core, Redis scripts
scripts/                             # Publication CLI/connection helper; demo tooling
config/                              # Kong bindings, policies, controlled test proxy
tests/                               # Real Redis/HTTP suites and focused mock tests
docs/design.md                       # Contracts, trade-offs, evidence, future work
compose.yaml, Dockerfile.*            # Local demo topology and container packaging
AGENTS.md                            # Contributor instructions
```

Redis bootstrap, outage, maintenance and verification scripts are demo tooling,
not a production rollout controller. The original assignment PDF is retained.

## Important boundaries

Admission counts permission to forward, not upstream success. Errors, timeouts,
disconnects and retries do not refund consumption. Demo key-auth runs before the
limiter. Missing required Consumer returns 403 without charging; path/IP-only rules
do not require one. An untrusted `X-Forwarded-For` never creates a new IP identity.
Only the demo's controlled proxy is trusted, and it discards supplied headers.

This take-home remains fail-closed: unavailable Redis/discovery, missing or damaged
policy, or unconfirmed admission return 503 and affect the business data plane.
A 503 may follow recorded consumption without forwarding; admission is not billing.
Confirmed exhaustion returns 429 with `Retry-After`, not a reservation.
HA does not make each switching-time request transparent. Asynchronous replication
can lose acknowledged counts/policy revisions, and ROLE checks cannot fence an
isolated former primary; there is no unconditional fault-time quota guarantee.

**Future work, not implemented:** production gateways may choose availability-first
storage-outage policies, bounded waiting, or time-/quota-bounded local fallback with
observability, accepting possible shared-limit overruns during degradation.
Do not indiscriminately fail-open: missing/damaged configuration or policy and
identity failures need separate handling; confirmed exhaustion remains 429.
Admission deduplication alone cannot recover writes lost through Sentinel replication.

Kong rollout and ingress replacement are outside this submission. Three instances
sharing Redis do not prove lossless rollout: readiness, traffic withdrawal and
in-flight draining belong to the deployment platform.

Pinned demo: Kong 3.9.1, Redis 7.4.3, Sentinel 7.4.2, Python 3.12.10, nginx 1.27.5.
This is a bounded take-home, **not production-ready HA infrastructure**.
Redis persistence and `noeviction` do not guarantee durability. See the design
for clock, memory, availability, security, scale and migration trade-offs.
