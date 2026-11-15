#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

restore=0
finish() {
  result=$?
  trap - EXIT
  if [ "$restore" -eq 1 ]; then
    if python3 scripts/ha.py status &&
       docker compose run --rm --no-deps publisher python scripts/publish.py cancel &&
       docker compose run --rm --no-deps publisher python scripts/publish.py apply --wait; then
      :
    else
      echo "error: restoring demo policies failed; run cancel and apply --wait" >&2
      if [ "$result" -eq 0 ]; then result=1; fi
    fi
  fi
  exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker compose up -d --wait redis redis-b redis-c sentinel-a sentinel-b sentinel-c
docker compose run --build --rm tests python -m unittest \
  tests.test_core tests.test_accounting tests.test_publication tests.test_cli \
  tests.test_connection tests.test_primary tests.test_ha_tools -v
docker compose --profile integration up --build -d --wait
restore=1
docker compose run --rm \
  -e REDIS_DATABASE=0 \
  -e KONG_URLS=http://kong-a:8000,http://kong-b:8000,http://kong-c:8000 \
  -e PROXY_URL=http://proxy:8080 \
  tests python -m unittest tests.test_http -v
python3 scripts/outage.py
python3 scripts/ha.py maintain
