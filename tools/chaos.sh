#!/usr/bin/env bash
#
# Chaos shortcuts for the kafka toxiproxy. Talks to the toxiproxy HTTP API
# at localhost:8474 (the toxiproxy container in docker-compose.yml).
#
#   ./tools/chaos.sh status                    # list active toxics
#   ./tools/chaos.sh latency 2000              # add 2s latency on every byte
#   ./tools/chaos.sh slow_close 500            # delay close by 500ms
#   ./tools/chaos.sh down                      # disable proxy entirely (drop all traffic)
#   ./tools/chaos.sh up                        # re-enable proxy
#   ./tools/chaos.sh bandwidth 1024            # throttle to 1 KB/s
#   ./tools/chaos.sh clear                     # remove all toxics
#
# Requires: docker compose stack running with toxiproxy on :8474.

set -euo pipefail

API="http://localhost:8474"
PROXY="kafka"

cmd=${1:-status}
shift || true

case "$cmd" in
  status)
    curl -s "$API/proxies/$PROXY" | jq '{enabled, listen, upstream, toxics}'
    ;;

  latency)
    ms=${1:?usage: ./tools/chaos.sh latency <ms>}
    curl -s -X POST "$API/proxies/$PROXY/toxics" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"latency_$ms\",\"type\":\"latency\",\"attributes\":{\"latency\":$ms}}" | jq
    ;;

  slow_close)
    ms=${1:?usage: ./tools/chaos.sh slow_close <ms>}
    curl -s -X POST "$API/proxies/$PROXY/toxics" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"slow_close_$ms\",\"type\":\"slow_close\",\"attributes\":{\"delay\":$ms}}" | jq
    ;;

  bandwidth)
    rate=${1:?usage: ./tools/chaos.sh bandwidth <KB/s>}
    curl -s -X POST "$API/proxies/$PROXY/toxics" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"bw_$rate\",\"type\":\"bandwidth\",\"attributes\":{\"rate\":$rate}}" | jq
    ;;

  down)
    curl -s -X POST "$API/proxies/$PROXY" \
      -H 'Content-Type: application/json' \
      -d '{"enabled":false}' | jq
    ;;

  up)
    curl -s -X POST "$API/proxies/$PROXY" \
      -H 'Content-Type: application/json' \
      -d '{"enabled":true}' | jq
    ;;

  clear)
    toxics=$(curl -s "$API/proxies/$PROXY/toxics" | jq -r '.[].name')
    for t in $toxics; do
      curl -s -X DELETE "$API/proxies/$PROXY/toxics/$t" >/dev/null
      echo "removed: $t"
    done
    curl -s -X POST "$API/proxies/$PROXY" \
      -H 'Content-Type: application/json' \
      -d '{"enabled":true}' >/dev/null
    echo "proxy enabled, all toxics cleared"
    ;;

  *)
    echo "unknown command: $cmd"
    echo "see header of $0 for usage"
    exit 2
    ;;
esac
