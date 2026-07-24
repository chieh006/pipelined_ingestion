#!/usr/bin/env bash
# Inject one-way network latency on the interface carrying benchmark traffic
# (parent §4). A per-direction delay of d gives RTT ~= 2d; the nominal value is
# never trusted as truth -- verify it with `rgw_ingest_bench rtt-probe`.
#
#   scripts/netem.sh set 1ms   # add delay
#   scripts/netem.sh clear     # remove the qdisc
#   scripts/netem.sh show      # print the current qdisc
#
# Requires root (tc), so it is invoked via sudo from the Makefile. Linux-only by
# nature -- tc does not exist on Windows/macOS, which is why this is a shell
# script kept out of the (cross-platform) Python package.
#
# NETEM_IFACE overrides interface discovery (e.g. NETEM_IFACE=lo for the
# loopback case); otherwise the docker bridge / default-route interface is used
# so only benchmark traffic is delayed (the §14b container argument).
set -euo pipefail

resolve_iface() {
  if [[ -n "${NETEM_IFACE:-}" ]]; then
    printf '%s\n' "${NETEM_IFACE}"
  elif ip link show docker0 >/dev/null 2>&1; then
    printf 'docker0\n'
  else
    ip route show default | awk '{print $5; exit}'
  fi
}

usage() {
  echo "usage: $0 {set <delay>|clear|show}" >&2
  exit 2
}

main() {
  local action="${1:-}"
  local iface
  iface="$(resolve_iface)"

  case "${action}" in
    set)
      [[ $# -eq 2 ]] || usage
      tc qdisc replace dev "${iface}" root netem delay "$2"
      echo "netem: dev ${iface} delay $2"
      ;;
    clear)
      tc qdisc del dev "${iface}" root 2>/dev/null || true
      echo "netem: cleared dev ${iface}"
      ;;
    show)
      tc qdisc show dev "${iface}"
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"
