#!/usr/bin/env bash
# pathtrace.sh — control-plane path trace across the FRR MPLS L3VPN lab.
# Walks each node's FIB/LFIB (vtysh JSON) hop by hop, following IP next-hops and
# MPLS label push/swap/pop, printing the label stack at every hop. Works where IP
# traceroute can't: the Linux MPLS core emits no ICMP for TTL-expired labels, so
# transit LSRs are invisible to traceroute — but the forwarding tables aren't.
#
# Usage:  pathtrace.sh <start-node> <dst-ip>      e.g.  pathtrace.sh ce1 10.255.1.4
# Run inside the my-frr VM.
set -uo pipefail

NODES="pe1 pe2 p1 p2 ce1 ce2 ce3 ce4"
START=${1:-ce1}
DST=${2:-10.255.1.4}
MAXHOPS=24

# ---- ip -> node map (every local v4 addr, incl. loopbacks & link IPs) --------
declare -A IP2NODE
for n in $NODES; do
  while read -r ip; do [ -n "$ip" ] && IP2NODE[$ip]=$n; done \
    < <(docker exec "$n" ip -o -4 addr show 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | grep -v '^127\.')
done

vrf_of() { # node ip -> vrf name of the iface owning ip (or "default")
  local iface
  iface=$(docker exec "$1" ip -o -4 addr show 2>/dev/null | awk -v ip="$2" 'index($4, ip"/")==1{print $2; exit}')
  [ -z "$iface" ] && { echo default; return; }
  docker exec "$1" ip -o link show "$iface" 2>/dev/null | grep -oE 'master [^ ]+' | awk '{print $2}' | head -1 | grep -q . \
    && docker exec "$1" ip -o link show "$iface" 2>/dev/null | grep -oE 'master [^ ]+' | awk '{print $2; exit}' \
    || echo default
}

route_lookup() { # node vrf dst -> "ip|iface|labelsCSV"
  local va=""; [ -n "$2" ] && [ "$2" != "default" ] && va="vrf $2"
  docker exec "$1" vtysh -c "show ip route $va $3 json" 2>/dev/null \
    | jq -r '.[][].nexthops[] | select(.fib==true) | "\(.ip)|\(.interfaceName)|\((.labels // [])|join(","))"' | head -1
}

mpls_lookup() { # node label -> "outLabel|nexthop|interface"
  docker exec "$1" vtysh -c "show mpls table json" 2>/dev/null \
    | jq -r --arg l "$2" 'if .[$l] then (.[$l].nexthops[0] | "\(.outLabel)|\(.nexthop // "")|\(.interface // "")") else "" end'
}

echo "==== control-plane path trace:  $START -> $DST ===="
node=$START; vrf=default; stack=""; seq="$START"

for ((h=0; h<MAXHOPS; h++)); do
  # reached the node that owns DST, and no labels left => done
  if [ "${IP2NODE[$DST]:-}" = "$node" ] && [ -z "$stack" ]; then
    printf "  %-4s  %-9s  destination %s reached\n" "$node" "[dest]" "$DST"
    break
  fi

  if [ -z "$stack" ]; then
    # -------- IP phase --------
    res=$(route_lookup "$node" "$vrf" "$DST")
    [ -z "$res" ] && { printf "  %-4s  no route to %s in vrf %s\n" "$node" "$DST" "$vrf"; break; }
    IFS='|' read -r nh iface labels <<<"$res"
    if [ -n "$labels" ]; then
      stack=$(echo "$labels" | tr ',' ' ')
      printf "  %-4s  IP/%-6s push[%s]  via %-8s -> %s\n" "$node" "$vrf" "$labels" "$iface" "$nh"
    else
      printf "  %-4s  IP/%-6s            via %-8s -> %s\n" "$node" "$vrf" "$iface" "$nh"
    fi
    nextnode=${IP2NODE[$nh]:-?}
    vrf=$(vrf_of "$nextnode" "$nh")
    node=$nextnode; seq+=" -> $node"
  else
    # -------- MPLS phase --------
    top=${stack%% *}
    res=$(mpls_lookup "$node" "$top")
    [ -z "$res" ] && { printf "  %-4s  no LFIB entry for label %s\n" "$node" "$top"; break; }
    IFS='|' read -r out nh iface <<<"$res"
    if [ -z "$nh" ]; then
      # egress: no IP next-hop => pop VPN label into a VRF (iface holds vrf name)
      vrf=$iface; stack=""
      printf "  %-4s  MPLS      pop %-4s -> VRF %s\n" "$node" "$top" "$vrf"
      continue
    fi
    rest=${stack#* }; [ "$rest" = "$stack" ] && rest=""
    nextnode=${IP2NODE[$nh]:-?}
    if [ "$out" = "3" ]; then
      stack=$rest
      printf "  %-4s  MPLS      pop %-4s (PHP)  via %-8s -> %s   stack[%s]\n" "$node" "$top" "$iface" "$nh" "$stack"
    else
      stack="$out${rest:+ $rest}"
      printf "  %-4s  MPLS      swap %s->%-4s via %-8s -> %s   stack[%s]\n" "$node" "$top" "$out" "$iface" "$nh" "$stack"
    fi
    node=$nextnode; seq+=" -> $node"
  fi
done

echo "---------------------------------------------------------------"
echo "  path:  $seq"
