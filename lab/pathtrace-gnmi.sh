#!/usr/bin/env bash
# pathtrace-gnmi.sh — control-plane path trace sourced ENTIRELY from the shim's
# gNMI, with no device login. Same hop-by-hop label-stack walk as pathtrace.sh,
# but every FIB/LFIB read is a gNMI Get against the node's management address —
# exactly how you'd trace across cEOS. It reads three shim exports:
#   openconfig:/network-instances/network-instance[name=*]/afts/ipv4-unicast/
#              ipv4-entry[prefix=*]/state/{next-hop,pushed-mpls-label-stack}
#   openconfig:/interfaces/interface[name=*]/subinterfaces/subinterface[index=0]/
#              ipv4/addresses/address[ip=*]/state/ip           (builds ip -> node)
#   frr:/mpls/lfib/entry[label=N]/state/{out-label,next-hop,interface}
#
# The VRF search is done server-side by the network-instance[name=*] wildcard, so
# the tracer never needs to know which VRF a prefix lives in. Longest-prefix match
# across the returned entries is done here (gNMI keys are exact-match only).
#
# Usage:  pathtrace-gnmi.sh <start-node> <dst-ip>   e.g.  pathtrace-gnmi.sh ce1 10.255.1.4
# Run inside the my-frr VM (needs the native `gnmic` and reachability to 172.31.0.0/24).
set -uo pipefail

# ---- inventory: node -> management address (an NMS already has this) ---------
declare -A MGMT=(
  [pe1]=172.31.0.11  [pe2]=172.31.0.12  [p1]=172.31.0.21   [p2]=172.31.0.22
  [ce1]=172.31.0.101 [ce2]=172.31.0.102 [ce3]=172.31.0.103 [ce4]=172.31.0.104
)
START=${1:-ce1}
DST=${2:-10.255.1.4}
MAXHOPS=24

GET() { # GET <node> <path>  -> flat "path: value" lines
  gnmic -a "${MGMT[$1]}:9339" --insecure --timeout 5s get --path "$2" --format flat 2>/dev/null
}
ip2int() { local a b c d; IFS=. read -r a b c d <<<"$1"; echo $(( (a<<24)|(b<<16)|(c<<8)|d )); }

[ -n "${MGMT[$START]:-}" ] || { echo "unknown start node: $START"; exit 1; }

# ---- ip -> node, built from each node's exported interface addresses ----------
declare -A IP2NODE
for n in "${!MGMT[@]}"; do
  while IFS= read -r line; do
    ip=${line#*address[ip=}; ip=${ip%%]*}
    [ -n "$ip" ] && [[ $ip != 127.* ]] && [[ $ip != 172.31.* ]] && IP2NODE[$ip]=$n
  done < <(GET "$n" "openconfig:/interfaces/interface[name=*]/subinterfaces/subinterface[index=0]/ipv4/addresses/address[ip=*]/state/ip")
done

# route_lookup <node> <dst> -> "vrf|prefix|nexthop|labelsCSV"  (LPM, wildcard vrf)
route_lookup() {
  local node=$1 dst=$2 di best=-1 out="" key val vrf pfx leaf
  local -A NH LB VRF
  di=$(ip2int "$dst")
  while IFS= read -r line; do
    key=${line%%: *}; val=${line#*: }
    vrf=${key#*network-instance[name=}; vrf=${vrf%%]*}
    pfx=${key#*ipv4-entry[prefix=}; pfx=${pfx%%]*}
    leaf=${key##*/}
    case $leaf in
      next-hop)                NH[$pfx]=$val; VRF[$pfx]=$vrf ;;
      pushed-mpls-label-stack) LB[$pfx]=$val ;;
    esac
  done < <(GET "$node" "openconfig:/network-instances/network-instance[name=*]/afts/ipv4-unicast/ipv4-entry[prefix=*]/state")
  for pfx in "${!NH[@]}"; do
    local net len ni mask
    net=${pfx%/*}; len=${pfx#*/}; ni=$(ip2int "$net")
    if [ "$len" -eq 0 ]; then mask=0; else mask=$(( (0xFFFFFFFF << (32-len)) & 0xFFFFFFFF )); fi
    if (( (di & mask) == (ni & mask) )) && (( len > best )); then
      best=$len; out="${VRF[$pfx]}|$pfx|${NH[$pfx]}|${LB[$pfx]:-}"
    fi
  done
  echo "$out"
}

# mpls_lookup <node> <label> -> "outLabelCSV|nexthop|interface"
mpls_lookup() {
  local node=$1 label=$2 out="" nh="" iface="" key val leaf
  while IFS= read -r line; do
    key=${line%%: *}; val=${line#*: }; leaf=${key##*/}
    case $leaf in
      out-label) out=$val ;;
      next-hop)  nh=$val ;;
      interface) iface=$val ;;
    esac
  done < <(GET "$node" "frr:/mpls/lfib/entry[label=$label]/state")
  echo "$out|$nh|$iface"
}

echo "==== gNMI path trace:  $START -> $DST  (source: shim gNMI, no device login) ===="
node=$START; stack=""; seq="$START"

for ((h=0; h<MAXHOPS; h++)); do
  if [ "${IP2NODE[$DST]:-}" = "$node" ] && [ -z "$stack" ]; then
    printf "  %-4s  %-12s destination %s reached\n" "$node" "[dest]" "$DST"; break
  fi

  if [ -z "$stack" ]; then
    # -------- IP phase --------
    res=$(route_lookup "$node" "$DST")
    [ -z "$res" ] && { printf "  %-4s  no route to %s\n" "$node" "$DST"; break; }
    IFS='|' read -r vrf pfx nh labels <<<"$res"
    if [ -n "$labels" ]; then
      stack=$(echo "$labels" | tr ',' ' ')
      printf "  %-4s  IP/%-6s push[%s]  -> %s  (%s)\n" "$node" "$vrf" "$labels" "$nh" "$pfx"
    else
      printf "  %-4s  IP/%-6s            -> %s  (%s)\n" "$node" "$vrf" "$nh" "$pfx"
    fi
    node=${IP2NODE[$nh]:-?}; seq+=" -> $node"
  else
    # -------- MPLS phase --------
    top=${stack%% *}
    IFS='|' read -r out nh iface <<<"$(mpls_lookup "$node" "$top")"
    [ -z "$out$nh$iface" ] && { printf "  %-4s  no LFIB entry for label %s\n" "$node" "$top"; break; }
    rest=${stack#* }; [ "$rest" = "$stack" ] && rest=""
    if [ -n "$out" ]; then
      # swap top label (out may be a CSV stack)
      out=$(echo "$out" | tr ',' ' ')
      stack="$out${rest:+ $rest}"
      printf "  %-4s  MPLS      swap %s->%-4s -> %s  (%s)   stack[%s]\n" "$node" "$top" "${out// /,}" "$nh" "$iface" "$stack"
      node=${IP2NODE[$nh]:-?}; seq+=" -> $node"
    elif [ -n "$nh" ]; then
      # penultimate-hop pop (implicit-null), forward on
      stack=$rest
      printf "  %-4s  MPLS      pop %-5s(PHP) -> %s  (%s)   stack[%s]\n" "$node" "$top" "$nh" "$iface" "$stack"
      node=${IP2NODE[$nh]:-?}; seq+=" -> $node"
    else
      # egress pop into a VRF (interface holds the VRF name); stay, resume IP phase
      stack=""
      printf "  %-4s  MPLS      pop %-5s-> VRF %s\n" "$node" "$top" "$iface"
    fi
  fi
done

echo "---------------------------------------------------------------"
echo "  path:  $seq"
