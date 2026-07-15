#!/usr/bin/env bash
# build-topo.sh — PHASE 1: connectivity only (no routing protocols yet).
# 8-node MPLS L3VPN topology, data plane = veth point-to-point (真·直连),
# management plane = dedicated docker network frr-mgmt (out-of-band, telemetry).
#
#   ce1                                   ce3
#      \                                 /
#       pe1 ── p1 ── p2 ── pe2
#      /                        \
#   ce2                          ce4
#
#   core  (veth): pe1-p1, p1-p2, p2-pe2
#   access(veth): pe1-ce1, pe1-ce2, pe2-ce3, pe2-ce4
#   mgmt  (docker frr-mgmt 172.31.0.0/24): all 8 nodes' eth0
#
# Interface names are deterministic: on node A facing B the iface is "A-B".
# Data links are /30. Loopbacks are /32. Run inside the my-frr VM.
# Protocols (OSPF/LDP/iBGP-VPNv4/VRF/eBGP) come in config-l3vpn.sh.
set -euo pipefail

IMG=router:v1
NODES="pe1 pe2 p1 p2 ce1 ce2 ce3 ce4"
MGMT_NET=frr-mgmt
MGMT_SUBNET=172.31.0.0/24
MGMT_GW=172.31.0.254

declare -A MGMT=(
  [pe1]=172.31.0.11 [pe2]=172.31.0.12 [p1]=172.31.0.21 [p2]=172.31.0.22
  [ce1]=172.31.0.101 [ce2]=172.31.0.102 [ce3]=172.31.0.103 [ce4]=172.31.0.104
)
declare -A LO=(
  [pe1]=10.255.0.1 [pe2]=10.255.0.2 [p1]=10.255.0.11 [p2]=10.255.0.12
  [ce1]=10.255.1.1 [ce2]=10.255.1.2 [ce3]=10.255.1.3 [ce4]=10.255.1.4
)
# data-plane veth links: "A B A-ip B-ip"  (ifname A side = A-B, B side = B-A, /30)
LINKS=(
  "pe1 p1  10.0.12.1 10.0.12.2"   # core
  "p1  p2  10.0.13.1 10.0.13.2"   # core
  "p2  pe2 10.0.14.1 10.0.14.2"   # core
  "pe1 ce1 10.0.21.1 10.0.21.2"   # access
  "pe1 ce2 10.0.22.1 10.0.22.2"   # access
  "pe2 ce3 10.0.23.1 10.0.23.2"   # access
  "pe2 ce4 10.0.24.1 10.0.24.2"   # access
)

echo "== teardown any prior run =="
for n in $NODES; do docker rm -f "$n" >/dev/null 2>&1 || true; done
# old 5-node data nets (harmless if absent)
for net in l_ce1pe1 l_pe1p1 l_p1pe2 l_pe2ce2; do docker network rm "$net" >/dev/null 2>&1 || true; done
docker network rm "$MGMT_NET" >/dev/null 2>&1 || true

echo "== create management network $MGMT_NET ($MGMT_SUBNET) =="
docker network create --subnet "$MGMT_SUBNET" --gateway "$MGMT_GW" "$MGMT_NET" >/dev/null

echo "== launch 8 containers (eth0 = mgmt only; data links added as veth) =="
for n in $NODES; do
  docker run -d --name "$n" --hostname "$n" --privileged \
    --network "$MGMT_NET" --ip "${MGMT[$n]}" "$IMG" \
    sh -c "sysctl -w net.mpls.platform_labels=100000 >/dev/null 2>&1; sleep infinity" >/dev/null
  echo "   $n  mgmt=${MGMT[$n]}"
done

echo "== wire data plane with veth point-to-point links =="
veth() { # A B Aip Bip  -> veth pair, ifname A-B / B-A, /30
  local a=$1 b=$2 aip=$3 bip=$4 aif bif pa pb
  aif="${a}-${b}"; bif="${b}-${a}"
  pa=$(docker inspect -f '{{.State.Pid}}' "$a")
  pb=$(docker inspect -f '{{.State.Pid}}' "$b")
  sudo ip link add "$aif" type veth peer name "$bif"
  sudo ip link set "$aif" netns "$pa"
  sudo ip link set "$bif" netns "$pb"
  docker exec "$a" ip addr add "$aip/30" dev "$aif"; docker exec "$a" ip link set "$aif" up
  docker exec "$b" ip addr add "$bip/30" dev "$bif"; docker exec "$b" ip link set "$bif" up
  echo "   $aif ($aip) <==> $bif ($bip)"
}
for l in "${LINKS[@]}"; do veth $l; done

echo "== loopbacks (/32) =="
for n in $NODES; do
  docker exec "$n" ip addr add "${LO[$n]}/32" dev lo 2>/dev/null || true
  echo "   $n  lo=${LO[$n]}"
done

echo
echo "== connectivity check: ping across every direct link (/30) =="
ok=0; bad=0
for l in "${LINKS[@]}"; do
  set -- $l; a=$1; b=$2; bip=$4
  if docker exec "$a" ping -c1 -W1 "$bip" >/dev/null 2>&1; then
    echo "   OK   $a -> $b ($bip)"; ok=$((ok+1))
  else
    echo "   FAIL $a -> $b ($bip)"; bad=$((bad+1))
  fi
done
echo
echo "== phase 1 done: $ok link(s) up, $bad failed. Next: config-l3vpn.sh =="
