#!/usr/bin/env bash
# deploy-shim.sh — build the frr-visible shim, deploy it EMBEDDED into all 8
# FRR containers, and point each FRR's FPM/BMP/OSPF-syslog at the local shim.
# Also installs lldpd (LLDP metric) and a bridge+FDB on ce1 (VLAN/FDB metric).
# Run inside the my-frr VM, after build-topo.sh + config-l3vpn.sh.
set -euo pipefail

NODES="pe1 pe2 p1 p2 ce1 ce2 ce3 ce4"
OSPF_NODES="pe1 p1 p2 pe2"
# p1/p2 run no bgpd; PEs are AS65000, each CE its own AS
declare -A ASN=( [pe1]=65000 [pe2]=65000 [ce1]=65101 [ce2]=65102 [ce3]=65103 [ce4]=65104 )

echo "== build shim (static) =="
cd /Users/fanwei/arista/frr-visible
CGO_ENABLED=0 GOFLAGS=-buildvcs=false go build -o /tmp/frr-visible ./cmd/frr-visible
echo "   $(ls -l /tmp/frr-visible | awk '{print $5}') bytes"

echo "== install + start lldpd in each container =="
for n in $NODES; do
  docker exec "$n" sh -c 'command -v lldpd >/dev/null 2>&1 || apk add --no-cache lldpd >/dev/null 2>&1' || \
    echo "   WARN: lldpd install failed on $n (no internet?)"
  docker exec "$n" sh -c 'pkill lldpd 2>/dev/null; mkdir -p /run/lldpd; lldpd -d >/dev/null 2>&1 &' || true
done

echo "== bridge + VLAN/FDB on ce1 (feeds the FDB metric) =="
docker exec ce1 sh -c '
  ip link add br0 type bridge vlan_filtering 1 2>/dev/null || true
  ip link add v0 type veth peer name v1 2>/dev/null || true
  ip link set v0 master br0 2>/dev/null; ip link set v1 master br0 2>/dev/null
  ip link set br0 up; ip link set v0 up; ip link set v1 up
  bridge vlan add vid 10 dev v0 2>/dev/null || true
  bridge vlan add vid 20 dev v1 2>/dev/null || true
  bridge fdb add 02:00:00:aa:bb:01 dev v0 vlan 10 master static 2>/dev/null || true
  bridge fdb add 02:00:00:aa:bb:02 dev v1 vlan 20 master static 2>/dev/null || true
' || true

echo "== deploy + run shim (embedded) in each container =="
for n in $NODES; do
  docker cp /tmp/frr-visible "$n":/shim >/dev/null
  docker exec "$n" sh -c 'pkill -f "^/shim" 2>/dev/null || true; sleep 0.3'
  docker exec -d "$n" sh -c "/shim -gnmi :9339 -fpm 127.0.0.1:2620 -bmp 127.0.0.1:5000 -target $n > /var/log/shim.log 2>&1"
  echo "   $n: shim up (gNMI :9339, target=$n)"
done

echo "== point FRR at the shim: FPM (all), BMP (bgp nodes), OSPF syslog =="
# FPM on every zebra
for n in $NODES; do
  docker exec "$n" vtysh -c 'configure terminal' -c 'fpm address 127.0.0.1 port 2620' >/dev/null 2>&1 || true
done
# BMP wherever bgpd runs
for n in "${!ASN[@]}"; do
  as=${ASN[$n]}
  docker exec "$n" vtysh \
    -c 'configure terminal' \
    -c "router bgp $as" \
    -c ' bmp targets T1' \
    -c '  bmp connect 127.0.0.1 port 5000 min-retry 1000 max-retry 5000' \
    -c '  bmp monitor ipv4 unicast pre-policy' \
    -c '  bmp monitor ipv4 unicast post-policy' \
    -c '  bmp monitor ipv4 vpn pre-policy' \
    -c '  bmp monitor ipv4 vpn post-policy' >/dev/null 2>&1 || true
done
# PE-CE eBGP sessions live in `router bgp 65000 vrf cust`; that instance needs its
# own bmp target so the shim sees the CE neighbors (bmp targets are per-instance).
for n in pe1 pe2; do
  docker exec "$n" vtysh \
    -c 'configure terminal' \
    -c 'router bgp 65000 vrf cust' \
    -c ' bmp targets T1' \
    -c '  bmp connect 127.0.0.1 port 5000 min-retry 1000 max-retry 5000' \
    -c '  bmp monitor ipv4 unicast pre-policy' \
    -c '  bmp monitor ipv4 unicast post-policy' >/dev/null 2>&1 || true
done
# OSPF adjacency changes -> syslog -> shim /dev/log
for n in $OSPF_NODES; do
  docker exec "$n" vtysh -c 'configure terminal' -c 'log syslog informational' >/dev/null 2>&1 || true
done

echo "== done. shim reachable at mgmt IPs :9339 =="
echo "   pe1 172.31.0.11 | pe2 172.31.0.12 | p1 172.31.0.21 | p2 172.31.0.22"
echo "   ce1 172.31.0.101 | ce2 172.31.0.102 | ce3 172.31.0.103 | ce4 172.31.0.104"
