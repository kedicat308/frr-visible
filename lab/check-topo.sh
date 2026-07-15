#!/usr/bin/env bash
# check-topo.sh — verify the 8-node backbone converged (run after config-l3vpn.sh).
set -uo pipefail
line() { printf '\n=== %s ===\n' "$1"; }

line "OSPF neighbors (pe1/p1/p2/pe2 — expect FULL; p1&p2 have 2 each)"
for n in pe1 p1 p2 pe2; do
  echo "-- $n"; docker exec "$n" vtysh -c "show ip ospf neighbor" 2>/dev/null | tail -n +2
done

line "LDP neighbors (expect OPERATIONAL)"
for n in pe1 p1 p2 pe2; do
  echo "-- $n"; docker exec "$n" vtysh -c "show mpls ldp neighbor" 2>/dev/null | tail -n +2
done

line "iBGP VPNv4 (pe1 <-> pe2)"
docker exec pe1 vtysh -c "show bgp ipv4 vpn summary" 2>/dev/null | grep -A3 Neighbor || echo "  (no vpn af)"

line "PE-CE eBGP in VRF cust (pe1: ce1/ce2 ; pe2: ce3/ce4)"
docker exec pe1 vtysh -c "show bgp vrf cust ipv4 summary" 2>/dev/null | grep -A4 Neighbor
docker exec pe2 vtysh -c "show bgp vrf cust ipv4 summary" 2>/dev/null | grep -A4 Neighbor

line "L3VPN forwarding: ce1 -> ce4 loopback across the VPN (src ce1 lo)"
docker exec ce1 ping -c2 -W2 -I 10.255.1.1 10.255.1.4 2>&1 | tail -n3

line "VRF FIB on pe1 (expect remote CE loopbacks via MPLS label)"
docker exec pe1 vtysh -c "show ip route vrf cust" 2>/dev/null | grep -E "B>.*label|10.255.1" | head
