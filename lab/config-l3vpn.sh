#!/usr/bin/env bash
# config-l3vpn.sh — PHASE 2: routing on top of the veth topology from build-topo.sh.
#   core  : OSPF area0 + LDP  (pe1-p1-p2-pe2)
#   VPN   : iBGP VPNv4 pe1<->pe2, VRF cust on both PEs
#   PE-CE : eBGP (ce1/ce2 @ pe1, ce3/ce4 @ pe2) — one VPN, RT 65000:1
# Interface names are deterministic (A-B), so no runtime detection needed.
# Run inside the my-frr VM after build-topo.sh.
set -euo pipefail

CORE="pe1 p1 p2 pe2"
BGPNODES="pe1 pe2 ce1 ce2 ce3 ce4"   # p1/p2 run no bgpd
ALL="pe1 pe2 p1 p2 ce1 ce2 ce3 ce4"

# ---- enable daemons with FPM/BMP modules + integrated config ----------------
DAEMONS='zebra=yes
bgpd=yes
ospfd=yes
ldpd=yes
staticd=yes
mgmtd=yes
vtysh_enable=yes
zebra_options="  -A 127.0.0.1 -s 90000000 -M dplane_fpm_nl"
bgpd_options="   -A 127.0.0.1 -M bmp"
ospfd_options="  -A 127.0.0.1"
ldpd_options="   -A 127.0.0.1"'
for n in $ALL; do
  docker exec "$n" sh -c "printf '%s\n' \"\$0\" > /etc/frr/daemons" "$DAEMONS"
  docker exec "$n" sh -c "echo 'service integrated-vtysh-config' > /etc/frr/vtysh.conf"
done

# ---- VRF cust on the PEs (enslave the CE-facing veths; addr is retained) ----
echo "== VRF cust on PEs =="
for pe in pe1 pe2; do
  docker exec "$pe" ip link add cust type vrf table 100 2>/dev/null || true
  docker exec "$pe" ip link set cust up
done
docker exec pe1 sh -c 'ip link set pe1-ce1 master cust; ip link set pe1-ce2 master cust'
docker exec pe2 sh -c 'ip link set pe2-ce3 master cust; ip link set pe2-ce4 master cust'

# ---- MPLS input on core-facing interfaces -----------------------------------
mplsin() { docker exec "$1" sysctl -w "net.mpls.conf.$2.input=1" >/dev/null 2>&1 || true; }
mplsin pe1 pe1-p1
mplsin p1 p1-pe1; mplsin p1 p1-p2
mplsin p2 p2-p1;  mplsin p2 p2-pe2
mplsin pe2 pe2-p2

# ---- per-node integrated frr.conf (interface IPs already in kernel) ---------
push() { docker exec -i "$1" sh -c "cat > /etc/frr/frr.conf"; }
echo "== render frr.conf =="

push pe1 <<'EOF'
hostname pe1
interface pe1-p1
 ip ospf network point-to-point
!
router ospf
 ospf router-id 10.255.0.1
 network 10.0.12.0/30 area 0
 network 10.255.0.1/32 area 0
 log-adjacency-changes detail
!
router bgp 65000
 bgp router-id 10.255.0.1
 neighbor 10.255.0.2 remote-as 65000
 neighbor 10.255.0.2 update-source lo
 address-family ipv4 unicast
  no neighbor 10.255.0.2 activate
 exit-address-family
 address-family ipv4 vpn
  neighbor 10.255.0.2 activate
 exit-address-family
!
router bgp 65000 vrf cust
 bgp router-id 10.255.0.1
 no bgp ebgp-requires-policy
 neighbor 10.0.21.2 remote-as 65101
 neighbor 10.0.22.2 remote-as 65102
 address-family ipv4 unicast
  neighbor 10.0.21.2 activate
  neighbor 10.0.22.2 activate
  redistribute connected
  label vpn export auto
  rd vpn export 65000:1
  rt vpn both 65000:1
  export vpn
  import vpn
 exit-address-family
EOF

push pe2 <<'EOF'
hostname pe2
interface pe2-p2
 ip ospf network point-to-point
!
router ospf
 ospf router-id 10.255.0.2
 network 10.0.14.0/30 area 0
 network 10.255.0.2/32 area 0
 log-adjacency-changes detail
!
router bgp 65000
 bgp router-id 10.255.0.2
 neighbor 10.255.0.1 remote-as 65000
 neighbor 10.255.0.1 update-source lo
 address-family ipv4 unicast
  no neighbor 10.255.0.1 activate
 exit-address-family
 address-family ipv4 vpn
  neighbor 10.255.0.1 activate
 exit-address-family
!
router bgp 65000 vrf cust
 bgp router-id 10.255.0.2
 no bgp ebgp-requires-policy
 neighbor 10.0.23.2 remote-as 65103
 neighbor 10.0.24.2 remote-as 65104
 address-family ipv4 unicast
  neighbor 10.0.23.2 activate
  neighbor 10.0.24.2 activate
  redistribute connected
  label vpn export auto
  rd vpn export 65000:2
  rt vpn both 65000:1
  export vpn
  import vpn
 exit-address-family
EOF

push p1 <<'EOF'
hostname p1
interface p1-pe1
 ip ospf network point-to-point
interface p1-p2
 ip ospf network point-to-point
!
router ospf
 ospf router-id 10.255.0.11
 network 10.0.12.0/30 area 0
 network 10.0.13.0/30 area 0
 network 10.255.0.11/32 area 0
 log-adjacency-changes detail
EOF

push p2 <<'EOF'
hostname p2
interface p2-p1
 ip ospf network point-to-point
interface p2-pe2
 ip ospf network point-to-point
!
router ospf
 ospf router-id 10.255.0.12
 network 10.0.13.0/30 area 0
 network 10.0.14.0/30 area 0
 network 10.255.0.12/32 area 0
 log-adjacency-changes detail
EOF

# CEs: plain eBGP to their PE, advertise connected (incl. loopback)
ce_conf() { # node asn peer-ip
  cat <<EOF
hostname $1
router bgp $2
 bgp router-id ${4}
 no bgp ebgp-requires-policy
 neighbor $3 remote-as 65000
 address-family ipv4 unicast
  redistribute connected
 exit-address-family
EOF
}
ce_conf ce1 65101 10.0.21.1 10.255.1.1 | push ce1
ce_conf ce2 65102 10.0.22.1 10.255.1.2 | push ce2
ce_conf ce3 65103 10.0.23.1 10.255.1.3 | push ce3
ce_conf ce4 65104 10.0.24.1 10.255.1.4 | push ce4

# ---- start FRR --------------------------------------------------------------
echo "== start FRR =="
for n in $ALL; do
  docker exec "$n" /usr/lib/frr/frrinit.sh start >/dev/null 2>&1 || \
    docker exec "$n" /usr/lib/frr/frrinit.sh restart >/dev/null 2>&1 || true
done

# ---- LDP via vtysh (reliable vs integrated-config load order) ----------------
echo "== configure LDP on the core =="
ldp() { # node router-id coreif...
  local n=$1 id=$2; shift 2
  local cmds=(-c 'configure terminal' -c 'mpls ldp' -c " router-id $id" -c ' address-family ipv4' -c "  discovery transport-address $id")
  local i; for i in "$@"; do cmds+=(-c "  interface $i"); done
  docker exec "$n" vtysh "${cmds[@]}" >/dev/null 2>&1 || true
}
sleep 2
ldp pe1 10.255.0.1 pe1-p1
ldp p1  10.255.0.11 p1-pe1 p1-p2
ldp p2  10.255.0.12 p2-p1 p2-pe2
ldp pe2 10.255.0.2 pe2-p2

echo "== phase 2 done: give OSPF/LDP/BGP ~40s, then check-topo.sh =="
