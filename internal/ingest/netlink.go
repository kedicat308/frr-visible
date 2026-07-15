// netlink.go subscribes to kernel netlink multicast groups (the same push the
// design calls for) and turns them into gNMI updates:
//   - link add/del/up/down  -> /interfaces/interface[name]/state/{admin,oper}-status  (ON_CHANGE)
//   - bridge FDB add/del     -> /network-instances/.../fdb/mac-table/entries/entry    (ON_CHANGE)
//   - interface counters     -> /interfaces/interface[name]/state/counters/*          (SAMPLE poll)
//
// Requires sharing FRR's netns (sidecar), same as vrf.go.
package ingest

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"

	"frr-visible/internal/correlate"
	"frr-visible/internal/lineage"
	"frr-visible/internal/state"
)

type Netlink struct {
	c    *state.Cache
	poll time.Duration
	cor  *correlate.Correlator
	lin  *lineage.Tracker
}

func NewNetlink(c *state.Cache, poll time.Duration) *Netlink {
	return &Netlink{c: c, poll: poll}
}

// SetCorrelator wires the convergence-trace correlator (optional).
func (n *Netlink) SetCorrelator(cor *correlate.Correlator) { n.cor = cor }

// SetLineage wires the cross-plane route lineage tracker (optional). netlink's
// kernel route group is the "kernel confirmed" witness in the chain.
func (n *Netlink) SetLineage(l *lineage.Tracker) { n.lin = l }

func (n *Netlink) Run() error {
	done := make(chan struct{})

	linkCh := make(chan netlink.LinkUpdate, 64)
	if err := netlink.LinkSubscribe(linkCh, done); err != nil {
		return err
	}
	go n.linkLoop(linkCh)

	neighCh := make(chan netlink.NeighUpdate, 64)
	if err := netlink.NeighSubscribe(neighCh, done); err == nil {
		go n.neighLoop(neighCh)
	}

	// Kernel route table (RTNLGRP_IPV4/IPV6_ROUTE), across all tables incl. VRFs.
	// ListExisting dumps the current FIB once so the baseline is known, then live.
	routeCh := make(chan netlink.RouteUpdate, 256)
	if err := netlink.RouteSubscribeWithOptions(routeCh, done,
		netlink.RouteSubscribeOptions{ListExisting: true}); err == nil {
		go n.routeLoop(routeCh)
	} else {
		log.Printf("[netlink] route subscribe failed (kernel-truth plane off): %v", err)
	}

	n.snapshot() // initial full state
	go n.counterLoop()

	log.Printf("[netlink] subscribed (link + fdb + kernel routes, counters every %s)", n.poll)
	select {}
}

// ---- kernel route table (RTNLGRP_*_ROUTE), ON_CHANGE ----
//
// This is the kernel's own testimony of what is *actually* installed, distinct
// from FPM (what zebra *intended* to push). The two firing together for a prefix
// is the fpm->kernel confirmation; FPM without a kernel route is a silent install
// failure. Feeds both the convergence correlator and the lineage tracker.
func (n *Netlink) routeLoop(ch <-chan netlink.RouteUpdate) {
	for u := range ch {
		if u.Route.Family != unix.AF_INET && u.Route.Family != unix.AF_INET6 {
			continue
		}
		prefix := routePrefix(u.Route)
		if prefix == "" {
			continue
		}
		table := strconv.Itoa(u.Route.Table)
		del := u.Type == unix.RTM_DELROUTE
		if del {
			_ = n.c.Update("frr", nil, []*gnmipb.Path{{Elem: routeElems(prefix, table, "")}})
			log.Printf("[netlink] kernel-route DEL %s table=%s", prefix, table)
			if n.cor != nil {
				n.cor.Emit("netlink", "kernel-route-del", prefix, "table="+table, false)
			}
			if n.lin != nil {
				n.lin.Observe(prefix, lineage.Kernel, false, "table="+table)
			}
			continue
		}
		proto := routeProto(u.Route.Protocol)
		gw := "-"
		if u.Route.Gw != nil {
			gw = u.Route.Gw.String()
		}
		ups := []*gnmipb.Update{
			leafUpdate(routeElems(prefix, table, "next-hop"), gw),
			leafUpdate(routeElems(prefix, table, "protocol"), proto),
		}
		_ = n.c.Update("frr", ups, nil)
		log.Printf("[netlink] kernel-route ADD %s via %s proto=%s table=%s", prefix, gw, proto, table)
		if n.cor != nil {
			n.cor.Emit("netlink", "kernel-route-add", prefix, "proto="+proto+" nh="+gw+" table="+table, false)
		}
		if n.lin != nil {
			n.lin.Observe(prefix, lineage.Kernel, true, "proto="+proto+" nh="+gw+" table="+table)
		}
	}
}

// routePrefix renders a route's destination as CIDR; nil Dst = default route.
func routePrefix(r netlink.Route) string {
	if r.Dst == nil {
		if r.Family == unix.AF_INET6 {
			return "::/0"
		}
		return "0.0.0.0/0"
	}
	return r.Dst.String()
}

// routeProto names the rtm_protocol (who installed it): zebra=confirms an FPM
// push landed; kernel/boot=connected/local; others as-is.
func routeProto(p netlink.RouteProtocol) string {
	switch int(p) {
	case unix.RTPROT_KERNEL:
		return "kernel"
	case unix.RTPROT_BOOT:
		return "boot"
	case unix.RTPROT_STATIC:
		return "static"
	case 11: // RTPROT_ZEBRA
		return "zebra"
	case unix.RTPROT_BGP:
		return "bgp"
	case unix.RTPROT_OSPF:
		return "ospf"
	default:
		return strconv.Itoa(int(p))
	}
}

// ---- link (interface status), ON_CHANGE ----

func (n *Netlink) linkLoop(ch <-chan netlink.LinkUpdate) {
	for u := range ch {
		name := u.Link.Attrs().Name
		if u.Header.Type == unix.RTM_DELLINK {
			_ = n.c.Update("openconfig", nil, []*gnmipb.Path{{Elem: ifaceElems(name, "")}})
			log.Printf("[netlink] link DEL %s", name)
			n.cor.Emit("netlink", "link-down", name, "link removed", true)
			continue
		}
		n.writeLinkState(u.Link)
		oper := operStatus(u.Link.Attrs().OperState)
		kind := "link-down"
		if oper == "UP" {
			kind = "link-up"
		}
		n.cor.Emit("netlink", kind, name, "oper="+oper, true)
	}
}

func (n *Netlink) writeLinkState(l netlink.Link) {
	a := l.Attrs()
	admin := "DOWN"
	if a.Flags&net.FlagUp != 0 {
		admin = "UP"
	}
	ups := []*gnmipb.Update{
		leafUpdate(ifaceElems(a.Name, "name"), a.Name),
		leafUpdate(ifaceElems(a.Name, "admin-status"), admin),
		leafUpdate(ifaceElems(a.Name, "oper-status"), operStatus(a.OperState)),
		leafUpdate(ifaceElems(a.Name, "type"), l.Type()),
		{Path: &gnmipb.Path{Elem: ifaceElems(a.Name, "ifindex")}, Val: uintVal(uint64(a.Index))},
		{Path: &gnmipb.Path{Elem: ifaceElems(a.Name, "mtu")}, Val: uintVal(uint64(a.MTU))},
	}
	_ = n.c.Update("openconfig", ups, nil)
	log.Printf("[netlink] link %s admin=%s oper=%s type=%s", a.Name, admin, operStatus(a.OperState), l.Type())
}

func operStatus(s netlink.LinkOperState) string {
	switch s {
	case netlink.OperUp:
		return "UP"
	case netlink.OperDown:
		return "DOWN"
	case netlink.OperLowerLayerDown:
		return "LOWER_LAYER_DOWN"
	case netlink.OperDormant:
		return "DORMANT"
	case netlink.OperTesting:
		return "TESTING"
	case netlink.OperNotPresent:
		return "NOT_PRESENT"
	default:
		return "UNKNOWN"
	}
}

// ---- bridge FDB (MAC table), ON_CHANGE ----

func (n *Netlink) neighLoop(ch <-chan netlink.NeighUpdate) {
	for u := range ch {
		if u.Neigh.Family != unix.AF_BRIDGE || !isUnicastMAC(u.Neigh.HardwareAddr) {
			continue
		}
		mac := u.Neigh.HardwareAddr.String()
		vlan := strconv.Itoa(u.Neigh.Vlan)
		if u.Type == unix.RTM_DELNEIGH {
			_ = n.c.Update("openconfig", nil, []*gnmipb.Path{{Elem: fdbElems(mac, vlan, "")}})
			log.Printf("[netlink] fdb DEL %s vlan=%s", mac, vlan)
			continue
		}
		ifName := ""
		if lk, err := netlink.LinkByIndex(u.Neigh.LinkIndex); err == nil {
			ifName = lk.Attrs().Name
		}
		ups := []*gnmipb.Update{
			leafUpdate(fdbElems(mac, vlan, "mac-address"), mac),
			leafUpdate(fdbElems(mac, vlan, "interface"), ifName),
		}
		_ = n.c.Update("openconfig", ups, nil)
		log.Printf("[netlink] fdb ADD %s vlan=%s if=%s", mac, vlan, ifName)
	}
}

// ---- interface counters, SAMPLE ----

func (n *Netlink) counterLoop() {
	t := time.NewTicker(n.poll)
	defer t.Stop()
	for range t.C {
		n.sampleCounters()
		n.snapshotMPLS()
		n.snapshotAddrs()
	}
}

// snapshotMPLS dumps the kernel MPLS forwarding table (the LFIB) and exports each
// entry under frr:/mpls/lfib/entry[label]/state. The vishvananda/netlink library
// decodes the AF_MPLS routes for us: MPLSDst=incoming label, NewDst=swap label(s),
// Via/Gw=next-hop, LinkIndex=out interface. Three shapes matter for path tracing:
//   - swap:        NewDst set + next-hop set   (e.g. "18 as to 16 via …")
//   - PHP pop:     NewDst nil + next-hop set   (e.g. "16 via …")
//   - egress pop:  NewDst nil + next-hop nil   (e.g. "80 dev cust"  -> pop into VRF)
func (n *Netlink) snapshotMPLS() {
	routes, err := netlink.RouteList(nil, unix.AF_MPLS)
	if err != nil {
		return
	}
	cnt := 0
	for _, r := range routes {
		if r.MPLSDst == nil {
			continue
		}
		label := uint32(*r.MPLSDst)
		out := ""
		if md, ok := r.NewDst.(*netlink.MPLSDestination); ok && md != nil {
			out = intsCSV(md.Labels)
		}
		nh := ""
		if via, ok := r.Via.(*netlink.Via); ok && via != nil && len(via.Addr) > 0 {
			nh = net.IP(via.Addr).String()
		} else if r.Gw != nil {
			nh = r.Gw.String()
		}
		iface := ""
		if r.LinkIndex > 0 {
			if lk, err := netlink.LinkByIndex(r.LinkIndex); err == nil {
				iface = lk.Attrs().Name
			}
		}
		ups := []*gnmipb.Update{
			leafUpdate(mplsElems(label, "in-label"), strconv.FormatUint(uint64(label), 10)),
			leafUpdate(mplsElems(label, "out-label"), out),
			leafUpdate(mplsElems(label, "next-hop"), nh),
			leafUpdate(mplsElems(label, "interface"), iface),
		}
		_ = n.c.Update("frr", ups, nil)
		cnt++
	}
	log.Printf("[netlink] mpls lfib snapshot: %d entries", cnt)
}

// snapshotAddrs exports each interface's IPv4 addresses under OpenConfig
// /interfaces/interface[name]/subinterfaces/subinterface[index=0]/ipv4/addresses.
// This lets a gNMI client map a data-plane next-hop IP back to the owning node
// (e.g. path tracing) without logging into the box.
func (n *Netlink) snapshotAddrs() {
	links, err := netlink.LinkList()
	if err != nil {
		return
	}
	for _, l := range links {
		addrs, err := netlink.AddrList(l, unix.AF_INET)
		if err != nil {
			continue
		}
		name := l.Attrs().Name
		for _, a := range addrs {
			if a.IPNet == nil {
				continue
			}
			ip := a.IP.String()
			pl, _ := a.Mask.Size()
			ups := []*gnmipb.Update{
				leafUpdate(addrElems(name, ip, "ip"), ip),
				leafUpdate(addrElems(name, ip, "prefix-length"), strconv.Itoa(pl)),
			}
			_ = n.c.Update("openconfig", ups, nil)
		}
	}
}

func (n *Netlink) sampleCounters() {
	links, err := netlink.LinkList()
	if err != nil {
		return
	}
	for _, l := range links {
		a := l.Attrs()
		s := a.Statistics
		if s == nil {
			continue
		}
		ups := []*gnmipb.Update{
			counterUpdate(a.Name, "in-octets", s.RxBytes),
			counterUpdate(a.Name, "out-octets", s.TxBytes),
			counterUpdate(a.Name, "in-pkts", s.RxPackets),
			counterUpdate(a.Name, "out-pkts", s.TxPackets),
			counterUpdate(a.Name, "in-errors", s.RxErrors),
			counterUpdate(a.Name, "out-errors", s.TxErrors),
			counterUpdate(a.Name, "in-discards", s.RxDropped),
			counterUpdate(a.Name, "out-discards", s.TxDropped),
		}
		_ = n.c.Update("openconfig", ups, nil)
	}
}

func (n *Netlink) snapshot() {
	links, err := netlink.LinkList()
	if err != nil {
		return
	}
	for _, l := range links {
		n.writeLinkState(l)
	}
	n.sampleCounters()
	n.snapshotFDB()
	n.snapshotMPLS()
	n.snapshotAddrs()
}

// snapshotFDB dumps the existing bridge FDB so entries present before the shim
// started are visible; NeighSubscribe only delivers subsequent ON_CHANGE events.
func (n *Netlink) snapshotFDB() {
	neighs, err := netlink.NeighList(0, unix.AF_BRIDGE)
	if err != nil {
		return
	}
	cnt := 0
	for _, ne := range neighs {
		if !isUnicastMAC(ne.HardwareAddr) {
			continue
		}
		mac := ne.HardwareAddr.String()
		vlan := strconv.Itoa(ne.Vlan)
		ifName := ""
		if lk, err := netlink.LinkByIndex(ne.LinkIndex); err == nil {
			ifName = lk.Attrs().Name
		}
		ups := []*gnmipb.Update{
			leafUpdate(fdbElems(mac, vlan, "mac-address"), mac),
			leafUpdate(fdbElems(mac, vlan, "interface"), ifName),
		}
		_ = n.c.Update("openconfig", ups, nil)
		cnt++
	}
	log.Printf("[netlink] fdb snapshot: %d unicast entries", cnt)
}

// isUnicastMAC reports whether addr is a non-empty unicast MAC (even first
// octet) — filters out multicast/broadcast bridge noise (33:33:*, 01:00:5e:*).
func isUnicastMAC(addr net.HardwareAddr) bool {
	return len(addr) == 6 && addr[0]&1 == 0
}

// ---- path helpers ----

func ifaceElems(name, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": name}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}

func counterUpdate(name, leaf string, v uint64) *gnmipb.Update {
	return &gnmipb.Update{
		Path: &gnmipb.Path{Elem: []*gnmipb.PathElem{
			{Name: "interfaces"},
			{Name: "interface", Key: map[string]string{"name": name}},
			{Name: "state"},
			{Name: "counters"},
			{Name: leaf},
		}},
		Val: uintVal(v),
	}
}

func fdbElems(mac, vlan, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "network-instances"},
		{Name: "network-instance", Key: map[string]string{"name": "default"}},
		{Name: "fdb"},
		{Name: "mac-table"},
		{Name: "entries"},
		{Name: "entry", Key: map[string]string{"mac-address": mac, "vlan": vlan}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}

func uintVal(u uint64) *gnmipb.TypedValue {
	return &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: u}}
}

// mplsElems: frr:/mpls/lfib/entry[label=N]/state/<leaf>
func mplsElems(label uint32, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "mpls"},
		{Name: "lfib"},
		{Name: "entry", Key: map[string]string{"label": strconv.FormatUint(uint64(label), 10)}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}

// addrElems: /interfaces/interface[name]/subinterfaces/subinterface[index=0]/ipv4/addresses/address[ip]/state/<leaf>
func addrElems(ifname, ip, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": ifname}},
		{Name: "subinterfaces"},
		{Name: "subinterface", Key: map[string]string{"index": "0"}},
		{Name: "ipv4"},
		{Name: "addresses"},
		{Name: "address", Key: map[string]string{"ip": ip}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}

// routeElems: frr:/kernel-fib/routes/route[prefix][table]/state/<leaf> — the
// kernel's own FIB (netlink), kept distinct from FPM's aft (zebra intent) so a
// subscriber can compare "intended" vs "actually installed".
func routeElems(prefix, table, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "kernel-fib"},
		{Name: "routes"},
		{Name: "route", Key: map[string]string{"prefix": prefix, "table": table}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}

// intsCSV renders an MPLS label list as "16" or "16,80" (top of stack first).
func intsCSV(v []int) string {
	s := ""
	for i, x := range v {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%d", x)
	}
	return s
}
