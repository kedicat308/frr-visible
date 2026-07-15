// ospf.go covers OSPF neighbor adjacency (metric 5). FRR has no clean external
// event bus for OSPF, so we use the design's chosen source: syslog. The shim
// binds /dev/log (unix datagram) and FRR `log syslog` PUSHES adjacency-change
// messages to it — event-driven, not polling. Each OSPF message triggers a
// reconcile via `vtysh show ip ospf neighbor json` (accurate structured state;
// syslog is only the "something changed" trigger, same pattern as lldp watch).
//
// Syslog is structurally self-blind: it can never tell it dropped a message, and
// on an FRR restart it goes silent with no signal at all. So reconcile is driven
// from three directions, exploiting the multi-plane design (other buses testify
// for the one that can't):
//   1. startup snapshot — one full reconcile on boot.
//   2. reconnect-triggered (Kick) — FPM/BMP are dial-in TCP; when zebra/bgpd
//      re-attach, FRR restarted, and syslog gave no hint. A channel WITH
//      reconciliation ability (FPM/BMP reconnect) triggers the one WITHOUT it.
//   3. low-frequency safety net (default 60s) — cheap backstop that guarantees
//      "my adjacency state is eventually correct" even if a syslog line is lost.
// Default-on for (3): not from distrust of syslog, but because syslog has no
// self-healing; one `show ip ospf neighbor json`/60s is negligible and buys the
// correctness floor the rest of the system's credibility rests on.
package ingest

import (
	"encoding/json"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"

	"frr-visible/internal/correlate"
	"frr-visible/internal/state"
)

type OSPF struct {
	c        *state.Cache
	vtysh    string
	sock     string
	fallback time.Duration
	seen     map[string]string // router-id -> interface (for deletes)
	st       map[string]string // router-id -> adjacency-state (for change detection)
	cor      *correlate.Correlator
	trigger  chan struct{} // reconcile requests (coalesced); shared by syslog/Kick/ticker
}

func NewOSPF(c *state.Cache, fallback time.Duration) *OSPF {
	return &OSPF{c: c, vtysh: "vtysh", sock: "/dev/log", fallback: fallback,
		seen: map[string]string{}, st: map[string]string{}, trigger: make(chan struct{}, 1)}
}

// SetCorrelator wires the convergence-trace correlator (optional).
func (o *OSPF) SetCorrelator(cor *correlate.Correlator) { o.cor = cor }

// Kick requests an out-of-band reconcile from another plane. Used when a bus
// that CAN detect FRR restarts (FPM/BMP dial-in reconnect) must wake the OSPF
// bus that CANNOT (syslog goes silent across a restart). Non-blocking/coalesced.
func (o *OSPF) Kick(reason string) {
	log.Printf("[ospf] reconcile kicked by %s", reason)
	signal(o.trigger)
}

func (o *OSPF) Run() error {
	o.reconcile() // startup snapshot
	go o.runSyslog(o.trigger)
	go o.reconcileWorker(o.trigger)
	if o.fallback > 0 {
		go func() {
			t := time.NewTicker(o.fallback)
			defer t.Stop()
			for range t.C {
				signal(o.trigger)
			}
		}()
	}
	select {}
}

// runSyslog binds /dev/log and ONLY drains it, signalling the worker on OSPF
// messages. It must never block on reconcile: /dev/log is a unix datagram socket
// with reliable delivery, so a slow reader back-pressures FRR's syslog() and can
// wedge the daemons. Draining continuously + a debounced worker keeps FRR safe —
// the monitor must never harm the monitored (cf. CoPP).
func (o *OSPF) runSyslog(trigger chan<- struct{}) {
	_ = os.Remove(o.sock)
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: o.sock, Net: "unixgram"})
	if err != nil {
		log.Printf("[ospf] syslog bind %s failed (snapshot/fallback only): %v", o.sock, err)
		return
	}
	_ = os.Chmod(o.sock, 0666)
	_ = conn.SetReadBuffer(1 << 20) // absorb adjacency bursts
	log.Printf("[ospf] syslog receiver on %s (configure FRR: log syslog informational)", o.sock)
	buf := make([]byte, 8192)
	for {
		n, _, err := conn.ReadFromUnix(buf) // always draining
		if err != nil {
			continue
		}
		if strings.Contains(strings.ToLower(string(buf[:n])), "ospf") {
			signal(trigger)
		}
	}
}

// reconcileWorker coalesces a burst of triggers into one debounced reconcile,
// bounding how often we fork vtysh.
func (o *OSPF) reconcileWorker(trigger <-chan struct{}) {
	for range trigger {
		time.Sleep(300 * time.Millisecond) // let the adjacency burst settle
		o.reconcile()
	}
}

// signal does a non-blocking send (coalesce: at most one pending trigger).
func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (o *OSPF) reconcile() {
	out, err := exec.Command(o.vtysh, "-c", "show ip ospf neighbor json").Output()
	if err != nil {
		return
	}
	cur := parseOSPFNbrs(out)

	seen := map[string]string{}
	for rid, n := range cur {
		seen[rid] = n.iface
		st := adjState(n.state)
		ups := []*gnmipb.Update{
			leafUpdate(ospfNbrElems(n.iface, rid, "adjacency-state"), st),
			leafUpdate(ospfNbrElems(n.iface, rid, "neighbor-address"), n.addr),
		}
		_ = o.c.Update("openconfig", ups, nil)
		log.Printf("[ospf] nbr %s if=%s state=%s", rid, n.iface, st)
		if old, ok := o.st[rid]; ok && old != st { // transition only (skip first-seen)
			o.cor.Emit("ospf", "adj-"+lc(st), rid, "if="+n.iface+" "+old+"->"+st, true)
		}
		o.st[rid] = st
	}
	for rid, iface := range o.seen {
		if _, ok := seen[rid]; !ok {
			_ = o.c.Update("openconfig", nil, []*gnmipb.Path{{Elem: ospfNbrElems(iface, rid, "")}})
			log.Printf("[ospf] nbr DEL %s", rid)
			o.cor.Emit("ospf", "adj-down", rid, "if="+iface+" neighbor gone", true)
			delete(o.st, rid)
		}
	}
	o.seen = seen
}

// lc lowercases an ASCII adjacency-state enum for a compact span kind.
func lc(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

type ospfNbr struct {
	routerID, iface, addr, state string
}

func parseOSPFNbrs(data []byte) map[string]*ospfNbr {
	res := map[string]*ospfNbr{}
	var root struct {
		Neighbors map[string][]struct {
			NbrState     string `json:"nbrState"`
			Converged    string `json:"converged"`
			IfaceName    string `json:"ifaceName"`
			IfaceAddress string `json:"ifaceAddress"`
		} `json:"neighbors"`
	}
	if json.Unmarshal(data, &root) != nil {
		return res
	}
	for rid, arr := range root.Neighbors {
		if len(arr) == 0 {
			continue
		}
		n := arr[0]
		iface := n.IfaceName
		if i := strings.IndexByte(iface, ':'); i >= 0 { // "eth0:172.30.0.11" -> "eth0"
			iface = iface[:i]
		}
		state := n.Converged
		if state == "" {
			state = n.NbrState
		}
		res[rid] = &ospfNbr{routerID: rid, iface: iface, addr: n.IfaceAddress, state: state}
	}
	return res
}

// adjState maps FRR state to the OpenConfig adjacency-state enum.
func adjState(s string) string {
	switch strings.SplitN(s, "/", 2)[0] {
	case "Full":
		return "FULL"
	case "2-Way":
		return "TWO_WAY"
	case "Init":
		return "INIT"
	case "Down":
		return "DOWN"
	case "ExStart":
		return "EXCHANGE_START"
	case "Exchange":
		return "EXCHANGE"
	case "Loading":
		return "LOADING"
	case "Attempt":
		return "ATTEMPT"
	default:
		return "UNKNOWN"
	}
}

// openconfig: /network-instances/network-instance[name=default]/protocols/
//
//	protocol[OSPF]/ospfv2/areas/area[0.0.0.0]/interfaces/interface[id]/neighbors/
//	neighbor[router-id]/state/<leaf>   (area defaulted to 0.0.0.0; single-area lab)
func ospfNbrElems(iface, rid, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "network-instances"},
		{Name: "network-instance", Key: map[string]string{"name": "default"}},
		{Name: "protocols"},
		{Name: "protocol", Key: map[string]string{"identifier": "OSPF", "name": "ospf"}},
		{Name: "ospfv2"},
		{Name: "areas"},
		{Name: "area", Key: map[string]string{"identifier": "0.0.0.0"}},
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"id": iface}},
		{Name: "neighbors"},
		{Name: "neighbor", Key: map[string]string{"router-id": rid}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}
