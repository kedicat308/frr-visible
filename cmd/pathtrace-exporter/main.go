// pathtrace-exporter periodically runs the gNMI-native control-plane path trace
// (the same walk as lab/pathtrace-gnmi.sh) for a set of configured flows and
// exposes the result as Prometheus metrics on /metrics. It speaks gNMI directly
// (no gnmic / no bash), so it runs in any minimal container.
//
// Config via env:
//
//	INVENTORY  node=mgmtIP,...            (all nodes; used for ip->node + hopping)
//	FLOWS      name:startNode>dstIP,...   (flows to trace)
//	INTERVAL   scrape refresh, e.g. 15s   (default 15s)
//	LISTEN     listen address             (default :9808)
//
// Metrics:
//
//	frr_pathtrace_reachable{flow,src,dst}          1 if dst reached, else 0
//	frr_pathtrace_hops{flow,src,dst}               number of hops walked
//	frr_pathtrace_duration_seconds{flow,src,dst}   wall time of the trace
//	frr_pathtrace_hop_info{flow,seq,node,kind,nexthop,labels,detail} 1   (one per hop)
//	frr_pathtrace_up                               1 if the last refresh completed
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type flow struct{ name, start, dst string }
type hop struct {
	seq            int
	node, kind     string
	nexthop, label string
	detail         string
}

var (
	inventory map[string]string // node -> mgmt ip
	flows     []flow
	interval  = 15 * time.Second
	listen    = ":9808"

	clients = map[string]gnmipb.GNMIClient{}
	dialMu  sync.Mutex

	out   string // rendered metrics text
	outMu sync.RWMutex
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[pathtrace-exporter] ")
	inventory = parseInventory(os.Getenv("INVENTORY"))
	flows = parseFlows(os.Getenv("FLOWS"))
	if v := os.Getenv("INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}
	if v := os.Getenv("LISTEN"); v != "" {
		listen = v
	}
	if len(inventory) == 0 || len(flows) == 0 {
		log.Fatalf("need INVENTORY and FLOWS (got %d nodes, %d flows)", len(inventory), len(flows))
	}
	log.Printf("%d nodes, %d flows, interval=%s, listen=%s", len(inventory), len(flows), interval, listen)

	go loop()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		outMu.RLock()
		defer outMu.RUnlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		io.WriteString(w, out)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "pathtrace-exporter — see /metrics\n")
	})
	log.Fatal(http.ListenAndServe(listen, nil))
}

func loop() {
	for {
		start := time.Now()
		outMu.Lock()
		out = render()
		outMu.Unlock()
		d := time.Since(start)
		if d < interval {
			time.Sleep(interval - d)
		}
	}
}

// ---- rendering ----

func render() string {
	var b strings.Builder
	b.WriteString("# HELP frr_pathtrace_reachable 1 if the destination was reached by the control-plane path walk\n")
	b.WriteString("# TYPE frr_pathtrace_reachable gauge\n")
	b.WriteString("# HELP frr_pathtrace_hops number of hops walked\n# TYPE frr_pathtrace_hops gauge\n")
	b.WriteString("# HELP frr_pathtrace_duration_seconds wall time of the trace\n# TYPE frr_pathtrace_duration_seconds gauge\n")
	b.WriteString("# HELP frr_pathtrace_hop_info one series per hop (value always 1)\n# TYPE frr_pathtrace_hop_info gauge\n")

	ip2node := buildIP2Node()

	for _, f := range flows {
		t0 := time.Now()
		hops, reached := trace(ip2node, f.start, f.dst)
		dur := time.Since(t0).Seconds()
		lbl := fmt.Sprintf(`flow=%q,src=%q,dst=%q`, f.name, f.start, f.dst)
		rv := 0
		if reached {
			rv = 1
		}
		fmt.Fprintf(&b, "frr_pathtrace_reachable{%s} %d\n", lbl, rv)
		fmt.Fprintf(&b, "frr_pathtrace_hops{%s} %d\n", lbl, len(hops))
		fmt.Fprintf(&b, "frr_pathtrace_duration_seconds{%s} %g\n", lbl, dur)
		for _, h := range hops {
			fmt.Fprintf(&b, "frr_pathtrace_hop_info{flow=%q,seq=%q,node=%q,kind=%q,nexthop=%q,labels=%q,detail=%q} 1\n",
				f.name, fmt.Sprintf("%02d", h.seq), h.node, h.kind, h.nexthop, h.label, h.detail)
		}
	}
	b.WriteString("# HELP frr_pathtrace_up 1 if the last refresh completed\n# TYPE frr_pathtrace_up gauge\n")
	b.WriteString("frr_pathtrace_up 1\n")
	return b.String()
}

// ---- the trace (mirrors lab/pathtrace-gnmi.sh) ----

func trace(ip2node map[string]string, start, dst string) ([]hop, bool) {
	var hops []hop
	node := start
	stack := []string{}
	seq := 1
	for i := 0; i < 24; i++ {
		if ip2node[dst] == node && len(stack) == 0 {
			hops = append(hops, hop{seq: seq, node: node, kind: "dest", detail: "destination " + dst + " reached"})
			return hops, true
		}
		if len(stack) == 0 {
			// IP phase
			vrf, pfx, nh, labels := routeLookup(node, dst)
			if nh == "" && labels == "" && pfx == "" {
				hops = append(hops, hop{seq: seq, node: node, kind: "drop", detail: "no route to " + dst})
				return hops, false
			}
			h := hop{seq: seq, node: node, nexthop: nh, detail: "vrf=" + vrf + " " + pfx}
			if labels != "" {
				stack = strings.Split(labels, ",")
				h.kind = "ip-push"
				h.label = labels
			} else {
				h.kind = "ip"
			}
			hops = append(hops, h)
			seq++
			node = nodeOf(ip2node, nh)
		} else {
			// MPLS phase
			top := stack[0]
			outLbl, nh, iface := mplsLookup(node, top)
			if outLbl == "" && nh == "" && iface == "" {
				hops = append(hops, hop{seq: seq, node: node, kind: "drop", detail: "no LFIB entry for label " + top})
				return hops, false
			}
			rest := stack[1:]
			switch {
			case outLbl != "":
				newTop := strings.Split(outLbl, ",")
				stack = append(append([]string{}, newTop...), rest...)
				hops = append(hops, hop{seq: seq, node: node, kind: "mpls-swap", nexthop: nh, label: top + "->" + outLbl,
					detail: "if=" + iface + " stack[" + strings.Join(stack, " ") + "]"})
				seq++
				node = nodeOf(ip2node, nh)
			case nh != "":
				stack = rest
				hops = append(hops, hop{seq: seq, node: node, kind: "mpls-pop-php", nexthop: nh, label: top,
					detail: "if=" + iface + " stack[" + strings.Join(stack, " ") + "]"})
				seq++
				node = nodeOf(ip2node, nh)
			default:
				stack = rest // egress pop into VRF (iface = vrf name); stay on node
				hops = append(hops, hop{seq: seq, node: node, kind: "mpls-pop-vrf", label: top, detail: "-> VRF " + iface})
				seq++
			}
		}
		if node == "?" {
			hops = append(hops, hop{seq: seq, node: "?", kind: "drop", detail: "next-hop not in inventory"})
			return hops, false
		}
	}
	return hops, false
}

func nodeOf(ip2node map[string]string, ip string) string {
	if n, ok := ip2node[ip]; ok {
		return n
	}
	return "?"
}

// ---- gNMI lookups ----

func buildIP2Node() map[string]string {
	m := map[string]string{}
	for node := range inventory {
		ups := get(node, addrPath())
		for _, u := range ups {
			keys, _ := dissect(u.Path)
			ip := keys["ip"]
			if ip == "" || strings.HasPrefix(ip, "127.") || strings.HasPrefix(ip, "172.31.") {
				continue
			}
			m[ip] = node
		}
	}
	return m
}

// routeLookup returns vrf, prefix, next-hop, pushed-labels for the longest-prefix
// match of dst across all VRFs (the network-instance[name=*] wildcard).
func routeLookup(node, dst string) (vrf, prefix, nexthop, labels string) {
	type ent struct{ vrf, nh, lbl string }
	byPfx := map[string]*ent{}
	for _, u := range get(node, aftPath()) {
		keys, leaf := dissect(u.Path)
		pfx := keys["prefix"]
		if pfx == "" {
			continue
		}
		e := byPfx[pfx]
		if e == nil {
			e = &ent{}
			byPfx[pfx] = e
		}
		switch leaf {
		case "next-hop":
			e.nh = sval(u.Val)
			e.vrf = keys["name"]
		case "pushed-mpls-label-stack":
			e.lbl = sval(u.Val)
		}
	}
	di := net.ParseIP(dst).To4()
	if di == nil {
		return
	}
	best := -1
	for pfx, e := range byPfx {
		if e.nh == "" && e.lbl == "" {
			continue
		}
		_, ipnet, err := net.ParseCIDR(pfx)
		if err != nil {
			continue
		}
		ones, _ := ipnet.Mask.Size()
		if ipnet.Contains(di) && ones > best {
			best = ones
			vrf, prefix, nexthop, labels = e.vrf, pfx, e.nh, e.lbl
		}
	}
	return
}

func mplsLookup(node, label string) (outLabel, nexthop, iface string) {
	for _, u := range get(node, lfibPath(label)) {
		_, leaf := dissect(u.Path)
		switch leaf {
		case "out-label":
			outLabel = sval(u.Val)
		case "next-hop":
			nexthop = sval(u.Val)
		case "interface":
			iface = sval(u.Val)
		}
	}
	return
}

// get performs a gNMI Get and flattens all notifications' updates.
func get(node string, p *gnmipb.Path) []*gnmipb.Update {
	cl := client(node)
	if cl == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cl.Get(ctx, &gnmipb.GetRequest{
		Path:     []*gnmipb.Path{p},
		Encoding: gnmipb.Encoding_JSON_IETF,
	})
	if err != nil {
		return nil
	}
	var ups []*gnmipb.Update
	for _, n := range resp.Notification {
		ups = append(ups, n.Update...)
	}
	return ups
}

func client(node string) gnmipb.GNMIClient {
	dialMu.Lock()
	defer dialMu.Unlock()
	if c, ok := clients[node]; ok {
		return c
	}
	addr := inventory[node]
	if addr == "" {
		return nil
	}
	if !strings.Contains(addr, ":") {
		addr += ":9339"
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("dial %s: %v", addr, err)
		return nil
	}
	c := gnmipb.NewGNMIClient(conn)
	clients[node] = c
	return c
}

// ---- path helpers + parsing ----

func addrPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "openconfig", Elem: []*gnmipb.PathElem{
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": "*"}},
		{Name: "subinterfaces"},
		{Name: "subinterface", Key: map[string]string{"index": "0"}},
		{Name: "ipv4"}, {Name: "addresses"},
		{Name: "address", Key: map[string]string{"ip": "*"}},
		{Name: "state"}, {Name: "ip"},
	}}
}

func aftPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "openconfig", Elem: []*gnmipb.PathElem{
		{Name: "network-instances"},
		{Name: "network-instance", Key: map[string]string{"name": "*"}},
		{Name: "afts"}, {Name: "ipv4-unicast"},
		{Name: "ipv4-entry", Key: map[string]string{"prefix": "*"}},
		{Name: "state"},
	}}
}

func lfibPath(label string) *gnmipb.Path {
	return &gnmipb.Path{Origin: "frr", Elem: []*gnmipb.PathElem{
		{Name: "mpls"}, {Name: "lfib"},
		{Name: "entry", Key: map[string]string{"label": label}},
		{Name: "state"},
	}}
}

func dissect(p *gnmipb.Path) (keys map[string]string, leaf string) {
	keys = map[string]string{}
	if p == nil {
		return
	}
	for _, e := range p.Elem {
		for k, v := range e.Key {
			keys[k] = v
		}
		leaf = e.Name
	}
	return
}

func sval(v *gnmipb.TypedValue) string {
	switch x := v.GetValue().(type) {
	case *gnmipb.TypedValue_StringVal:
		return x.StringVal
	case *gnmipb.TypedValue_JsonIetfVal:
		return strings.Trim(string(x.JsonIetfVal), `"`)
	case *gnmipb.TypedValue_JsonVal:
		return strings.Trim(string(x.JsonVal), `"`)
	case *gnmipb.TypedValue_UintVal:
		return strconv.FormatUint(x.UintVal, 10)
	}
	return ""
}

// ---- config parsing ----

func parseInventory(s string) map[string]string {
	m := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		if i := strings.IndexByte(kv, '='); i > 0 {
			m[kv[:i]] = kv[i+1:]
		}
	}
	return m
}

func parseFlows(s string) []flow {
	var fs []flow
	for _, spec := range strings.Split(s, ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		name, sd := spec, spec
		if i := strings.IndexByte(spec, ':'); i > 0 {
			name, sd = spec[:i], spec[i+1:]
		}
		i := strings.IndexByte(sd, '>')
		if i <= 0 {
			continue
		}
		fs = append(fs, flow{name: name, start: sd[:i], dst: sd[i+1:]})
	}
	sort.Slice(fs, func(i, j int) bool { return fs[i].name < fs[j].name })
	return fs
}
