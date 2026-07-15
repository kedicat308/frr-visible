// topology-exporter builds the network's multi-layer topology by reading each
// node's gNMI shim directly (no gnmic dependency) and exposes it as Prometheus
// metrics shaped for a Grafana Node Graph panel. Four layers share one node set:
//
//	physical  LLDP adjacency        (/lldp/.../system-name)
//	ospf      OSPF full adjacencies (/…/ospfv2/…/adjacency-state == FULL)
//	bgp       BGP sessions          (/…/bgp/…/session-state == ESTABLISHED; iBGP
//	                                 shows as a logical edge across the core)
//	mpls      LFIB label-switched   (frr:/mpls/lfib/entry/state, next-hop/iface)
//	          transport segments
//
// Peer identity differs per layer (LLDP=node name, OSPF=router-id + iface name,
// BGP=peer IP, MPLS=next-hop IP/iface); everything is normalised back to node
// names in Go, which is exactly the mapping PromQL/label_join cannot do.
//
// Config via env:
//
//	INVENTORY  node=mgmtIP,...   (all nodes; mgmt gNMI :9339)
//	INTERVAL   refresh (default 15s)
//	LISTEN     listen address (default :9810)
//
// Metrics (one Node Graph query per frame kind, filtered by the `layer` label):
//
//	frr_topo_node{layer,id,title,role}         1
//	frr_topo_edge{layer,id,source,target,detail} 1
//	frr_topo_up                                1 if the last refresh completed
package main

import (
	"context"
	"fmt"
	"log"
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

var (
	inventory map[string]string // node -> mgmt ip
	interval  = 15 * time.Second
	listen    = ":9810"

	clients = map[string]gnmipb.GNMIClient{}
	dialMu  sync.Mutex

	out   string
	outMu sync.RWMutex
)

type edge struct {
	layer, src, dst, detail string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[topology-exporter] ")
	inventory = parseInventory(os.Getenv("INVENTORY"))
	if v := os.Getenv("INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}
	if v := os.Getenv("LISTEN"); v != "" {
		listen = v
	}
	if len(inventory) == 0 {
		log.Fatalf("need INVENTORY (node=ip,...)")
	}
	log.Printf("%d nodes, interval=%s, listen=%s", len(inventory), interval, listen)

	go loop()

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		outMu.RLock()
		defer outMu.RUnlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(out))
	})
	log.Fatal(http.ListenAndServe(listen, nil))
}

func loop() {
	for {
		outMu.Lock()
		out = render()
		outMu.Unlock()
		time.Sleep(interval)
	}
}

func render() string {
	ip2node := buildIP2Node()
	edges := map[string]edge{} // unordered pair "layer|a|b" -> edge (dedup both directions)
	addEdge := func(layer, a, b, detail string) {
		// Only real nodes: this also drops LFIB egress entries that point into a
		// VRF device (e.g. "cust") rather than a downstream LSR.
		if _, ok := inventory[a]; !ok {
			return
		}
		if _, ok := inventory[b]; !ok {
			return
		}
		if a == b {
			return
		}
		x, y := a, b
		if x > y {
			x, y = y, x
		}
		key := layer + "|" + x + "|" + y
		// A session is seen from both ends; for BGP the two ends can disagree on
		// the VRF (a PE reports vrf=cust, its CE reports vrf=default). Keep the
		// more specific non-default detail so the merge is order-independent.
		if prev, ok := edges[key]; ok && !strings.HasSuffix(prev.detail, "=default") {
			return
		}
		edges[key] = edge{layer: layer, src: x, dst: y, detail: detail}
	}
	// addPseudo is like addEdge but keeps pseudo-nodes (vlan:/mac:) that are not
	// in the inventory — the VLAN layer is a bridge-local switch--vlan--mac graph,
	// not a node-to-node fabric graph.
	addPseudo := func(a, b, detail string) {
		if a == "" || b == "" || a == b {
			return
		}
		x, y := a, b
		if x > y {
			x, y = y, x
		}
		key := "vlan|" + x + "|" + y
		if _, ok := edges[key]; ok {
			return
		}
		edges[key] = edge{layer: "vlan", src: x, dst: y, detail: detail}
	}

	for node := range inventory {
		// physical: LLDP neighbor system-name is already a node name.
		for _, u := range get(node, lldpPath()) {
			addEdge("physical", node, sval(u.Val), "lldp")
		}
		// ospf: interface id is "<self>-<peer>"; only FULL adjacencies are edges.
		for _, u := range get(node, ospfPath()) {
			if !strings.EqualFold(sval(u.Val), "FULL") {
				continue
			}
			keys, _ := dissect(u.Path)
			peer := ifacePeer(keys["id"], node)
			if peer == "" {
				peer = ip2node[keys["router-id"]]
			}
			addEdge("ospf", node, peer, "area")
		}
		// bgp: peer is an IP; map to node. VRF (network-instance) goes in detail.
		for _, u := range get(node, bgpPath()) {
			if !strings.EqualFold(sval(u.Val), "ESTABLISHED") {
				continue
			}
			keys, _ := dissect(u.Path)
			addEdge("bgp", node, ip2node[keys["neighbor-address"]], "vrf="+niName(u.Path))
		}
		// mpls: LFIB entry's next-hop / out interface points at the downstream LSR.
		nh := map[string]string{}  // label -> next-hop ip
		oif := map[string]string{} // label -> out interface
		for _, u := range get(node, lfibPath()) {
			keys, leaf := dissect(u.Path)
			switch leaf {
			case "next-hop":
				nh[keys["label"]] = sval(u.Val)
			case "interface":
				oif[keys["label"]] = sval(u.Val)
			}
		}
		for lbl, hop := range nh {
			peer := ip2node[hop]
			if peer == "" {
				peer = ifacePeer(oif[lbl], node)
			}
			addEdge("mpls", node, peer, "lsp")
		}
		// vlan: L2 bridge domains, local to a switch — switch--vlan--mac. Skip the
		// untagged/default VLANs (0/1) which just carry the bridge's own churn.
		for _, u := range get(node, fdbPath()) {
			keys, leaf := dissect(u.Path)
			if leaf != "mac-address" {
				continue
			}
			vlan, mac := keys["vlan"], keys["mac-address"]
			if vlan == "" || vlan == "0" || vlan == "1" || mac == "" {
				continue
			}
			addPseudo(node, "vlan:"+vlan, "member")
			addPseudo("vlan:"+vlan, "mac:"+mac, "learned")
		}
	}

	// Nodes are the endpoints of that layer's edges (so vlan/mac pseudo-nodes and
	// only the participating routers show up — no isolated dots).
	layerNodes := map[string]map[string]bool{}
	for _, e := range edges {
		if layerNodes[e.layer] == nil {
			layerNodes[e.layer] = map[string]bool{}
		}
		layerNodes[e.layer][e.src] = true
		layerNodes[e.layer][e.dst] = true
	}

	var b strings.Builder
	b.WriteString("# HELP frr_topo_node A node in the topology (per layer).\n# TYPE frr_topo_node gauge\n")
	for _, layer := range []string{"physical", "vlan", "ospf", "bgp", "mpls"} {
		names := make([]string, 0, len(layerNodes[layer]))
		for n := range layerNodes[layer] {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(&b, "frr_topo_node{layer=%q,id=%q,title=%q,role=%q} 1\n", layer, n, title(n), role(n))
		}
	}
	b.WriteString("# HELP frr_topo_edge An adjacency/session between two nodes (per layer).\n# TYPE frr_topo_edge gauge\n")
	ids := make([]string, 0, len(edges))
	for k := range edges {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	for _, k := range ids {
		e := edges[k]
		fmt.Fprintf(&b, "frr_topo_edge{layer=%q,id=%q,source=%q,target=%q,detail=%q} 1\n",
			e.layer, e.src+"|"+e.dst, e.src, e.dst, e.detail)
	}
	b.WriteString("# HELP frr_topo_up 1 if the last refresh completed.\n# TYPE frr_topo_up gauge\nfrr_topo_up 1\n")
	return b.String()
}

func role(name string) string {
	switch {
	case strings.HasPrefix(name, "vlan:"):
		return "VLAN"
	case strings.HasPrefix(name, "mac:"):
		return "MAC"
	case strings.HasPrefix(name, "pe"):
		return "PE"
	case strings.HasPrefix(name, "ce"):
		return "CE"
	case strings.HasPrefix(name, "p"):
		return "P"
	}
	return "?"
}

// title is the human label shown on a node: inventory nodes keep their name;
// vlan/mac pseudo-nodes get a compact form.
func title(name string) string {
	if s, ok := strings.CutPrefix(name, "vlan:"); ok {
		return "vlan" + s
	}
	if s, ok := strings.CutPrefix(name, "mac:"); ok {
		if p := strings.Split(s, ":"); len(p) >= 2 {
			return "…" + p[len(p)-2] + ":" + p[len(p)-1]
		}
		return s
	}
	return name
}

// niName returns the network-instance (VRF) name from a path. A dedicated walk is
// needed because both network-instance and protocol carry a "name" key, so a
// flattened key map would let protocol's name="bgp" shadow the VRF name.
func niName(p *gnmipb.Path) string {
	for _, e := range p.Elem {
		if e.Name == "network-instance" {
			return e.Key["name"]
		}
	}
	return ""
}

// ifacePeer parses the peer node out of a "<self>-<peer>" interface name.
func ifacePeer(iface, self string) string {
	for _, part := range strings.Split(iface, "-") {
		if part != "" && part != self {
			return part
		}
	}
	return ""
}

// ---- gNMI lookups (shared shape with pathtrace-exporter) ----

func buildIP2Node() map[string]string {
	m := map[string]string{}
	for node := range inventory {
		for _, u := range get(node, addrPath()) {
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

func get(node string, p *gnmipb.Path) []*gnmipb.Update {
	cl := client(node)
	if cl == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cl.Get(ctx, &gnmipb.GetRequest{Path: []*gnmipb.Path{p}, Encoding: gnmipb.Encoding_JSON_IETF})
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

// ---- path helpers ----

func lldpPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "openconfig", Elem: []*gnmipb.PathElem{
		{Name: "lldp"}, {Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": "*"}},
		{Name: "neighbors"},
		{Name: "neighbor", Key: map[string]string{"id": "*"}},
		{Name: "state"}, {Name: "system-name"},
	}}
}

func ospfPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "openconfig", Elem: []*gnmipb.PathElem{
		{Name: "network-instances"},
		{Name: "network-instance", Key: map[string]string{"name": "*"}},
		{Name: "protocols"},
		{Name: "protocol", Key: map[string]string{"identifier": "*", "name": "*"}},
		{Name: "ospfv2"}, {Name: "areas"},
		{Name: "area", Key: map[string]string{"identifier": "*"}},
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"id": "*"}},
		{Name: "neighbors"},
		{Name: "neighbor", Key: map[string]string{"router-id": "*"}},
		{Name: "state"}, {Name: "adjacency-state"},
	}}
}

func bgpPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "openconfig", Elem: []*gnmipb.PathElem{
		{Name: "network-instances"},
		{Name: "network-instance", Key: map[string]string{"name": "*"}},
		{Name: "protocols"},
		{Name: "protocol", Key: map[string]string{"identifier": "*", "name": "*"}},
		{Name: "bgp"}, {Name: "neighbors"},
		{Name: "neighbor", Key: map[string]string{"neighbor-address": "*"}},
		{Name: "state"}, {Name: "session-state"},
	}}
}

func lfibPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "frr", Elem: []*gnmipb.PathElem{
		{Name: "mpls"}, {Name: "lfib"},
		{Name: "entry", Key: map[string]string{"label": "*"}},
		{Name: "state"},
	}}
}

func fdbPath() *gnmipb.Path {
	return &gnmipb.Path{Origin: "openconfig", Elem: []*gnmipb.PathElem{
		{Name: "network-instances"},
		{Name: "network-instance", Key: map[string]string{"name": "*"}},
		{Name: "fdb"}, {Name: "mac-table"}, {Name: "entries"},
		{Name: "entry", Key: map[string]string{"mac-address": "*", "vlan": "*"}},
		{Name: "state"},
	}}
}

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
