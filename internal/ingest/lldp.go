// lldp.go turns lldpd neighbor changes into gNMI updates under /lldp.
// FRR does not do LLDP; a separate lldpd must run in the shim's mount ns.
// Pattern: use `lldpcli watch` purely as an event trigger (event-driven, no
// periodic poll), and on each change re-read `lldpcli -f keyvalue show neighbors`
// to reconcile the cache (add/update present, delete gone). keyvalue is a flat,
// robust format — far less fragile than parsing watch's human output or JSON.
package ingest

import (
	"bufio"
	"encoding/json"
	"log"
	"os/exec"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"

	"frr-visible/internal/state"
)

type LLDP struct {
	c    *state.Cache
	bin  string
	seen map[string]string // ifname -> chassis-id currently in cache
}

func NewLLDP(c *state.Cache) *LLDP {
	return &LLDP{c: c, bin: "lldpcli", seen: map[string]string{}}
}

func (l *LLDP) Run() error {
	l.reconcile() // initial snapshot

	cmd := exec.Command(l.bin, "watch")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	log.Printf("[lldp] watching lldpd for neighbor changes")

	// Safety-net reconcile: lldpcli watch block-buffers its stdout over a pipe,
	// so events can be delayed; a slow periodic reconcile guarantees convergence.
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for range t.C {
			l.reconcile()
		}
	}()

	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if strings.Contains(sc.Text(), "neighbor") { // "LLDP neighbor updated/deleted"
			l.reconcile()
		}
	}
	return cmd.Wait()
}

func (l *LLDP) reconcile() {
	out, err := exec.Command(l.bin, "-f", "json", "show", "neighbors").Output()
	if err != nil {
		return
	}
	cur := parseLLDP(out)

	seen := map[string]string{}
	for ifn, nb := range cur {
		if nb.chassisID == "" {
			continue
		}
		seen[ifn] = nb.chassisID
		ups := []*gnmipb.Update{
			leafUpdate(lldpElems(ifn, nb.chassisID, "chassis-id"), nb.chassisID),
			leafUpdate(lldpElems(ifn, nb.chassisID, "port-id"), nb.portID),
			leafUpdate(lldpElems(ifn, nb.chassisID, "system-name"), nb.sysName),
			leafUpdate(lldpElems(ifn, nb.chassisID, "system-description"), nb.sysDescr),
			leafUpdate(lldpElems(ifn, nb.chassisID, "management-address"), nb.mgmtIP),
		}
		_ = l.c.Update("openconfig", ups, nil)
		log.Printf("[lldp] neighbor if=%s chassis=%s sysname=%s port=%s", ifn, nb.chassisID, nb.sysName, nb.portID)
	}
	// delete neighbors that disappeared
	for ifn, id := range l.seen {
		if _, ok := seen[ifn]; !ok {
			_ = l.c.Update("openconfig", nil, []*gnmipb.Path{{Elem: lldpElems(ifn, id, "")}})
			log.Printf("[lldp] neighbor DEL if=%s chassis=%s", ifn, id)
		}
	}
	l.seen = seen
}

type lldpNbr struct {
	ifname, chassisID, portID, sysName, sysDescr, mgmtIP string
}

// parseLLDP parses `lldpcli -f json show neighbors`. lldpd nests chassis under
// its system-name and renders single/multiple items as object/array, so we
// traverse defensively. Shape:
//   lldp.interface.<if>.chassis.<sysname>.{id.value, descr, mgmt-ip}
//   lldp.interface.<if>.port.id.value
func parseLLDP(data []byte) map[string]*lldpNbr {
	res := map[string]*lldpNbr{}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		log.Printf("[lldp] json parse error: %v", err)
		return res
	}
	lldp := firstObj(root["lldp"])
	if lldp == nil {
		return res
	}
	if ifaces := asMap(lldp["interface"]); ifaces != nil { // object keyed by ifname
		for name, v := range ifaces {
			res[name] = parseIface(name, asMap(v))
		}
	} else if arr, ok := lldp["interface"].([]any); ok { // array form
		for _, e := range arr {
			m := asMap(e)
			if name := asStr(m["name"]); name != "" { // [{name:.., ...}]
				res[name] = parseIface(name, m)
			} else { // [{"<ifname>": {...}}] — lldpd 1.0.x default
				for name, v := range m {
					res[name] = parseIface(name, asMap(v))
				}
			}
		}
	}
	return res
}

func parseIface(ifname string, ifm map[string]any) *lldpNbr {
	nb := &lldpNbr{ifname: ifname}
	if ch := asMap(ifm["chassis"]); ch != nil {
		if _, direct := ch["id"]; direct {
			nb.sysName = asStr(ch["name"])
			fillChassis(nb, ch)
		} else {
			for name, cv := range ch { // keyed by system-name
				nb.sysName = name
				fillChassis(nb, asMap(cv))
				break
			}
		}
	}
	if port := asMap(ifm["port"]); port != nil {
		if id := asMap(port["id"]); id != nil {
			nb.portID = asStr(id["value"])
		}
		if nb.portID == "" { // some lldpd builds omit id.value in json
			nb.portID = asStr(port["descr"])
		}
	}
	if nb.chassisID == "" { // fall back to system-name as a stable id
		nb.chassisID = nb.sysName
	}
	return nb
}

func fillChassis(nb *lldpNbr, ch map[string]any) {
	if id := asMap(ch["id"]); id != nil {
		nb.chassisID = asStr(id["value"])
	}
	nb.sysDescr = asStr(ch["descr"])
	nb.mgmtIP = asStr(ch["mgmt-ip"])
}

// ---- defensive JSON coercion (lldpd renders single vs many as object vs array) ----

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func firstObj(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		return t
	case []any:
		if len(t) > 0 {
			return asMap(t[0])
		}
	}
	return nil
}

func asStr(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []any:
		if len(t) > 0 {
			return asStr(t[0])
		}
	}
	return ""
}

// openconfig: /lldp/interfaces/interface[name=if]/neighbors/neighbor[id=chassis]/state/<leaf>
func lldpElems(ifname, id, leaf string) []*gnmipb.PathElem {
	e := []*gnmipb.PathElem{
		{Name: "lldp"},
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": ifname}},
		{Name: "neighbors"},
		{Name: "neighbor", Key: map[string]string{"id": id}},
	}
	if leaf != "" {
		e = append(e, &gnmipb.PathElem{Name: "state"}, &gnmipb.PathElem{Name: leaf})
	}
	return e
}
