// cgroup.go samples the container's own cgroup (v2) for CPU/memory and writes
// them under the `host` origin — OpenConfig has no "container" concept, so these
// live in a private tree. These are gauges/counters (no kernel event), hence
// SAMPLE, exactly like interface counters. In embedded deployment (shim in the
// FRR container) /sys/fs/cgroup is the FRR container's cgroup — the number we want.
package ingest

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"

	"frr-visible/internal/state"
)

type Cgroup struct {
	c    *state.Cache
	poll time.Duration
	root string
}

func NewCgroup(c *state.Cache, poll time.Duration) *Cgroup {
	return &Cgroup{c: c, poll: poll, root: "/sys/fs/cgroup"}
}

func (g *Cgroup) Run() error {
	if _, err := os.Stat(g.root + "/cpu.stat"); err != nil {
		log.Printf("[cgroup] %s/cpu.stat not found (cgroup v2?), disabling: %v", g.root, err)
		return nil
	}
	g.sample()
	t := time.NewTicker(g.poll)
	defer t.Stop()
	for range t.C {
		g.sample()
	}
	return nil
}

func (g *Cgroup) sample() {
	var ups []*gnmipb.Update

	cpu := readKV(g.root + "/cpu.stat")
	for _, f := range []struct{ key, leaf string }{
		{"usage_usec", "usage-usec"},
		{"user_usec", "user-usec"},
		{"system_usec", "system-usec"},
		{"nr_periods", "nr-periods"},
		{"nr_throttled", "nr-throttled"},
		{"throttled_usec", "throttled-usec"},
	} {
		if v, ok := cpu[f.key]; ok {
			ups = append(ups, hostUpdate("cpu", f.leaf, v))
		}
	}

	if v, ok := readUint(g.root + "/memory.current"); ok {
		ups = append(ups, hostUpdate("memory", "current", v))
	}
	if v, ok := readUint(g.root + "/memory.swap.current"); ok {
		ups = append(ups, hostUpdate("memory", "swap-current", v))
	}
	if v, ok := readLimit(g.root + "/memory.max"); ok { // "max" -> no limit -> skip
		ups = append(ups, hostUpdate("memory", "max", v))
	}

	if len(ups) > 0 {
		_ = g.c.Update("host", ups, nil)
	}
	log.Printf("[cgroup] sampled cpu.usage_usec=%d mem.current=%d", cpu["usage_usec"], mustUint(g.root+"/memory.current"))
}

// ---- helpers ----

func hostUpdate(group, leaf string, v uint64) *gnmipb.Update {
	return &gnmipb.Update{
		Path: &gnmipb.Path{Elem: []*gnmipb.PathElem{
			{Name: "container"},
			{Name: group},
			{Name: "state"},
			{Name: leaf},
		}},
		Val: uintVal(v),
	}
}

// readKV parses "key value" lines (cpu.stat) into a map.
func readKV(path string) map[string]uint64 {
	m := map[string]uint64{}
	f, err := os.Open(path)
	if err != nil {
		return m
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 {
			if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				m[fields[0]] = v
			}
		}
	}
	return m
}

func readUint(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// readLimit reads memory.max ("max" means unlimited -> skip).
func readLimit(path string) (uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(b))
	if s == "max" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	return v, err == nil
}

func mustUint(path string) uint64 { v, _ := readUint(path); return v }
