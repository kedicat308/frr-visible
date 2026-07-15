package ingest

import (
	"fmt"
	"sync"

	"github.com/vishvananda/netlink"
)

// VRFResolver maps a kernel routing-table id to a Linux VRF device name.
// A VRF netdev (`ip link add cust type vrf table 100`) ties table 100 -> "cust".
// Requires the shim to share FRR's network namespace (sidecar), else it sees
// no VRF devices and falls back to "table:<id>".
type VRFResolver struct {
	mu sync.RWMutex
	m  map[uint32]string
}

func NewVRFResolver() *VRFResolver {
	r := &VRFResolver{m: map[uint32]string{}}
	r.Refresh()
	return r
}

// Refresh re-reads VRF devices from the current netns.
func (r *VRFResolver) Refresh() {
	links, err := netlink.LinkList()
	if err != nil {
		return
	}
	m := map[uint32]string{}
	for _, l := range links {
		if v, ok := l.(*netlink.Vrf); ok {
			m[uint32(v.Table)] = v.Name
		}
	}
	r.mu.Lock()
	r.m = m
	r.mu.Unlock()
}

// Name returns the network-instance name for a routing-table id.
func (r *VRFResolver) Name(table uint32) string {
	switch table {
	case 0, 254: // RT_TABLE_UNSPEC / RT_TABLE_MAIN
		return "default"
	case 255: // RT_TABLE_LOCAL
		return "local"
	}
	if n, ok := r.lookup(table); ok {
		return n
	}
	r.Refresh() // a VRF may have appeared since last refresh
	if n, ok := r.lookup(table); ok {
		return n
	}
	return fmt.Sprintf("table:%d", table)
}

// NonDefaultNames returns the names of all VRF devices in the current netns
// (i.e. every network-instance other than the default table). Refreshes first so
// a VRF configured after startup is seen.
func (r *VRFResolver) NonDefaultNames() []string {
	r.Refresh()
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.m))
	for _, n := range r.m {
		names = append(names, n)
	}
	return names
}

func (r *VRFResolver) lookup(table uint32) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.m[table]
	return n, ok
}
