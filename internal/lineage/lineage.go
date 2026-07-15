// Package lineage joins a prefix's life across the planes that can each witness
// one hop of it, so the shim can answer "where did this route die?":
//
//	BMP adj-rib-in  ->  FPM (zebra pushed)  ->  netlink route (kernel confirmed)
//	  neighbor sent me      my intent               kernel's own testimony
//
// No single plane can testify to the whole chain: BMP only knows what a peer
// sent, FPM only what zebra *intended* to install, the kernel route group only
// what is *actually* there. The gap between FPM and kernel is where silent
// install failures live (unresolved nexthop, full table, MPLS off) — the same
// cross-witness idea as OSPF's reconnect-triggered reconcile, applied to routes.
//
// A reconcile loop flags an entry that FPM claims installed but the kernel never
// confirmed within a grace window (install-gap), and a kernel route with no FPM
// intent behind it (orphan). Both surface via the supplied emit hook (correlator)
// and the /lineage snapshot.
package lineage

import (
	"strings"
	"sync"
	"time"
)

type Stage int

const (
	RibIn  Stage = iota // BMP: a neighbor advertised this to me (adj-rib-in)
	Fpm                 // zebra pushed it to the kernel (intent)
	Kernel              // the kernel confirmed the install (truth)
	nStages
)

var StageName = [nStages]string{"rib-in", "fpm", "kernel"}

type hop struct {
	present bool
	ts      time.Time
	detail  string
}

// Entry is a prefix's per-stage witness record.
type Entry struct {
	Prefix  string
	hops    [nStages]hop
	kproto  string // kernel route's rtm_protocol (zebra/kernel/boot/...) for orphan filtering
	flagged bool   // install-gap already emitted (edge-trigger)
}

// present reports whether a stage currently holds the prefix.
func (e *Entry) present(s Stage) bool { return e.hops[s].present }

// Tracker accumulates per-prefix, per-stage observations and reconciles them.
type Tracker struct {
	mu     sync.Mutex
	routes map[string]*Entry
	grace  time.Duration
	now    func() time.Time
	emit   func(kind, prefix, detail string) // anomalies -> correlator (nil ok)
}

func New(grace time.Duration, emit func(kind, prefix, detail string)) *Tracker {
	if grace <= 0 {
		grace = 3 * time.Second
	}
	return &Tracker{routes: map[string]*Entry{}, grace: grace, now: time.Now, emit: emit}
}

// Observe records that `stage` now (present) or no longer (!present) holds
// `prefix`. Safe for concurrent callers (each ingester runs its own goroutine).
func (t *Tracker) Observe(prefix string, stage Stage, present bool, detail string) {
	if prefix == "" || stage >= nStages {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.routes[prefix]
	if e == nil {
		e = &Entry{Prefix: prefix}
		t.routes[prefix] = e
	}
	e.hops[stage] = hop{present: present, ts: t.now(), detail: detail}
	if stage == Kernel && present {
		e.flagged = false           // kernel confirmed; re-arm gap detection for future churn
		e.kproto = protoOf(detail)  // remember who installed it (orphan filter)
	}
	if !present && !e.present(RibIn) && !e.present(Fpm) && !e.present(Kernel) {
		delete(t.routes, prefix) // fully withdrawn everywhere
	}
}

// reconcile scans for cross-plane contradictions once. Returns anomalies emitted.
func (t *Tracker) reconcile() {
	t.mu.Lock()
	type anomaly struct{ kind, prefix, detail string }
	var out []anomaly
	nowT := t.now()
	for _, e := range t.routes {
		// install-gap: zebra pushed it (FPM) but the kernel never confirmed.
		if e.present(Fpm) && !e.present(Kernel) && !e.flagged &&
			nowT.Sub(e.hops[Fpm].ts) > t.grace {
			e.flagged = true
			out = append(out, anomaly{"install-gap", e.Prefix,
				"fpm pushed but kernel unconfirmed >" + t.grace.String() + " (" + e.hops[Fpm].detail + ")"})
		}
	}
	t.mu.Unlock()
	if t.emit != nil {
		for _, a := range out {
			t.emit(a.kind, a.prefix, a.detail)
		}
	}
}

// protoOf pulls the "proto=x" token out of a kernel-stage observe detail.
func protoOf(detail string) string {
	const k = "proto="
	i := strings.Index(detail, k)
	if i < 0 {
		return ""
	}
	s := detail[i+len(k):]
	if j := strings.IndexByte(s, ' '); j >= 0 {
		s = s[:j]
	}
	return s
}

// Run drives the reconcile loop until the process exits.
func (t *Tracker) Run(interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for range tk.C {
		t.reconcile()
	}
}

// Row is one prefix's flattened lineage for the /lineage snapshot.
type Row struct {
	Prefix string            `json:"prefix"`
	Stages map[string]string `json:"stages"` // stage -> detail (only present stages)
	Gap    string            `json:"gap,omitempty"`
}

// Snapshot renders every tracked prefix's stage coverage (for /lineage).
func (t *Tracker) Snapshot() []Row {
	t.mu.Lock()
	defer t.mu.Unlock()
	rows := make([]Row, 0, len(t.routes))
	for _, e := range t.routes {
		r := Row{Prefix: e.Prefix, Stages: map[string]string{}}
		for s := Stage(0); s < nStages; s++ {
			if e.present(s) {
				d := e.hops[s].detail
				if d == "" {
					d = "yes"
				}
				r.Stages[StageName[s]] = d
			}
		}
		if e.present(Fpm) && !e.present(Kernel) {
			r.Gap = "install-gap"
		} else if e.present(Kernel) && !e.present(Fpm) && e.kproto == "zebra" {
			// zebra-installed in the kernel but FPM never reported the push — a real
			// intent/truth mismatch. Connected/local (proto kernel/boot) are
			// legitimately kernel-only and not flagged.
			r.Gap = "orphan-kernel"
		}
		rows = append(rows, r)
	}
	return rows
}
