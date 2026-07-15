// trace-aggregator stitches per-node convergence traces (each shim's :9340/traces)
// into cross-device "distributed traces": one topology event seen across every
// router it touched, as a single merged timeline.
//
// Routing has no propagated trace-id, so correlation is reconstructed: per-node
// traces whose start times fall within a window are grouped (a link flap is seen
// at both ends within sub-millisecond, and remote FIB churn follows within the
// window). Link endpoints (pe1-p1 <-> p1-pe1) normalize to one link as corroboration.
//
// Config via env:  INVENTORY node=mgmtIP,...   WINDOW (default 1.5s)   INTERVAL (3s)   LISTEN (:9341)
// Serves the distributed traces as JSON at /dtraces.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"frr-visible/internal/correlate"
)

type dspan struct {
	OffsetMs int64  `json:"off_ms"`
	Node     string `json:"node"`
	Bus      string `json:"bus"`
	Kind     string `json:"kind"`
	Key      string `json:"key"`
	Detail   string `json:"detail"`
	abs      time.Time
}

type dtrace struct {
	ID     int       `json:"id"`
	Start  time.Time `json:"start"`
	SpanMs int64     `json:"span_ms"`
	Link   string    `json:"link,omitempty"`
	Nodes  []string  `json:"nodes"`
	Roots  []string  `json:"roots"`
	Spans  []dspan   `json:"spans"`
	Txn    string    `json:"txn,omitempty"`    // governing transaction (git commit) that caused this convergence
	TxnAct string    `json:"txn_action,omitempty"`
	TxnBy  string    `json:"txn_agent,omitempty"`
}

// txnRec is one governance transaction anchor pulled from Loki (job=frr-txn),
// written by the write plane (frr-netctl/frr-mgmtd) at git-commit time.
type txnRec struct {
	ID       string `json:"txn"`
	Lab      string `json:"lab"`
	Action   string `json:"action"`
	Agent    string `json:"agent"`
	CommitNs int64  `json:"ts_commit_ns"`
}

var (
	inventory = map[string]string{}
	window    = 1500 * time.Millisecond
	interval  = 3 * time.Second
	listen    = ":9341"
	zipkinURL = "" // TEMPO_ZIPKIN, e.g. http://tempo:9411/api/v2/spans; empty disables export

	lokiTxnURL = ""                       // LOKI_TXN, e.g. http://loki:3100; empty disables txn stitching
	labFilter  = ""                       // LAB — only stitch txns for this lab (empty = any)
	txnWindow  = 120 * time.Second        // TXN_WINDOW — max lag from commit to convergence start
	txnSkew    = 10 * time.Second         // tolerate mac(commit)↔VM(trace) clock skew

	pushed = map[string]bool{} // trace_ids already exported to Tempo
	mu     sync.RWMutex
	out    []dtrace
	txns   []txnRec // recent governance anchors, newest last
	txnMu  sync.RWMutex
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("[trace-aggregator] ")
	inventory = parseInventory(os.Getenv("INVENTORY"))
	if v := os.Getenv("WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			window = d
		}
	}
	if v := os.Getenv("INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			interval = d
		}
	}
	if v := os.Getenv("LISTEN"); v != "" {
		listen = v
	}
	zipkinURL = os.Getenv("TEMPO_ZIPKIN")
	lokiTxnURL = strings.TrimRight(os.Getenv("LOKI_TXN"), "/")
	labFilter = os.Getenv("LAB")
	if v := os.Getenv("TXN_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			txnWindow = d
		}
	}
	if len(inventory) == 0 {
		log.Fatal("need INVENTORY (node=mgmtIP,...)")
	}
	log.Printf("%d nodes, window=%s, interval=%s, listen=%s", len(inventory), window, interval, listen)
	if lokiTxnURL != "" {
		log.Printf("txn stitching on: loki=%s lab=%q window=%s", lokiTxnURL, labFilter, txnWindow)
		go pollTxns(&http.Client{Timeout: 4 * time.Second})
	}

	go loop()
	http.HandleFunc("/dtraces", func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	log.Fatal(http.ListenAndServe(listen, nil))
}

func loop() {
	cl := &http.Client{Timeout: 4 * time.Second}
	for {
		dt := build(cl)
		for i := range dt {
			if t := matchTxn(dt[i].Start); t != nil {
				dt[i].Txn, dt[i].TxnAct, dt[i].TxnBy = t.ID, t.Action, t.Agent
			}
		}
		mu.Lock()
		out = dt
		mu.Unlock()
		if zipkinURL != "" {
			for i := range dt {
				pushZipkin(cl, dt[i])
			}
		}
		time.Sleep(interval)
	}
}

// pollTxns pulls recent governance anchors from Loki (job=frr-txn) on a slow
// cadence. Each anchor is a git commit stamped at write-plane commit time; we
// keep a rolling window so matchTxn can attribute a reconstructed convergence
// trace to the transaction that caused it — the read/write seam, closed.
func pollTxns(cl *http.Client) {
	for {
		fetched := fetchTxns(cl)
		if fetched != nil {
			txnMu.Lock()
			txns = fetched
			txnMu.Unlock()
		}
		time.Sleep(10 * time.Second)
	}
}

func fetchTxns(cl *http.Client) []txnRec {
	q := `{job="frr-txn"}`
	end := time.Now()
	start := end.Add(-30 * time.Minute)
	u := lokiTxnURL + "/loki/api/v1/query_range?limit=200&direction=forward" +
		"&query=" + urlQuery(q) +
		"&start=" + strconv.FormatInt(start.UnixNano(), 10) +
		"&end=" + strconv.FormatInt(end.UnixNano(), 10)
	resp, err := cl.Get(u)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var lr struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &lr) != nil {
		return nil
	}
	var recs []txnRec
	for _, s := range lr.Data.Result {
		for _, v := range s.Values {
			var r txnRec
			if json.Unmarshal([]byte(v[1]), &r) == nil && r.ID != "" && r.CommitNs > 0 {
				recs = append(recs, r)
			}
		}
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].CommitNs < recs[j].CommitNs })
	return recs
}

// matchTxn attributes a convergence trace (started at `start`) to the most
// recent governance transaction whose commit precedes it within TXN_WINDOW.
// A small negative skew is tolerated because the commit timestamp comes from
// the write plane (mac) while trace times come from the shims (VM).
func matchTxn(start time.Time) *txnRec {
	st := start.UnixNano()
	lo := st - int64(txnWindow)
	hi := st + int64(txnSkew)
	txnMu.RLock()
	defer txnMu.RUnlock()
	var best *txnRec
	for i := range txns {
		t := &txns[i]
		if labFilter != "" && t.Lab != labFilter {
			continue
		}
		if t.CommitNs < lo || t.CommitNs > hi {
			continue
		}
		if best == nil || t.CommitNs > best.CommitNs {
			best = t
		}
	}
	return best
}

// urlQuery percent-encodes a LogQL selector for the query string.
func urlQuery(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			const hexd = "0123456789ABCDEF"
			b.WriteByte(hexd[c>>4])
			b.WriteByte(hexd[c&0xf])
		}
	}
	return b.String()
}

// build pulls every node's per-device traces and clusters them by start time.
func build(cl *http.Client) []dtrace {
	var all []correlate.Trace
	for node, ip := range inventory {
		addr := ip
		if !strings.Contains(addr, ":") {
			addr += ":9340"
		}
		for _, t := range fetch(cl, "http://"+addr+"/traces") {
			if t.Node == "" {
				t.Node = node
			}
			all = append(all, t)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Start.Before(all[j].Start) })

	var groups [][]correlate.Trace
	for _, t := range all {
		if n := len(groups); n > 0 && t.Start.Sub(groups[n-1][0].Start) <= window {
			groups[n-1] = append(groups[n-1], t)
		} else {
			groups = append(groups, []correlate.Trace{t})
		}
	}

	dts := make([]dtrace, 0, len(groups))
	for i, g := range groups {
		dts = append(dts, merge(i+1, g))
	}
	return dts
}

func merge(id int, g []correlate.Trace) dtrace {
	var spans []dspan
	nodeSet := map[string]bool{}
	var roots []string
	linkVotes := map[string]int{}
	for _, t := range g {
		nodeSet[t.Node] = true
		roots = append(roots, t.Node+": "+t.Root)
		if lk := linkKey(t.Root); lk != "" {
			linkVotes[lk]++
		}
		for _, s := range t.Spans {
			abs := t.Start.Add(time.Duration(s.OffsetMs) * time.Millisecond)
			spans = append(spans, dspan{
				Node: t.Node, Bus: s.Bus, Kind: s.Kind, Key: s.Key, Detail: s.Detail, abs: abs,
			})
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].abs.Before(spans[j].abs) })
	start := time.Time{}
	if len(spans) > 0 {
		start = spans[0].abs
	}
	var span int64
	for i := range spans {
		spans[i].OffsetMs = spans[i].abs.Sub(start).Milliseconds()
		if spans[i].OffsetMs > span {
			span = spans[i].OffsetMs
		}
	}
	nodes := keys(nodeSet)
	sort.Strings(nodes)
	link := ""
	best := 0
	for k, v := range linkVotes {
		if v > best {
			best, link = v, k
		}
	}
	return dtrace{ID: id, Start: start, SpanMs: span, Link: link, Nodes: nodes, Roots: roots, Spans: spans}
}

// linkKey normalizes a link-event root ("netlink/link-down pe1-p1") to a
// direction-independent key ("p1--pe1"); returns "" for non-link roots.
func linkKey(root string) string {
	i := strings.Index(root, "link-")
	if i < 0 {
		return ""
	}
	f := strings.Fields(root[i:])
	if len(f) < 2 {
		return ""
	}
	iface := f[1] // e.g. pe1-p1
	parts := strings.SplitN(iface, "-", 2)
	if len(parts) != 2 {
		return ""
	}
	a, b := parts[0], parts[1]
	if a > b {
		a, b = b, a
	}
	return a + "--" + b
}

func fetch(cl *http.Client, url string) []correlate.Trace {
	resp, err := cl.Get(url)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var ts []correlate.Trace
	_ = json.Unmarshal(body, &ts)
	return ts
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func parseInventory(s string) map[string]string {
	m := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if i := strings.IndexByte(kv, '='); i > 0 {
			m[kv[:i]] = kv[i+1:]
		}
	}
	return m
}

// ---- Tempo export (Zipkin v2 spans; no OTel SDK needed) ----

type zEndpoint struct {
	ServiceName string `json:"serviceName"`
}

type zSpan struct {
	TraceID       string            `json:"traceId"`
	ID            string            `json:"id"`
	ParentID      string            `json:"parentId,omitempty"`
	Name          string            `json:"name"`
	Timestamp     int64             `json:"timestamp"` // epoch micros
	Duration      int64             `json:"duration"`  // micros
	LocalEndpoint zEndpoint         `json:"localEndpoint"`
	Tags          map[string]string `json:"tags,omitempty"`
}

// txnTags stamps the governing-transaction attributes onto a span's tag set.
func txnTags(m map[string]string, dt dtrace) {
	if dt.Txn == "" {
		return
	}
	m["txn.id"] = dt.Txn
	if dt.TxnAct != "" {
		m["txn.action"] = dt.TxnAct
	}
	if dt.TxnBy != "" {
		m["txn.agent"] = dt.TxnBy
	}
}

func hexID(seed string, n int) string {
	h := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(h[:n])
}

// pushZipkin exports one distributed trace to Tempo as Zipkin v2 spans, exactly
// once per trace_id. The dtrace becomes a root span; each hop becomes a child
// span timestamped at its absolute time and tagged with its owning node — so
// Grafana renders the cross-device waterfall natively.
func pushZipkin(cl *http.Client, dt dtrace) {
	seed := dt.Start.Format(time.RFC3339Nano) + "|" + dt.Link + "|" + strings.Join(dt.Roots, ";")
	traceID := hexID(seed, 16) // 16 bytes -> 32 hex
	if pushed[traceID] {
		return
	}
	// Hold a not-yet-attributed trace briefly so the Loki txn poll can catch the
	// governing commit; otherwise export once settled (organic, un-governed flap).
	if lokiTxnURL != "" && dt.Txn == "" && time.Since(dt.Start) < 12*time.Second {
		return
	}
	rootID := hexID(traceID+"|root", 8)
	name := "convergence"
	if dt.Link != "" {
		name += " " + dt.Link
	}
	rootTags := map[string]string{"link": dt.Link, "nodes": strings.Join(dt.Nodes, ","), "roots": strings.Join(dt.Roots, " | ")}
	txnTags(rootTags, dt) // stamp the governing transaction, if attributed
	spans := []zSpan{{
		TraceID: traceID, ID: rootID, Name: name,
		Timestamp: dt.Start.UnixMicro(), Duration: dt.SpanMs*1000 + 1,
		LocalEndpoint: zEndpoint{ServiceName: "network"},
		Tags:          rootTags,
	}}
	for i, s := range dt.Spans {
		abs := dt.Start.Add(time.Duration(s.OffsetMs) * time.Millisecond)
		dur := int64(1000)
		if i+1 < len(dt.Spans) {
			if g := (dt.Spans[i+1].OffsetMs - s.OffsetMs) * 1000; g > dur {
				dur = g
			}
		}
		childTags := map[string]string{"key": s.Key, "detail": s.Detail}
		txnTags(childTags, dt) // children carry txn.id too, so any span is queryable by transaction
		spans = append(spans, zSpan{
			TraceID: traceID, ID: hexID(traceID+"|"+strconv.Itoa(i), 8), ParentID: rootID,
			Name:      s.Bus + "/" + s.Kind,
			Timestamp: abs.UnixMicro(), Duration: dur,
			LocalEndpoint: zEndpoint{ServiceName: s.Node},
			Tags:          childTags,
		})
	}
	b, _ := json.Marshal(spans)
	resp, err := cl.Post(zipkinURL, "application/json", bytes.NewReader(b))
	if err != nil {
		log.Printf("zipkin push %s: %v", traceID, err)
		return
	}
	resp.Body.Close()
	pushed[traceID] = true
	log.Printf("exported dtrace #%d -> tempo (trace_id=%s, %d spans)", dt.ID, traceID, len(spans))
}
