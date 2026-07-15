// Command frr-visible is the gNMI shim: it runs event-driven ingesters that
// feed a shared OpenConfig cache, and serves gNMI Subscribe over it.
// v0 wires just the FPM ingester (route/FIB) — the shortest closed loop.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"time"

	"frr-visible/internal/correlate"
	"frr-visible/internal/gnmiserver"
	"frr-visible/internal/ingest"
	"frr-visible/internal/lineage"
	"frr-visible/internal/state"
)

func main() {
	gnmiAddr := flag.String("gnmi", ":9339", "gNMI Subscribe listen address")
	fpmAddr := flag.String("fpm", ":2620", "FPM listen address (zebra dials in here)")
	bmpAddr := flag.String("bmp", ":5000", "BMP listen address (bgpd dials in here)")
	target := flag.String("target", "frr", "gNMI cache target name")
	ospfReconcile := flag.Duration("ospf-reconcile", 60*time.Second, "OSPF safety-net reconcile interval (0=off); default-on because syslog can't detect its own drops/restarts")
	traceHTTP := flag.String("trace-http", ":9340", "convergence-trace HTTP endpoint (/traces); empty to disable")
	flag.Parse()

	c := state.New(*target)

	// Convergence-trace correlator: folds cross-bus events into causal traces.
	cor := correlate.New(*target)
	go cor.Run()

	// Route lineage: joins a prefix across BMP(rib-in) -> FPM(intent) -> kernel
	// (truth) so "where did this route die?" is answerable. Anomalies (e.g. an
	// install-gap: pushed by zebra but never confirmed by the kernel) surface as
	// root-cause spans in the correlator.
	lin := lineage.New(3*time.Second, func(kind, prefix, detail string) {
		cor.Emit("lineage", kind, prefix, detail, true)
	})
	go lin.Run(2 * time.Second)

	if *traceHTTP != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/traces", cor.ServeHTTP)
		mux.HandleFunc("/lineage", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(lin.Snapshot())
		})
		go func() {
			log.Printf("[trace] convergence-trace HTTP on %s (/traces, /lineage)", *traceHTTP)
			if err := http.ListenAndServe(*traceHTTP, mux); err != nil {
				log.Printf("trace-http: %v", err)
			}
		}()
	}

	// gNMI Subscribe server over the cache.
	grpcSrv, err := gnmiserver.New(c)
	if err != nil {
		log.Fatalf("gnmi server: %v", err)
	}
	lis, err := net.Listen("tcp", *gnmiAddr)
	if err != nil {
		log.Fatalf("gnmi listen %s: %v", *gnmiAddr, err)
	}
	go func() {
		log.Printf("[gnmi] Subscribe server on %s (target=%q)", *gnmiAddr, *target)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("gnmi serve: %v", err)
		}
	}()

	// OSPF ingester (created early so the FPM/BMP reconnect hooks can kick it —
	// a dial-in reconnect is FRR's only in-band "I restarted" signal, and syslog
	// is silent across a restart). Started further below.
	ospf := ingest.NewOSPF(c, *ospfReconcile)
	ospf.SetCorrelator(cor)

	// BMP ingester (BGP/L3VPN control plane).
	bmp := ingest.NewBMP(*bmpAddr, c)
	bmp.SetCorrelator(cor)
	bmp.SetReconnectHook(ospf.Kick)
	bmp.SetLineage(lin)
	go func() {
		if err := bmp.Run(); err != nil {
			log.Fatalf("bmp: %v", err)
		}
	}()

	// Netlink ingester (interfaces/VLAN/FDB from the kernel).
	nl := ingest.NewNetlink(c, 10*time.Second)
	nl.SetCorrelator(cor)
	nl.SetLineage(lin)
	go func() {
		if err := nl.Run(); err != nil {
			log.Printf("netlink: %v", err)
		}
	}()

	// LLDP ingester (neighbors via lldpd, if present).
	lldp := ingest.NewLLDP(c)
	go func() {
		if err := lldp.Run(); err != nil {
			log.Printf("lldp: %v", err)
		}
	}()

	// Cgroup ingester (container CPU/memory).
	cg := ingest.NewCgroup(c, 10*time.Second)
	go func() {
		if err := cg.Run(); err != nil {
			log.Printf("cgroup: %v", err)
		}
	}()

	// OSPF ingester (neighbor adjacency via syslog trigger + vtysh reconcile;
	// also kicked by FPM/BMP reconnect + a default-on safety-net ticker).
	go func() {
		if err := ospf.Run(); err != nil {
			log.Printf("ospf: %v", err)
		}
	}()

	// FPM ingester (route/FIB, blocks).
	fpm := ingest.NewFPM(*fpmAddr, c)
	fpm.SetCorrelator(cor)
	fpm.SetReconnectHook(ospf.Kick)
	fpm.SetLineage(lin)
	if err := fpm.Run(); err != nil {
		log.Fatalf("fpm: %v", err)
	}
}
