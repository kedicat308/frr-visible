// Package state holds the OpenConfig gNMI cache — the single state tree that
// every ingester writes into and the gNMI Subscribe server reads from.
// This is the "cache 居中" design: ingesters and subscribers are decoupled.
package state

import (
	"log"
	"time"

	"github.com/openconfig/gnmi/cache"
	"github.com/openconfig/gnmi/ctree"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// Cache wraps openconfig/gnmi cache with a fixed target and a simple Update API.
type Cache struct {
	target string
	c      *cache.Cache
}

// New creates a cache holding a single gNMI target.
func New(target string) *Cache {
	c := cache.New([]string{target})
	return &Cache{target: target, c: c}
}

// Raw exposes the underlying cache for the subscribe server.
func (s *Cache) Raw() *cache.Cache { return s.c }

// Target returns the gNMI target name.
func (s *Cache) Target() string { return s.target }

// SetClient wires cache change notifications to a consumer (the subscribe server).
func (s *Cache) SetClient(fn func(*ctree.Leaf)) { s.c.SetClient(fn) }

// Update pushes leaf updates (and optional deletes) under the given origin.
// Origin lets us keep "openconfig" / "host" / "frr" trees side by side.
func (s *Cache) Update(origin string, ups []*gnmipb.Update, dels []*gnmipb.Path) error {
	n := &gnmipb.Notification{
		Timestamp: time.Now().UnixNano(),
		Prefix:    &gnmipb.Path{Target: s.target, Origin: origin},
		Update:    ups,
		Delete:    dels,
	}
	if err := s.c.GnmiUpdate(n); err != nil {
		log.Printf("[cache] GnmiUpdate error (origin=%q): %v", origin, err)
		return err
	}
	return nil
}
