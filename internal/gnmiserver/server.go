// Package gnmiserver serves gNMI backed by the shared cache. Subscribe is
// delegated to openconfig/gnmi's subscribe.Server; Get and Capabilities are
// implemented here over the same cache. Set stays Unimplemented (config-push
// is a later, higher-risk step).
package gnmiserver

import (
	"context"

	"github.com/openconfig/gnmi/ctree"
	"github.com/openconfig/gnmi/path"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnmi/subscribe"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"frr-visible/internal/state"
)

// Server wraps subscribe.Server (Subscribe) and adds Get + Capabilities.
type Server struct {
	*subscribe.Server // provides Subscribe; Set stays Unimplemented
	cache *state.Cache
}

// New builds a grpc.Server exposing Subscribe + Get + Capabilities over the cache.
func New(c *state.Cache) (*grpc.Server, error) {
	sub, err := subscribe.NewServer(c.Raw())
	if err != nil {
		return nil, err
	}
	c.SetClient(sub.Update) // cache mutations -> live subscribers

	srv := &Server{Server: sub, cache: c}
	g := grpc.NewServer()
	gnmipb.RegisterGNMIServer(g, srv)
	return g, nil
}

// Subscribe defaults the request prefix target to our single cache target when
// the client omits it. The embedded subscribe.Server rejects target-less
// requests ("request must contain a prefix"), but most collectors (gnmic,
// Telegraf) don't set a target — a single-target gNMI box shouldn't require it.
func (s *Server) Subscribe(stream gnmipb.GNMI_SubscribeServer) error {
	return s.Server.Subscribe(&targetDefaultingStream{stream, s.cache.Target()})
}

// targetDefaultingStream wraps the Subscribe server stream and injects our
// cache target into every incoming SubscribeRequest that lacks one.
type targetDefaultingStream struct {
	gnmipb.GNMI_SubscribeServer
	target string
}

func (w *targetDefaultingStream) Recv() (*gnmipb.SubscribeRequest, error) {
	req, err := w.GNMI_SubscribeServer.Recv()
	if err != nil {
		return req, err
	}
	if sub := req.GetSubscribe(); sub != nil {
		if sub.Prefix == nil {
			sub.Prefix = &gnmipb.Path{}
		}
		if sub.Prefix.Target == "" {
			sub.Prefix.Target = w.target
		}
	}
	return req, nil
}

// Capabilities advertises the models and encodings we speak.
func (s *Server) Capabilities(_ context.Context, _ *gnmipb.CapabilityRequest) (*gnmipb.CapabilityResponse, error) {
	return &gnmipb.CapabilityResponse{
		GNMIVersion: "0.10.0",
		SupportedModels: []*gnmipb.ModelData{
			{Name: "openconfig-interfaces", Organization: "OpenConfig"},
			{Name: "openconfig-network-instance", Organization: "OpenConfig"},
			{Name: "openconfig-lldp", Organization: "OpenConfig"},
			// private origins carried alongside OpenConfig
			{Name: "frr", Organization: "frr-visible", Version: "0.1.0"},
			{Name: "host", Organization: "frr-visible", Version: "0.1.0"},
		},
		SupportedEncodings: []gnmipb.Encoding{
			gnmipb.Encoding_JSON_IETF,
			gnmipb.Encoding_PROTO,
		},
	}, nil
}

// Get returns a one-shot snapshot of the requested paths from the cache.
func (s *Server) Get(_ context.Context, req *gnmipb.GetRequest) (*gnmipb.GetResponse, error) {
	prefix := req.GetPrefix()
	target := prefix.GetTarget()
	if target == "" {
		target = s.cache.Target()
	}

	paths := req.GetPath()
	if len(paths) == 0 {
		paths = []*gnmipb.Path{{}} // empty path = whole subtree
	}

	var notifs []*gnmipb.Notification
	for _, p := range paths {
		query, err := path.CompletePath(prefix, p)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if err := s.cache.Raw().Query(target, query, func(_ []string, _ *ctree.Leaf, val interface{}) error {
			if n, ok := val.(*gnmipb.Notification); ok {
				notifs = append(notifs, n)
			}
			return nil
		}); err != nil {
			return nil, status.Error(codes.NotFound, err.Error())
		}
	}
	return &gnmipb.GetResponse{Notification: notifs}, nil
}
