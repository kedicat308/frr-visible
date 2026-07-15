// Command subtest is a tiny gNMI Subscribe client used to verify the shim.
// It sets prefix.target so it matches the cache target, subscribes to the
// network-instances subtree, and prints notifications.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("a", "127.0.0.1:9339", "gNMI server address")
	target := flag.String("target", "frr", "gNMI prefix target")
	origin := flag.String("origin", "openconfig", "gNMI prefix origin")
	path := flag.String("path", "network-instances", "subscribe path (slash-separated, empty=root)")
	once := flag.Bool("once", false, "ONCE mode (snapshot then exit)")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sub, err := gnmipb.NewGNMIClient(conn).Subscribe(ctx)
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}

	mode := gnmipb.SubscriptionList_STREAM
	if *once {
		mode = gnmipb.SubscriptionList_ONCE
	}
	if err := sub.Send(&gnmipb.SubscribeRequest{
		Request: &gnmipb.SubscribeRequest_Subscribe{
			Subscribe: &gnmipb.SubscriptionList{
				Prefix: &gnmipb.Path{Target: *target, Origin: *origin},
				Mode:   mode,
				Subscription: []*gnmipb.Subscription{{
					Path: &gnmipb.Path{Elem: parsePath(*path)},
				}},
			},
		},
	}); err != nil {
		log.Fatalf("send: %v", err)
	}

	updates, deletes := 0, 0
	for {
		resp, err := sub.Recv()
		if err != nil {
			log.Printf("recv end: %v", err)
			return
		}
		switch r := resp.Response.(type) {
		case *gnmipb.SubscribeResponse_Update:
			for _, u := range r.Update.Update {
				updates++
				fmt.Printf("UPDATE %s = %s\n", pathStr(r.Update.Prefix, u.Path), valStr(u.Val))
			}
			for _, d := range r.Update.Delete {
				deletes++
				fmt.Printf("DELETE %s\n", pathStr(r.Update.Prefix, d))
			}
		case *gnmipb.SubscribeResponse_SyncResponse:
			fmt.Printf("--- sync_response (updates=%d deletes=%d) ---\n", updates, deletes)
			if *once {
				return
			}
		}
	}
}

func parsePath(s string) []*gnmipb.PathElem {
	var elems []*gnmipb.PathElem
	for _, part := range strings.Split(s, "/") {
		if part == "" {
			continue
		}
		elems = append(elems, &gnmipb.PathElem{Name: part})
	}
	return elems
}

func pathStr(prefix, p *gnmipb.Path) string {
	var b strings.Builder
	if prefix != nil && prefix.Origin != "" {
		b.WriteString(prefix.Origin + ":")
	}
	for _, e := range append(prefixElems(prefix), p.GetElem()...) {
		b.WriteString("/" + e.Name)
		for k, v := range e.Key {
			b.WriteString(fmt.Sprintf("[%s=%s]", k, v))
		}
	}
	return b.String()
}

func prefixElems(p *gnmipb.Path) []*gnmipb.PathElem {
	if p == nil {
		return nil
	}
	return p.GetElem()
}

func valStr(v *gnmipb.TypedValue) string {
	if v == nil {
		return "<nil>"
	}
	switch t := v.Value.(type) {
	case *gnmipb.TypedValue_StringVal:
		return t.StringVal
	case *gnmipb.TypedValue_UintVal:
		return fmt.Sprintf("%d", t.UintVal)
	case *gnmipb.TypedValue_IntVal:
		return fmt.Sprintf("%d", t.IntVal)
	default:
		return fmt.Sprintf("%v", v.Value)
	}
}
