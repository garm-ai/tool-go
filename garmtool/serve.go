// Package garmtool runs a tool service.
//
// A service written against this handles tool calls and nothing else. It does
// not authenticate, authorise, check input or redact output — the daemon did
// all of that before the request arrived, and doing any of it again here
// would be a second, unreviewed implementation of the chain in the one place
// that must not have one.
//
// What this package owns is the hop: subscribe, unmarshal, call the handler,
// marshal, reply. Plus the lifecycle around it.
package garmtool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"

	"github.com/garm-ai/tool-go/toolbind"
)

// Service is a registrar that serves what is registered on it over NATS.
type Service struct {
	name    string
	version string

	mu      sync.Mutex
	methods map[string]toolbind.Method // by FullMethod
}

// New starts an empty service. Generated bindings register onto it, then Run
// serves them.
//
// Version is what a daemon reads back to decide whether this process
// implements the contract it has. Advertising one and implementing another is
// the drift that reconciliation exists to catch, so it should come from the
// build rather than from a literal.
func New(name, version string) *Service {
	return &Service{name: name, version: version, methods: map[string]toolbind.Method{}}
}

// Register implements toolbind.Registrar.
func (s *Service) Register(service string, m toolbind.Method) error {
	if m.FullMethod == "" || m.NewRequest == nil || m.Handle == nil {
		return fmt.Errorf("registering %s: a method needs a route, a constructor and a handler", service)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.methods[m.FullMethod]; dup {
		// Registering twice means two handlers claim one route and the second
		// silently wins. Refuse at startup rather than serve a coin flip.
		return fmt.Errorf("%s is registered twice", m.FullMethod)
	}
	s.methods[m.FullMethod] = m
	return nil
}

// Subject is the NATS subject a route is served on.
//
// The FullMethod with its separators swapped for dots, so the subject and the
// route are the same string in two syntaxes and neither side has a mapping
// table to get wrong.
func Subject(fullMethod string) string {
	return strings.ReplaceAll(strings.TrimPrefix(fullMethod, "/"), "/", ".")
}

// QueueGroup is what replicas of a service share so NATS balances across them.
// The proto service name: every replica of one service, and nothing else.
func QueueGroup(fullMethod string) string {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	if i := strings.Index(trimmed, "/"); i >= 0 {
		return trimmed[:i]
	}
	return trimmed
}

// Run serves until ctx is done, then drains.
//
// Drain rather than close: NATS stops delivering new requests to this
// instance while in-flight ones finish. A tool call cut off mid-flight is a
// call whose effect the caller cannot determine, which for anything
// non-idempotent is the worst answer available.
func (s *Service) Run(ctx context.Context, nc *nats.Conn) error {
	s.mu.Lock()
	methods := make(map[string]toolbind.Method, len(s.methods))
	for k, v := range s.methods {
		methods[k] = v
	}
	s.mu.Unlock()

	if len(methods) == 0 {
		return errors.New("no tools registered: a service with nothing to serve is never intended")
	}

	cfg := micro.Config{
		Name:        s.name,
		Version:     s.version,
		Description: fmt.Sprintf("%d tool(s)", len(methods)),
	}
	svc, err := micro.AddService(nc, cfg)
	if err != nil {
		return fmt.Errorf("registering the micro service: %w", err)
	}

	for route, m := range methods {
		m := m
		group := svc.AddGroup(QueueGroup(route))
		endpoint := lastSegment(route)
		if err := group.AddEndpoint(endpoint,
			micro.HandlerFunc(func(r micro.Request) { s.handle(ctx, m, r) }),
			micro.WithEndpointSubject(Subject(route)),
		); err != nil {
			return fmt.Errorf("serving %s: %w", route, err)
		}
	}

	<-ctx.Done()
	return svc.Stop()
}

func (s *Service) handle(ctx context.Context, m toolbind.Method, r micro.Request) {
	req := m.NewRequest()
	if err := proto.Unmarshal(r.Data(), req); err != nil {
		// The daemon marshalled this from a descriptor in its catalogue. A
		// failure here means the two are looking at different schemas, which
		// is worth saying plainly rather than reporting as a handler error.
		_ = r.Error("400", "request does not match the contract this service implements", nil)
		return
	}

	resp, err := m.Handle(ctx, req)
	if err != nil {
		_ = r.Error("500", err.Error(), nil)
		return
	}
	if resp == nil {
		// Returning (nil, nil) would otherwise reply with an empty body the
		// daemon would unmarshal into a zero-valued response — a successful
		// call that returns nothing, which no caller can distinguish from a
		// genuine empty result.
		_ = r.Error("500", "handler returned no response and no error", nil)
		return
	}

	body, err := proto.Marshal(resp)
	if err != nil {
		_ = r.Error("500", "response could not be marshalled", nil)
		return
	}
	_ = r.Respond(body)
}

func lastSegment(route string) string {
	if i := strings.LastIndex(route, "/"); i >= 0 {
		return route[i+1:]
	}
	return route
}
