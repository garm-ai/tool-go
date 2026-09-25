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

	"github.com/garm-ai/garm/contracts/wire"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"

	"github.com/garm-ai/tool-go/toolbind"
)

// Service is a registrar that serves what is registered on it over NATS.
type Service struct {
	name    string
	version string

	mu   sync.Mutex
	eps  map[string]endpoint // by subject
	hash string              // the DescriptorHash every binding agreed on
}

type endpoint struct {
	ref        toolbind.ToolRef
	newRequest func() proto.Message
	handle     toolbind.Handler
}

// New starts an empty service. Generated bindings register onto it, then Run
// serves them.
//
// Version is what a daemon reads back to decide whether this process
// implements the contract it has. Advertising one and implementing another is
// the drift that reconciliation exists to catch, so it should come from the
// build rather than from a literal.
func New(name, version string) *Service {
	// NATS micro requires bare semver and rejects a leading "v", while a Go
	// module version always carries one — so passing the obvious thing, the
	// version of the module being served, would fail at startup with a
	// message about SemVer that does not mention the v. Stripping it is
	// kinder than explaining it.
	return &Service{
		name:    name,
		version: strings.TrimPrefix(version, "v"),
		eps:     map[string]endpoint{},
	}
}

// Endpoint implements toolbind.Registrar.
func (s *Service) Endpoint(ref toolbind.ToolRef, newRequest func() proto.Message, h toolbind.Handler) error {
	if ref.Subject == "" || newRequest == nil || h == nil {
		return fmt.Errorf("registering %s: an endpoint needs a subject, a constructor and a handler", ref.FQN)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.eps[ref.Subject]; dup {
		// Two handlers claiming one subject means the second silently wins.
		// Refuse at startup rather than serve a coin flip.
		return fmt.Errorf("%s is registered twice", ref.Subject)
	}

	// Every binding in one process must agree on the descriptor hash, because
	// the process advertises ONE and a daemon compares it against the
	// catalogue. Two contracts in one service would make that advertisement a
	// half-truth, and the half it omitted is the half that drifted.
	if s.hash == "" {
		s.hash = ref.DescriptorHash
	} else if ref.DescriptorHash != "" && ref.DescriptorHash != s.hash {
		return fmt.Errorf("%s was generated from a different contract than the tools "+
			"already registered (%s vs %s); one process serves one contract",
			ref.FQN, ref.DescriptorHash, s.hash)
	}

	s.eps[ref.Subject] = endpoint{ref: ref, newRequest: newRequest, handle: h}
	return nil
}

// Subject and QueueGroup are re-exported from the contract rather than
// reimplemented here.
//
// They used to be implemented in this file AND in the daemon, identically, in
// different repositories, with nothing keeping them that way. The symptom of
// a divergence is a call that goes nowhere — one side publishing where the
// other never subscribed — and no amount of contract hashing catches it,
// because both sides can agree perfectly about a message while disagreeing
// about where to send it.
var (
	Subject    = wire.Subject
	QueueGroup = wire.QueueGroup
)

// Run serves until ctx is done, then drains.
//
// Drain rather than close: NATS stops delivering new requests to this
// instance while in-flight ones finish. A tool call cut off mid-flight is a
// call whose effect the caller cannot determine, which for anything
// non-idempotent is the worst answer available.
func (s *Service) Run(ctx context.Context, nc *nats.Conn) error {
	s.mu.Lock()
	eps := make(map[string]endpoint, len(s.eps))
	for k, v := range s.eps {
		eps[k] = v
	}
	hash := s.hash
	s.mu.Unlock()

	if len(eps) == 0 {
		return errors.New("no tools registered: a service with nothing to serve is never intended")
	}

	// Advertised so a daemon can reconcile what is running against the
	// catalogue it loaded, from $SRV.INFO rather than from a promise. The key
	// is what garmd's discovery reads into Service.Identity, which it treats
	// as opaque — it compares, it does not interpret.
	var contract string
	for _, e := range eps {
		contract = e.ref.ContractVersion
		break
	}
	cfg := micro.Config{
		Name:        s.name,
		Version:     s.version,
		Description: fmt.Sprintf("%d tool(s)", len(eps)),
		Metadata: map[string]string{
			"garm.identity":         hash,
			"garm.contract_version": contract,
		},
	}
	svc, err := micro.AddService(nc, cfg)
	if err != nil {
		return fmt.Errorf("registering the micro service: %w", err)
	}

	for subject, e := range eps {
		e := e
		// Endpoints go on the service directly, with the full subject.
		//
		// Not via AddGroup: a group PREFIXES the subject it is given, so a
		// group named for the service plus a subject already containing the
		// service yields calc.v1.Calculator.calc.v1.Calculator.Add — a
		// service that starts cleanly and answers nothing.
		//
		// The queue group is set explicitly to the proto service name, which
		// is what makes NATS balance across replicas of one service and only
		// that service.
		if err := svc.AddEndpoint(wire.MicroServiceName(subject),
			micro.HandlerFunc(func(r micro.Request) { s.handle(ctx, e, r) }),
			micro.WithEndpointSubject(subject),
			micro.WithEndpointQueueGroup(e.ref.Service),
		); err != nil {
			return fmt.Errorf("serving %s: %w", e.ref.FQN, err)
		}
	}

	<-ctx.Done()
	return svc.Stop()
}

func (s *Service) handle(ctx context.Context, e endpoint, r micro.Request) {
	req := e.newRequest()
	if err := proto.Unmarshal(r.Data(), req); err != nil {
		// The daemon marshalled this from a descriptor in its catalogue. A
		// failure here means the two are looking at different schemas, which
		// is worth saying plainly rather than reporting as a handler error.
		_ = r.Error("400", "request does not match the contract this service implements", nil)
		return
	}

	resp, err := e.handle(ctx, req)
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
