// Package garmtool runs a tool service.
//
// A service written against this handles tool calls and nothing else. It does
// not authenticate, authorise, check input or redact output — the daemon did
// all of that before the request arrived, and doing any of it again here
// would be a second, unreviewed implementation of the chain in the one place
// that must not have one.
//
// What this package owns is the hop: subscribe, decode the invocation
// context, unmarshal, call the handler, marshal, reply, behind a bounded
// worker pool per endpoint. Plus the lifecycle around it.
package garmtool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/garm-ai/garm/contracts/callctx"
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

	// concurrency bounds each endpoint's pool. Read once per Run, so a value
	// changed after serving has begun does not apply to a live endpoint.
	concurrency int

	mu   sync.Mutex
	eps  map[string]endpoint // by subject
	hash string              // the DescriptorHash every binding agreed on

	// stopping is set once Run has begun shutting down, before svc.Stop is
	// even called. A dispatch that reads it true must not join inFlight: by
	// the time it can be true, Run may already be waiting on inFlight (or
	// past that wait), and an Add reaching it after Wait has started, or
	// after Wait has returned, is the misuse the field exists to prevent.
	stopping bool
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
//
// A Service is single-use: Run sets stopping once shutdown begins and never
// clears it, so a second Run on the same Service would refuse every request
// immediately rather than serve. Call New again for a second run.
func New(name, version string, opts ...Option) *Service {
	// NATS micro requires bare semver and rejects a leading "v", while a Go
	// module version always carries one — so passing the obvious thing, the
	// version of the module being served, would fail at startup with a
	// message about SemVer that does not mention the v. Stripping it is
	// kinder than explaining it.
	s := &Service{
		name:        name,
		version:     strings.TrimPrefix(version, "v"),
		eps:         map[string]endpoint{},
		concurrency: DefaultConcurrency,
	}
	for _, o := range opts {
		o(s)
	}
	return s
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
// non-idempotent is the worst answer available — so a dispatched handler runs
// under a context of its own, not ctx, and ctx being done does not reach it.
// It gets up to DefaultDrainTimeout to finish on its own; only past that
// deadline is the handlers' context cancelled too, and Run returns once they
// exit.
func (s *Service) Run(ctx context.Context, nc *nats.Conn) error {
	s.mu.Lock()
	eps := make(map[string]endpoint, len(s.eps))
	for k, v := range s.eps {
		eps[k] = v
	}
	hash := s.hash
	limit := s.concurrency
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

	// One WaitGroup for the whole service: Run must not return while any
	// handler is still running, whichever endpoint it belongs to.
	var inFlight sync.WaitGroup

	// A context of the handlers' own, not ctx: ctx is done the instant
	// shutdown starts, and if handlers ran under it directly a
	// context-respecting one would be cut off mid-flight the moment
	// shutdown began, making the "drain, not drop" promise false for
	// exactly the handler it was meant to protect. Cancelled only once the
	// drain has had DefaultDrainTimeout to finish on its own, or sooner if
	// it does.
	//
	// WithoutCancel(ctx) rather than Background(): this must not inherit
	// ctx's cancellation — that is the whole point above — but it should
	// still carry whatever values a caller attached to Run's ctx, a logger or
	// a tracer, so a handler running during drain sees the same values one
	// running before shutdown began would have.
	drainCtx, cancelDrain := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelDrain()

	for subject, e := range eps {
		e := e
		// A slot per endpoint. Buffered, and taken with a non-blocking send:
		// a blocking one would queue behind a busy handler, which is the
		// ceiling this pool exists to remove rather than to move.
		slots := make(chan struct{}, limit)

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
			micro.HandlerFunc(func(r micro.Request) {
				s.dispatch(drainCtx, e, r, slots, &inFlight)
			}),
			micro.WithEndpointSubject(subject),
			micro.WithEndpointQueueGroup(e.ref.Service),
		); err != nil {
			return fmt.Errorf("serving %s: %w", e.ref.FQN, err)
		}
	}

	<-ctx.Done()
	// stopping first, under the lock the dispatcher reads it with, and
	// strictly before Stop: only that order guarantees no dispatch reaching
	// inFlight.Add after this goroutine reaches Wait below.
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	// Stop next, then wait. Stop drains: NATS stops delivering new requests
	// to this instance, and the handlers already running finish. Waiting
	// first would be waiting on a queue that is still being fed.
	err = svc.Stop()

	drained := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(DefaultDrainTimeout):
	}
	// Cancelling here is a no-op if the drain already finished (drainCtx has
	// nothing left to bound) and forces the rest to return if it did not.
	// Either way, this Wait is for whatever the deadline left running.
	cancelDrain()
	inFlight.Wait()
	return err
}

// dispatch is one endpoint's gate in front of handle: acquire a slot, refuse
// if the pool is full or the service is already stopping, otherwise run the
// handler in a tracked goroutine and release both when it returns.
//
// Pulled out of Run's HandlerFunc closure so the stopping race has a seam a
// test can drive directly — through Run and a real NATS connection, the
// window this closes is open only for as long as NATS's own subscription
// drain takes to reach the server, which on a local connection is not
// reliably reproducible on demand.
func (s *Service) dispatch(ctx context.Context, e endpoint, r micro.Request, slots chan struct{}, inFlight *sync.WaitGroup) {
	select {
	case slots <- struct{}{}:
	default:
		// Shed, immediately. A caller told "overloaded" retries or scales
		// the service out; a caller left waiting learns the same thing from
		// its own timeout, minutes later, with no way to tell an overloaded
		// service from a hung one.
		_ = r.Error("429", "overloaded", nil)
		return
	}

	// NATS's own Subscription.Drain (inside svc.Stop, in Run) sends UNSUB
	// and returns without waiting for it to take effect, so this can still
	// run after Stop has returned and Run is already waiting on inFlight,
	// or has already stopped waiting. stopping is set, under this same
	// lock, before Stop is even called, so a dispatch that reads it true
	// here never calls Add — closing that window rather than racing it.
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		<-slots
		_ = r.Error("503", "shutting down", nil)
		return
	}
	inFlight.Add(1)
	s.mu.Unlock()

	go func() {
		defer inFlight.Done()
		defer func() { <-slots }()
		s.handle(ctx, e, r)
	}()
}

// handle is one request: the invocation context first, then the message.
//
// The context comes first because it decides whether there is a request at
// all. garmd sets Garm-Invocation on every hop (program plan §3.5), so a
// request without one did not come through the chain, and unmarshalling it
// before refusing would only be work done for a caller nobody identified.
func (s *Service) handle(ctx context.Context, e endpoint, r micro.Request) {
	ic, err := callctx.Decode(r.Headers().Get(callctx.Header))
	if err != nil {
		// One message for absent and for malformed, deliberately. Telling a
		// caller which of the two it got says whether the header name was
		// right, which is the first thing anyone probing would want to know.
		// The detail is the daemon's to log, not this service's to publish.
		_ = r.Error("400", "missing invocation context", nil)
		return
	}
	ctx = callctx.NewContext(ctx, ic)
	// Absolute, so it does not restart on this hop. A relative deadline
	// re-based here would let a chain of three hops take three times what the
	// caller allowed.
	if d := ic.GetDeadline(); d != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d.AsTime())
		defer cancel()
	}

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
		// The handler's own classification, when it gave one. errors.As
		// rather than a type assertion, so a handler that wrapped its error
		// with %w to add context for its own logs still publishes the code it
		// chose.
		var coded toolbind.CodedError
		if errors.As(err, &coded) && coded.Code != "" {
			// The CODED message, not the wrapper's text: what the handler
			// chose to publish is this, and the context it wrapped around it
			// is for its own logs.
			msg := coded.Message
			if msg == "" {
				// micro's request.Error refuses an empty description just as
				// it refuses an empty code — it returns an error and never
				// replies at all. A reply must always be sent, so a handler
				// that named a code but no message gets the code back as its
				// own description rather than leaving the caller to hang.
				msg = coded.Code
			}
			_ = r.Error(coded.Code, msg, nil)
			return
		}
		// Unclassified — including a CodedError with an empty code. micro's
		// request.Error refuses an empty code outright, returning an error
		// and never replying, so treating one as coded here would leave the
		// caller with no reply at all and a hang until its own deadline,
		// not a 500.
		desc := err.Error()
		if desc == "" {
			// The same refusal applies to an empty description on the 500
			// path: an error whose Error() is "" must still produce a reply.
			desc = "internal error"
		}
		_ = r.Error("500", desc, nil)
		return
	}
	if resp == nil || !resp.ProtoReflect().IsValid() {
		// Returning (nil, nil) would otherwise reply with an empty body the
		// daemon would unmarshal into a zero-valued response — a successful
		// call that returns nothing, which no caller can distinguish from a
		// genuine empty result.
		//
		// IsValid as well as == nil, because those are two different nils and
		// the one that actually reaches here is the second. Generated code
		// adapts a typed handler by returning its result straight into a
		// proto.Message, so a handler that returned (*pb.Reply)(nil) arrives
		// as a NON-nil interface holding a nil pointer — which proto.Marshal
		// encodes to zero bytes without complaining.
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
