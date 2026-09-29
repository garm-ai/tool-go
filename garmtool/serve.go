// Package garmtool runs a tool service.
//
// A service written against this handles tool calls and nothing else. It does
// not authenticate, authorise, check input or redact output — the daemon did
// all of that before the request arrived, and doing any of it again here
// would be a second, unreviewed implementation of the chain in the one place
// that must not have one.
//
// What this package owns is the hop: subscribe, decode the invocation
// context, unmarshal, call the handler, marshal, reply. Plus the lifecycle
// around it.
//
// A handler runs SYNCHRONOUSLY, inside the delivery goroutine NATS gives the
// subscription, and has replied by the time the runtime returns to micro.
// Concurrency comes from registering several micro service instances in one
// process — see WithConcurrency — not from spawning a goroutine per request.
// Why, in detail, is on Run.
package garmtool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/garm-ai/contracts/callctx"
	"github.com/garm-ai/contracts/wire"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"

	"github.com/garm-ai/tool-go/toolbind"
)

// Service is a registrar that serves what is registered on it over NATS.
type Service struct {
	name    string
	version string

	// concurrency is how many micro service instances Run registers, and so
	// how many requests this process handles at once per tool. Read once per
	// Run, so a value changed after serving has begun does not apply.
	concurrency int

	// log is where the effective configuration is reported at startup. nil is
	// the default and means silence: a library that writes to a process's
	// stderr uninvited is one every service then has to silence.
	log *slog.Logger

	mu   sync.Mutex
	eps  map[string]endpoint // by subject
	hash string              // the DescriptorHash every binding agreed on

	// stopping is set once Run has begun shutting down, before any instance's
	// Stop is even called. A dispatch that reads it true must not join
	// inFlight: by the time it can be true, Run may already be waiting on
	// inFlight (or past that wait), and an Add reaching it after Wait has
	// started, or after Wait has returned, is the misuse the field exists to
	// prevent.
	//
	// Still load-bearing with synchronous handling. nats.go's
	// Subscription.Drain — which micro's Stop calls — sends UNSUB and returns
	// without waiting (it spawns checkDrained), so the subscription's delivery
	// goroutine goes on handing this runtime whatever was already pending
	// after Stop has returned. Those late deliveries are exactly what this
	// refuses with 503.
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
// # A handler runs synchronously, and why
//
// The obvious shape — spawn a goroutine per request and return — is wrong
// here, and not subtly. nats.go/micro's service.reqHandler calls
// Handler.Handle(req) and then, the moment it returns, reads
// req.respondError to decide whether the call was an error and how long it
// took. A handler that has not replied by the time it returns leaves that
// read looking at a field another goroutine is about to write: a data race
// on every coded refusal, and statistics that say a call took as long as it
// took to spawn a goroutine. micro documents no contract about this either
// way — which is why the pattern looked safe, and why it is written down in
// KNOWN-GAPS.md as an upstream defect rather than only fixed here.
//
// So this replies before returning. Everything that follows is a consequence
// of that.
//
// # Where concurrency comes from
//
// nats.go runs exactly one delivery goroutine per subscription
// (go nc.waitForMsgs(sub), one per subscribe), so one subscription handles
// one request at a time. Concurrency inside a process therefore needs more
// subscriptions, and this registers WithConcurrency(n) separate micro
// service instances, each carrying every endpoint, all in the same queue
// group. The server balances across them, so n requests run at once per
// tool, bounded and with no goroutine of this package's own.
//
// Each instance has its own nuid identity, so to garmd's discovery n
// instances look exactly like n replicas of the service — a case it already
// handles, because a scatter-gather on $SRV.INFO cannot know how many
// instances exist and accumulates whatever answers within its window.
//
// The alternative that also works — n endpoints on ONE micro service, same
// subject, same queue group, since micro's addEndpoint has no duplicate
// check — is rejected for exactly that reason. micro advertises every
// endpoint individually in $SRV.INFO, so n endpoints per tool would put n
// entries for one tool in the one document garmd reads to answer "is this
// tool declared but unreachable". n instances pollute nothing; they add
// responders, which is a cost with a number on it (see DefaultConcurrency).
//
// # Drain rather than close
//
// NATS stops delivering new requests to this process while in-flight ones
// finish. A tool call cut off mid-flight is a call whose effect the caller
// cannot determine, which for anything non-idempotent is the worst answer
// available — so a handler runs under a context of its own, not ctx, and ctx
// being done does not reach it. It gets up to DefaultDrainTimeout to finish
// on its own; only past that deadline is the handlers' context cancelled
// too, and Run returns once they exit.
func (s *Service) Run(ctx context.Context, nc *nats.Conn) error {
	s.mu.Lock()
	eps := make(map[string]endpoint, len(s.eps))
	for k, v := range s.eps {
		eps[k] = v
	}
	hash := s.hash
	instances := s.concurrency
	log := s.log
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

	// One WaitGroup for the whole service: Run must not return while any
	// handler is still running, whichever endpoint or instance it belongs to.
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

	// Every instance that was registered before something failed has live
	// subscriptions on the contract's subjects. Returning an error without
	// stopping them would leave a process that reported a failed start still
	// answering calls — and with n instances it is n of them, not one.
	var svcs []micro.Service
	stopAll := func() {
		for _, svc := range svcs {
			_ = svc.Stop()
		}
	}

	for i := 0; i < instances; i++ {
		svc, err := micro.AddService(nc, cfg)
		if err != nil {
			stopAll()
			return fmt.Errorf("registering the micro service: %w", err)
		}
		svcs = append(svcs, svc)

		for subject, e := range eps {
			e := e
			// Endpoints go on the service directly, with the full subject.
			//
			// Not via AddGroup: a group PREFIXES the subject it is given, so
			// a group named for the service plus a subject already containing
			// the service yields calc.v1.Calculator.calc.v1.Calculator.Add —
			// a service that starts cleanly and answers nothing.
			//
			// The queue group is set explicitly to the proto service name,
			// which is what makes NATS balance across replicas of one service
			// and only that service — and, here, across this process's own
			// instances.
			if err := svc.AddEndpoint(wire.MicroServiceName(subject),
				micro.HandlerFunc(func(r micro.Request) {
					s.dispatch(drainCtx, e, r, &inFlight)
				}),
				micro.WithEndpointSubject(subject),
				micro.WithEndpointQueueGroup(e.ref.Service),
			); err != nil {
				stopAll()
				return fmt.Errorf("serving %s: %w", e.ref.FQN, err)
			}
		}
	}

	// The configuration actually in force, defaults included, so nobody has
	// to read the source to find out which numbers are running. Logged after
	// the subscriptions landed, so the line means "serving" and not
	// "attempting to".
	if log != nil {
		log.Info("serving tools",
			"service", s.name,
			"version", s.version,
			"tools", len(eps),
			"concurrency", instances,
			"queue_groups", queueGroups(eps),
			"identity", hash,
			"contract_version", contract,
		)
	}

	<-ctx.Done()
	// stopping first, under the lock the dispatcher reads it with, and
	// strictly before any Stop: only that order guarantees no dispatch
	// reaching inFlight.Add after this goroutine reaches Wait below.
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	// Stop next, then wait. Stop drains: NATS stops delivering new requests
	// to this process, and the handlers already running finish. Waiting
	// first would be waiting on a queue that is still being fed. Every
	// instance is stopped even if one fails, because a half-stopped process
	// keeps answering on the subjects it did not drain.
	var err error
	for _, svc := range svcs {
		if stopErr := svc.Stop(); stopErr != nil && err == nil {
			err = stopErr
		}
	}

	// Still a wait, and still a timeout, even though handlers are
	// synchronous now. micro's Stop drains each subscription, and nats.go's
	// Subscription.Drain sends UNSUB and returns without waiting for the
	// delivery goroutine to work through what is already pending — it spawns
	// checkDrained and leaves. So a handler can be running, and further
	// pending requests can still be delivered, after every Stop above has
	// returned. inFlight is what makes Run outlive them.
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

// queueGroups is the distinct set of queue groups this process joins, for the
// startup line. Usually one — a process serves one proto service — but a
// binding set with two would otherwise report only whichever the map handed
// back first.
func queueGroups(eps map[string]endpoint) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range eps {
		if e.ref.Service == "" || seen[e.ref.Service] {
			continue
		}
		seen[e.ref.Service] = true
		out = append(out, e.ref.Service)
	}
	sort.Strings(out)
	return out
}

// dispatch is the gate in front of handle: refuse if the service is already
// stopping, otherwise run the handler to completion and account for it.
//
// Synchronous, deliberately. micro reads req.respondError the instant this
// returns, so the reply must already have been sent — see Run. The cost is
// that this subscription's delivery goroutine is busy until the handler is
// done, which is what WithConcurrency buys its way out of.
//
// Pulled out of Run's HandlerFunc closure so the stopping race has a seam a
// test can drive directly — through Run and a real NATS connection, the
// window this closes is open only for as long as NATS's own subscription
// drain takes to reach the server, which on a local connection is not
// reliably reproducible on demand.
func (s *Service) dispatch(ctx context.Context, e endpoint, r micro.Request, inFlight *sync.WaitGroup) {
	// NATS's own Subscription.Drain (inside micro's Stop, in Run) sends
	// UNSUB and returns without waiting for it to take effect, so this can
	// still run after every Stop has returned and Run is already waiting on
	// inFlight, or has already stopped waiting. stopping is set, under this
	// same lock, before any Stop is called, so a dispatch that reads it true
	// here never calls Add — closing that window rather than racing it.
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		_ = r.Error("503", "shutting down", nil)
		return
	}
	inFlight.Add(1)
	s.mu.Unlock()
	defer inFlight.Done()

	s.handle(ctx, e, r)
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
		if !time.Now().Before(d.AsTime()) {
			// Load shedding, and the only form of it this runtime has left.
			//
			// There used to be a slot pool, and a request arriving to find it
			// full was answered 429 immediately. With one request at a time
			// per subscription there is no pool to be full: a request either
			// is being handled or is sitting in the subscription's pending
			// queue, and nothing in nats.go's or micro's public API tells a
			// handler how deep that queue is. So capacity is no longer
			// something this can observe.
			//
			// What it CAN observe is the consequence. A request whose
			// absolute deadline has already passed before this process even
			// reached it waited somewhere, and on this hop the only place to
			// wait is that pending queue. Running it would spend a whole
			// slot's worth of capacity — and, for a non-idempotent tool, take
			// a real effect — on behalf of a caller that has already given
			// up. Refusing it is what lets a backlog clear instead of
			// compounding.
			//
			// 429 rather than 504, because what the operator needs to see in
			// garmd's counters is a service that is behind, and garmd already
			// reads 429 as resource exhaustion. Be honest about who sees it,
			// though: the caller that sent THIS request has usually stopped
			// listening, so the reply is mostly for the record. The value is
			// the work not done.
			//
			// And the honest limit: an invocation with no deadline cannot be
			// shed this way, so a plane whose callers set none has no shedding
			// at all. Backlog then grows in the subscription's pending queue
			// until NATS drops it as a slow consumer, with no reply, and the
			// caller learns from its own timeout. KNOWN-GAPS.md records that.
			_ = r.Error("429", "overloaded: queued past its deadline", nil)
			return
		}
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
