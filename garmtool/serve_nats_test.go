package garmtool

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/garm-ai/contracts/callctx"
	"github.com/garm-ai/contracts/wire"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/garm-ai/tool-go/toolbind"
)

// Three of the bugs this file exists for produced a service that started
// cleanly and answered nothing: a group that double-prefixed every subject, a
// dot in an endpoint name, and a leading "v" on the version. None of them is
// visible from inside the process — only a caller that publishes where the
// contract says to publish can tell the difference. So these run against a
// real server.
//
// Embedded, on a port the kernel picks: assuming a NATS server is running
// makes the suite pass or fail on what else is on the machine, and 4222 in
// particular is usually already taken by someone's local broker.
func embeddedNATS(t *testing.T) *nats.Conn {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("building the embedded server: %v", err)
	}
	go srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("the embedded server never became ready")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("connecting to the embedded server: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// serve runs the service until the test ends and waits until it is actually
// answering. Returning before the endpoints are subscribed would make every
// test below race the goroutine and fail on the timing of the machine.
//
// base is what Run is called with, defaulting to context.Background(); a test
// checking what a caller's ctx carries into a handler supplies its own.
func serve(t *testing.T, s *Service, nc *nats.Conn, probe string, base ...context.Context) {
	t.Helper()
	root := context.Background()
	if len(base) > 0 {
		root = base[0]
	}
	ctx, cancel := context.WithCancel(root)
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, nc) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after its context was cancelled")
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		// ErrNoResponders comes back immediately, so this spins only for as
		// long as the subscription takes to land.
		_, err := nc.Request(probe, nil, 200*time.Millisecond)
		if err == nil || !isNoResponder(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing ever answered on %s", probe)
		}
	}
}

func isNoResponder(err error) bool {
	return err == nats.ErrNoResponders || strings.Contains(err.Error(), "no responders")
}

// requestWithContext publishes the way garmd does: the request bytes plus the
// Garm-Invocation header the chain sets on every hop. Spelling the header out
// in each test would let the two sides drift apart silently.
func requestWithContext(t *testing.T, nc *nats.Conn, subject string, data []byte) (*nats.Msg, error) {
	t.Helper()
	enc, err := callctx.Encode(anInvocation())
	if err != nil {
		t.Fatalf("encoding the invocation context: %v", err)
	}
	m := nats.NewMsg(subject)
	m.Data = data
	m.Header.Set(callctx.Header, enc)
	return nc.RequestMsg(m, 5*time.Second)
}

// The subject a caller publishes to is the one wire.Subject names, spelled
// exactly. A group would have prefixed it with the service name and produced
// calc.v1.Calculator.calc.v1.Calculator.Add, which subscribes fine, starts
// fine, and is reachable by nobody.
//
// The version here deliberately carries the leading "v" a Go module version
// has: micro rejects it as invalid SemVer, so a service that stopped
// stripping it would fail this before it failed anything else.
func TestARequestReachesTheHandlerOnTheSubjectTheContractNames(t *testing.T) {
	nc := embeddedNATS(t)

	s := New("calculator", "v0.4.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)

	msg, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("hello")))
	if err != nil {
		t.Fatalf("calling %s: %v", subject, err)
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
		t.Fatalf("the service answered with an error: %s %s", code, msg.Header.Get(micro.ErrorHeader))
	}
	var got wrapperspb.StringValue
	if err := proto.Unmarshal(msg.Data, &got); err != nil {
		t.Fatalf("the reply is not the response message: %v", err)
	}
	if got.GetValue() != "hello" {
		t.Errorf("value = %q, want %q", got.GetValue(), "hello")
	}
}

// A daemon reconciles what is running against the catalogue it loaded, and it
// does that from $SRV.INFO rather than from a promise. If these keys stop
// being advertised the drift detection does not fail — it stops happening,
// which is the failure nobody sees.
func TestServiceInfoAdvertisesTheIdentityAndTheContractVersion(t *testing.T) {
	nc := embeddedNATS(t)

	s := New("calculator", "v0.4.0")
	ref := addRef()
	if err := s.Endpoint(ref, newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	serve(t, s, nc, wire.Subject(addRoute))

	msg, err := nc.Request("$SRV.INFO.calculator", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("asking for $SRV.INFO: %v", err)
	}
	var info micro.Info
	if err := json.Unmarshal(msg.Data, &info); err != nil {
		t.Fatalf("$SRV.INFO is not micro.Info: %v", err)
	}

	if got := info.Metadata["garm.identity"]; got != ref.DescriptorHash {
		t.Errorf("garm.identity = %q, want %q; a daemon has nothing to compare against", got, ref.DescriptorHash)
	}
	if got := info.Metadata["garm.contract_version"]; got != ref.ContractVersion {
		t.Errorf("garm.contract_version = %q, want %q", got, ref.ContractVersion)
	}
	// The version is what micro stored, so this is the stripped form arriving
	// where a daemon reads it — not just the field New set.
	if info.Version != "0.4.0" {
		t.Errorf("version = %q, want the bare SemVer micro accepts", info.Version)
	}

	if len(info.Endpoints) != 1 {
		t.Fatalf("%d endpoints advertised, want 1", len(info.Endpoints))
	}
	e := info.Endpoints[0]
	if e.Subject != wire.Subject(addRoute) {
		t.Errorf("subject = %q, want %q", e.Subject, wire.Subject(addRoute))
	}
	if strings.Contains(e.Name, ".") {
		t.Errorf("endpoint name %q carries a dot, which micro rejects", e.Name)
	}
	// Replicas of one service share a queue group so NATS balances across
	// them; the default "q" would balance across every unrelated service on
	// the same connection instead.
	if e.QueueGroup != ref.Service {
		t.Errorf("queue group = %q, want %q", e.QueueGroup, ref.Service)
	}
}

// Every endpoint registered gets served, not just the first. A map is iterated
// in a random order, so a loop that stopped early would be intermittent rather
// than absent.
func TestEveryRegisteredToolIsReachable(t *testing.T) {
	nc := embeddedNATS(t)

	s := New("calculator", "v0.4.0")
	routes := []string{"/calc.v1.Calculator/Add", "/calc.v1.Calculator/Sub", "/calc.v1.Calculator/Mul"}
	for _, route := range routes {
		ref := addRef()
		ref.FQN = wire.Subject(route)
		ref.Subject = wire.Subject(route)
		if err := s.Endpoint(ref, newString, echo); err != nil {
			t.Fatalf("registering %s: %v", route, err)
		}
	}
	serve(t, s, nc, wire.Subject(routes[0]))

	for _, route := range routes {
		subject := wire.Subject(route)
		msg, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("x")))
		if err != nil {
			t.Errorf("calling %s: %v", subject, err)
			continue
		}
		if code := msg.Header.Get(micro.ErrorCodeHeader); code != "" {
			t.Errorf("%s answered %s: %s", subject, code, msg.Header.Get(micro.ErrorHeader))
		}
	}
}

// Drain rather than close, so a call cut off mid-flight — whose effect the
// caller cannot determine — does not happen on a routine shutdown. What is
// observable from outside is that the subscription is gone once Run returns:
// a caller is told there is no responder instead of waiting out its timeout.
func TestTheSubjectStopsRespondingOnceRunReturns(t *testing.T) {
	nc := embeddedNATS(t)

	s := New("calculator", "v0.4.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, nc) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := nc.Request(subject, marshal(t, wrapperspb.String("x")), 200*time.Millisecond); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing ever answered on %s", subject)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v; shutting down is not a failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}

	if _, err := nc.Request(subject, marshal(t, wrapperspb.String("x")), 2*time.Second); !isNoResponder(err) {
		t.Errorf("after Run returned the call gave %v; the subscription outlived the service", err)
	}
}

// A service NATS will not accept must come back from Run as an error, so the
// process exits non-zero and an operator sees it.
//
// Both of these are the same shape as the bugs this file was written for — a
// name micro's charset rejects — and the only reason they are not as bad is
// that micro says so out loud. What must not happen is Run swallowing that and
// blocking on ctx.Done() with nothing subscribed: a healthy-looking process
// serving nothing.
func TestAServiceNATSRefusesDoesNotStartQuietly(t *testing.T) {
	nc := embeddedNATS(t)

	t.Run("a service name micro rejects", func(t *testing.T) {
		// The obvious name for a tool service is its proto service, and micro
		// allows no dots in one. Whoever hits this needs to be told at
		// startup, not by a caller timing out.
		s := New("calc.v1.Calculator", "v0.4.0")
		if err := s.Endpoint(addRef(), newString, echo); err != nil {
			t.Fatalf("registering: %v", err)
		}
		err := runBriefly(t, s, nc)
		if err == nil {
			t.Fatal("a service micro cannot name reported a clean run")
		}
		if !strings.Contains(err.Error(), "service") {
			t.Errorf("the error does not say the service itself was refused: %v", err)
		}
	})

	t.Run("a subject micro rejects", func(t *testing.T) {
		s := New("calculator", "v0.4.0")
		ref := addRef()
		ref.Subject = "calc.v1.Calculator has a space"
		if err := s.Endpoint(ref, newString, echo); err != nil {
			t.Fatalf("registering: %v", err)
		}
		err := runBriefly(t, s, nc)
		if err == nil {
			t.Fatal("an unsubscribable subject reported a clean run")
		}
		// Named, because a service with a dozen tools needs to know which one.
		if !strings.Contains(err.Error(), ref.FQN) {
			t.Errorf("the error does not name the tool at fault: %v", err)
		}
	})
}

// runBriefly expects Run to fail before it ever reaches ctx.Done(); the
// cancel is what turns "returned nothing" into a failed test instead of a
// hang the timeout eventually kills.
func runBriefly(t *testing.T, s *Service, nc *nats.Conn) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, nc) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run neither failed nor returned")
		return nil
	}
}

// A malformed request has to be answered, not dropped. A caller that gets
// nothing back cannot tell a contract mismatch from a service that is down,
// and waits out its whole timeout to learn neither.
func TestAMalformedRequestIsAnsweredRatherThanDropped(t *testing.T) {
	nc := embeddedNATS(t)

	s := New("calculator", "v0.4.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)

	// With the invocation context, so the 400 this asserts is the one the
	// malformed body earns. A bare request would now be refused for its
	// missing header before the bytes were ever parsed, and this test would go
	// on passing without exercising what it is named for.
	msg, err := requestWithContext(t, nc, subject, []byte{0xff, 0xff, 0xff, 0xff})
	if err != nil {
		t.Fatalf("calling %s: %v", subject, err)
	}
	if got := msg.Header.Get(micro.ErrorCodeHeader); got != "400" {
		t.Errorf("error code = %q, want 400", got)
	}
	if got := msg.Header.Get(micro.ErrorHeader); !strings.Contains(got, "contract") {
		t.Errorf("error = %q, which does not point at the contract", got)
	}
}

// The unit tests in serve_test.go drive CodedError through handle() directly;
// this is the same behaviour observed the way a caller actually sees it — the
// micro error code header on a reply that came back over a real broker.
func TestAHandlerChosenCodeReachesTheWireAsTheMicroErrorCodeHeader(t *testing.T) {
	nc := embeddedNATS(t)

	notFound := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, toolbind.CodedError{Code: "404", Message: "not found"}
	}

	s := New("calculator", "v0.4.0")
	if err := s.Endpoint(addRef(), newString, notFound); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)

	msg, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("a-1")))
	if err != nil {
		t.Fatalf("calling %s: %v", subject, err)
	}
	if got := msg.Header.Get(micro.ErrorCodeHeader); got != "404" {
		t.Errorf("error code header = %q, want 404: the handler's own classification "+
			"was dropped somewhere between here and the wire", got)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got != "not found" {
		t.Errorf("error header = %q, want %q", got, "not found")
	}
}

// The endpoint sheds rather than queues.
//
// A NATS subscription delivers to its handler sequentially, so before the pool
// existed a blocked handler blocked the endpoint and every caller waited out
// its own timeout — a throughput ceiling that scaled with handler latency
// rather than with the machine, and nothing in the API said so. Shedding is
// the honest answer: the caller learns immediately, and retries or scales the
// service out.
func TestAnEndpointAtCapacityRefusesRatherThanQueues(t *testing.T) {
	nc := embeddedNATS(t)

	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	releaseAll := sync.OnceFunc(func() { close(release) })

	block := func(context.Context, proto.Message) (proto.Message, error) {
		entered <- struct{}{}
		<-release
		return wrapperspb.String("done"), nil
	}

	s := New("calculator", "v0.1.0", WithConcurrency(1))
	if err := s.Endpoint(addRef(), newString, block); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)
	// Registered after serve, so cleanups run in the right order: serve's
	// cleanup cancels Run's context and waits for it to return, which would
	// hang forever if the blocked handler were released only afterwards.
	// t.Cleanup runs LIFO, so registering this one last runs it first.
	t.Cleanup(releaseAll)

	// The first call occupies the only slot. Built here and sent from the
	// goroutine, because t.Fatal from a non-test goroutine is not allowed.
	enc, err := callctx.Encode(anInvocation())
	if err != nil {
		t.Fatal(err)
	}
	busy := nats.NewMsg(subject)
	busy.Data = marshal(t, wrapperspb.String("one"))
	busy.Header.Set(callctx.Header, enc)
	first := make(chan error, 1)
	go func() {
		_, err := nc.RequestMsg(busy, 30*time.Second)
		first <- err
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first request never reached the handler")
	}

	msg, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("two")))
	if err != nil {
		t.Fatalf("the second call got no answer at all: %v", err)
	}
	if code := msg.Header.Get(micro.ErrorCodeHeader); code != "429" {
		t.Fatalf("code = %q, want 429: the endpoint queued behind a busy handler "+
			"instead of shedding", code)
	}
	if got := msg.Header.Get(micro.ErrorHeader); got != "overloaded" {
		t.Errorf("description = %q, want %q", got, "overloaded")
	}
	if len(entered) != 0 {
		t.Error("the refused request still reached the handler")
	}

	// And the endpoint recovers: shedding must not be a state it stays in.
	releaseAll()
	if err := <-first; err != nil {
		t.Fatalf("the first call: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		msg, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("three")))
		if err == nil && msg.Header.Get(micro.ErrorCodeHeader) == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the endpoint never started answering again after the pool drained")
		}
	}
}

// The default applies to the two-argument form every existing service uses, so
// upgrading the runtime is what removes the ceiling — not editing every main.
func TestTheDefaultConcurrencyLetsTwoCallsRunAtOnce(t *testing.T) {
	nc := embeddedNATS(t)

	var inFlight atomic.Int64
	var peak atomic.Int64
	both := make(chan struct{})
	var once sync.Once

	overlap := func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		if n >= 2 {
			once.Do(func() { close(both) })
		}
		select {
		case <-both:
		case <-time.After(5 * time.Second):
		}
		inFlight.Add(-1)
		return wrapperspb.String("done"), nil
	}

	s := New("calculator", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, overlap); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := nats.NewMsg(subject)
			m.Data = marshal(t, wrapperspb.String("x"))
			enc, err := callctx.Encode(anInvocation())
			if err != nil {
				return
			}
			m.Header.Set(callctx.Header, enc)
			_, _ = nc.RequestMsg(m, 30*time.Second)
		}()
	}
	wg.Wait()

	if peak.Load() < 2 {
		t.Errorf("peak concurrency was %d; the endpoint still serialises every call, "+
			"so throughput scales with handler latency and not with the machine",
			peak.Load())
	}
}

// Drain, not drop. A call cut off mid-flight is a call whose effect the caller
// cannot determine, which for anything non-idempotent is the worst answer
// available — so Run must not return while a pooled handler is still running.
func TestRunWaitsForInFlightHandlersBeforeReturning(t *testing.T) {
	nc := embeddedNATS(t)

	started := make(chan struct{})
	finished := make(chan struct{})
	slow := func(context.Context, proto.Message) (proto.Message, error) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		close(finished)
		return wrapperspb.String("done"), nil
	}

	s := New("calculator", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, slow); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, nc) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := nc.Request(subject, nil, 200*time.Millisecond); err == nil || !isNoResponder(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing ever answered on %s", subject)
		}
	}

	go func() {
		m := nats.NewMsg(subject)
		m.Data = marshal(t, wrapperspb.String("x"))
		enc, err := callctx.Encode(anInvocation())
		if err != nil {
			return
		}
		m.Header.Set(callctx.Header, enc)
		_, _ = nc.RequestMsg(m, 30*time.Second)
	}()

	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned")
	}
	select {
	case <-finished:
	default:
		t.Error("Run returned while a handler was still running; the call was cut off mid-flight")
	}
}

// The handlers' context is its own for cancellation, but not a stranger's for
// values: a logger or a tracer a caller attached to Run's ctx must still reach
// a handler, or the only way to get one there would be a package-level global.
type ctxKey struct{}

func TestAValueOnRunsContextReachesTheHandler(t *testing.T) {
	nc := embeddedNATS(t)

	var got any
	seen := func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		got = ctx.Value(ctxKey{})
		return wrapperspb.String("done"), nil
	}

	s := New("calculator", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, seen); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	ctx := context.WithValue(context.Background(), ctxKey{}, "a-logger")
	serve(t, s, nc, subject, ctx)

	if _, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("x"))); err != nil {
		t.Fatalf("calling %s: %v", subject, err)
	}
	if got != "a-logger" {
		t.Errorf("ctx.Value(ctxKey{}) = %v, want %q: a value on Run's ctx did not reach the handler", got, "a-logger")
	}
}

// Handlers are dispatched under a context of their own, not Run's: Run's ctx
// is cancelled the instant shutdown begins, and a handler that is honouring
// ITS context correctly must not be cut off mid-flight just because that one
// was — that would make "drain, not drop" false for the one kind of handler
// it exists to protect.
func TestAHandlersContextIsNotCancelledByRunsShutdown(t *testing.T) {
	nc := embeddedNATS(t)

	started := make(chan struct{})
	sawCancelled := make(chan bool, 1)
	slow := func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		close(started)
		// Long enough that Run's own ctx is certainly already cancelled by
		// the time this reads it.
		time.Sleep(200 * time.Millisecond)
		sawCancelled <- ctx.Err() != nil
		return wrapperspb.String("done"), nil
	}

	s := New("calculator", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, slow); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, nc) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := nc.Request(subject, nil, 200*time.Millisecond); err == nil || !isNoResponder(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing ever answered on %s", subject)
		}
	}

	go func() {
		m := nats.NewMsg(subject)
		m.Data = marshal(t, wrapperspb.String("x"))
		enc, err := callctx.Encode(anInvocation())
		if err != nil {
			return
		}
		m.Header.Set(callctx.Header, enc)
		_, _ = nc.RequestMsg(m, 30*time.Second)
	}()

	<-started
	cancel()

	select {
	case cancelled := <-sawCancelled:
		if cancelled {
			t.Error("the handler's context was already cancelled by Run's shutdown, mid-flight")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never reported its context state")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run never returned")
	}
}
