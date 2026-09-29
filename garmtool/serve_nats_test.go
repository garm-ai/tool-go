package garmtool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/garm-ai/contracts/callctx"
	toolv1 "github.com/garm-ai/contracts/garm/tool/v1"
	"github.com/garm-ai/contracts/wire"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// requestWithInvocation is requestWithContext for a test that needs to say
// something about the invocation itself — a deadline, above all. timeout is
// how long the CALLER waits, which is deliberately not the same thing as the
// deadline in the context: shedding happens when those two disagree.
func requestWithInvocation(t *testing.T, nc *nats.Conn, subject string, data []byte, ic *toolv1.InvocationContext, timeout time.Duration) (*nats.Msg, error) {
	t.Helper()
	enc, err := callctx.Encode(ic)
	if err != nil {
		t.Fatalf("encoding the invocation context: %v", err)
	}
	m := nats.NewMsg(subject)
	m.Data = data
	m.Header.Set(callctx.Header, enc)
	return nc.RequestMsg(m, timeout)
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

// A request that queued past its deadline is shed rather than run.
//
// This is the real shape of the thing, not a synthetic expired context: one
// instance, a handler that is busy, a second call queued behind it in the
// subscription's pending queue, and a caller whose deadline is shorter than
// the wait. What the second call must NOT do is run several seconds after the
// caller gave up — for a non-idempotent tool that is an effect nobody will
// ever learn the outcome of.
func TestACallThatQueuedPastItsDeadlineIsShedRatherThanRun(t *testing.T) {
	nc := embeddedNATS(t)

	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	releaseAll := sync.OnceFunc(func() { close(release) })

	var ran atomic.Int64
	block := func(context.Context, proto.Message) (proto.Message, error) {
		ran.Add(1)
		entered <- struct{}{}
		<-release
		return wrapperspb.String("done"), nil
	}

	// One instance, so there is exactly one delivery goroutine and the second
	// call has nowhere to go but the pending queue.
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

	// The first call occupies the instance. Sent from a goroutine, because
	// t.Fatal from a non-test goroutine is not allowed.
	busy := make(chan error, 1)
	go func() {
		_, err := requestWithInvocation(t, nc, subject,
			marshal(t, wrapperspb.String("one")), anInvocation(), 30*time.Second)
		busy <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first request never reached the handler")
	}

	// The second call carries a deadline shorter than the wait it is about to
	// have. The caller's own timeout is much longer, so the reply it sees is
	// the service's answer and not a timeout.
	short := anInvocation()
	short.CallId = "call-2"
	short.Deadline = timestamppb.New(time.Now().Add(300 * time.Millisecond))
	queued := make(chan *nats.Msg, 1)
	go func() {
		msg, err := requestWithInvocation(t, nc, subject,
			marshal(t, wrapperspb.String("two")), short, 30*time.Second)
		if err == nil {
			queued <- msg
		} else {
			queued <- nil
		}
	}()

	// Long enough that the queued call's deadline is certainly gone by the
	// time the instance gets to it.
	time.Sleep(600 * time.Millisecond)
	releaseAll()

	select {
	case msg := <-queued:
		if msg == nil {
			t.Fatal("the queued call got no answer at all")
		}
		if code := msg.Header.Get(micro.ErrorCodeHeader); code != "429" {
			t.Fatalf("code = %q, want 429: a call that outlived its deadline in the "+
				"queue was run anyway", code)
		}
		// The reason, not just the code. A 429 that said "overloaded" could
		// have come from a capacity check this runtime no longer has, and a
		// caller reading the two apart is how an operator tells a backlog
		// from a refusal.
		if got := msg.Header.Get(micro.ErrorHeader); !strings.Contains(got, "deadline") {
			t.Errorf("description = %q, which does not say the deadline was what ran out", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the queued call never came back")
	}

	if err := <-busy; err != nil {
		t.Fatalf("the first call: %v", err)
	}
	if got := ran.Load(); got != 1 {
		t.Errorf("the handler ran %d times, want 1: the shed call reached it anyway", got)
	}
}

// WithConcurrency(n) is n requests at once, in one process.
//
// It is the whole reason this package registers n micro service instances
// rather than one: a handler must reply before returning, nats.go runs one
// delivery goroutine per subscription, so n concurrent requests need n
// subscriptions. If that stopped being true, throughput would silently
// collapse to one request per tool per process and nothing would say so.
func TestWithConcurrencyIsHowManyRequestsRunAtOnce(t *testing.T) {
	const n = 3
	nc := embeddedNATS(t)

	var inFlight atomic.Int64
	var peak atomic.Int64
	all := make(chan struct{})
	var once sync.Once

	overlap := func(context.Context, proto.Message) (proto.Message, error) {
		c := inFlight.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		if c >= n {
			once.Do(func() { close(all) })
		}
		select {
		case <-all:
		case <-time.After(5 * time.Second):
		}
		inFlight.Add(-1)
		return wrapperspb.String("done"), nil
	}

	s := New("calculator", "v0.1.0", WithConcurrency(n))
	if err := s.Endpoint(addRef(), newString, overlap); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)

	// More calls than instances, because NATS picks a queue subscriber per
	// message and picks it at random: exactly n calls can land two on one
	// instance and none on another, which would make this fail on the
	// server's coin flips rather than on the design. Six times n makes every
	// instance busy at once with room to spare, and the surplus simply queues
	// — none of these carry a deadline, so nothing is shed.
	var wg sync.WaitGroup
	for i := 0; i < 6*n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = requestWithInvocation(t, nc, subject,
				marshal(t, wrapperspb.String("x")), anInvocation(), 30*time.Second)
		}()
	}
	wg.Wait()

	if peak.Load() < n {
		t.Errorf("peak concurrency was %d, want %d: the process serialises calls, "+
			"so throughput scales with handler latency and not with what was configured",
			peak.Load(), n)
	}
}

// Each unit of concurrency is a separate responder on $SRV.INFO, and that is
// the cost that decides DefaultConcurrency.
//
// garmd's discovery is a scatter-gather into a channel buffered at 64 that
// drops when full, so the number of responders one process contributes is not
// an implementation detail — it is the thing that makes a large concurrency
// setting break discovery for the whole plane. Anyone tempted to raise the
// default should have to change this test's arithmetic first.
func TestEachUnitOfConcurrencyIsOneMoreDiscoveryResponder(t *testing.T) {
	const n = 3
	nc := embeddedNATS(t)

	s := New("calculator", "v0.1.0", WithConcurrency(n))
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	serve(t, s, nc, wire.Subject(addRoute))

	replies := make(chan *nats.Msg, 64)
	inbox := nats.NewInbox()
	sub, err := nc.ChanSubscribe(inbox, replies)
	if err != nil {
		t.Fatalf("subscribing for discovery replies: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest("$SRV.INFO", inbox, nil); err != nil {
		t.Fatalf("asking for service info: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flushing: %v", err)
	}

	ids := map[string]bool{}
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
collect:
	for {
		select {
		case <-timer.C:
			break collect
		case msg := <-replies:
			var info micro.Info
			if err := json.Unmarshal(msg.Data, &info); err != nil {
				continue
			}
			ids[info.ID] = true
			// Every instance advertises the same contract, so a daemon
			// reconciling sees n replicas and not n different services.
			if info.Metadata["garm.identity"] != addRef().DescriptorHash {
				t.Errorf("instance %s advertises identity %q, not the one the bindings agreed on",
					info.ID, info.Metadata["garm.identity"])
			}
			if len(info.Endpoints) != 1 {
				t.Errorf("instance %s advertises %d endpoints for one tool; discovery reads this "+
					"document to decide what is reachable", info.ID, len(info.Endpoints))
			}
		}
	}

	if len(ids) != n {
		t.Errorf("%d instances answered discovery, want %d", len(ids), n)
	}
}

// micro's own statistics are true again, and this is the test that says so.
//
// service.reqHandler reads req.respondError and stops its clock the moment
// Handle returns. While this package dispatched to a goroutine, that read saw
// a refusal that had not happened yet: every coded refusal was uncounted,
// every ProcessingTime was the cost of a goroutine spawn, and the read itself
// raced the write — the reason this suite runs under -race.
func TestACodedRefusalIsCountedAndTimedInTheServicesStatistics(t *testing.T) {
	const handlerTime = 40 * time.Millisecond
	nc := embeddedNATS(t)

	refuse := func(context.Context, proto.Message) (proto.Message, error) {
		time.Sleep(handlerTime)
		return nil, toolbind.CodedError{Code: "404", Message: "not found"}
	}

	// One instance, so there is one set of statistics to read rather than n.
	s := New("calculator", "v0.1.0", WithConcurrency(1))
	if err := s.Endpoint(addRef(), newString, refuse); err != nil {
		t.Fatalf("registering: %v", err)
	}
	subject := wire.Subject(addRoute)
	serve(t, s, nc, subject)

	if _, err := requestWithContext(t, nc, subject, marshal(t, wrapperspb.String("x"))); err != nil {
		t.Fatalf("calling %s: %v", subject, err)
	}

	msg, err := nc.Request("$SRV.STATS.calculator", nil, 5*time.Second)
	if err != nil {
		t.Fatalf("asking for $SRV.STATS: %v", err)
	}
	var stats micro.Stats
	if err := json.Unmarshal(msg.Data, &stats); err != nil {
		t.Fatalf("$SRV.STATS is not micro.Stats: %v", err)
	}
	if len(stats.Endpoints) != 1 {
		t.Fatalf("%d endpoints in the statistics, want 1", len(stats.Endpoints))
	}
	e := stats.Endpoints[0]

	if e.NumErrors == 0 {
		t.Error("NumErrors is 0 after a coded refusal: the refusal happened after micro " +
			"had already read whether the call failed")
	}
	if !strings.Contains(e.LastError, "404") {
		t.Errorf("LastError = %q, which does not name the code the handler chose", e.LastError)
	}
	if e.ProcessingTime < handlerTime {
		t.Errorf("ProcessingTime = %v for a handler that took %v: the clock stopped before "+
			"the handler did, so every service's reported latency is the cost of dispatch",
			e.ProcessingTime, handlerTime)
	}
}

// The same thing again, under enough contention for the race detector to see
// it rather than infer it.
//
// micro's reqHandler takes the service mutex between Handle returning and its
// read of req.respondError. Uncontended, the read lands before a spawned
// handler could possibly reply and the detector sees an accidental ordering;
// contended — several tools on one service, $SRV.STATS being polled, which is
// an ordinary Tuesday for a real tool service — the read is delayed past the
// reply and the race is real. This is the shape that made tasksd's suite
// unrunnable under -race.
func TestACodedRefusalDoesNotRaceMicrosStatistics(t *testing.T) {
	nc := embeddedNATS(t)

	refuse := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, toolbind.CodedError{Code: "404", Message: "not found"}
	}

	s := New("calculator", "v0.1.0", WithConcurrency(2))
	routes := []string{
		"/calc.v1.Calculator/Add", "/calc.v1.Calculator/Sub",
		"/calc.v1.Calculator/Mul", "/calc.v1.Calculator/Div",
	}
	for _, route := range routes {
		ref := addRef()
		ref.FQN = wire.Subject(route)
		ref.Subject = wire.Subject(route)
		if err := s.Endpoint(ref, newString, refuse); err != nil {
			t.Fatalf("registering %s: %v", route, err)
		}
	}
	serve(t, s, nc, wire.Subject(routes[0]))

	stop := make(chan struct{})
	var pollers sync.WaitGroup
	for i := 0; i < 3; i++ {
		pollers.Add(1)
		go func() {
			defer pollers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = nc.Request("$SRV.STATS.calculator", nil, time.Second)
			}
		}()
	}

	var callers sync.WaitGroup
	for _, route := range routes {
		subject := wire.Subject(route)
		for i := 0; i < 3; i++ {
			callers.Add(1)
			go func() {
				defer callers.Done()
				for j := 0; j < 40; j++ {
					_, _ = requestWithInvocation(t, nc, subject,
						marshal(t, wrapperspb.String("x")), anInvocation(), 5*time.Second)
				}
			}()
		}
	}
	callers.Wait()
	close(stop)
	pollers.Wait()
}

// lockedBuffer is a writer a test goroutine may read while Run's goroutine
// writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The configuration actually in force is logged, defaults included.
//
// Nobody should have to read this package to find out which concurrency a
// process is running with, and the value that most needs saying out loud is
// the one nobody passed: a default that changed between releases is exactly
// the number an operator will otherwise still believe is 16.
func TestTheEffectiveConfigurationIsLoggedAtStartup(t *testing.T) {
	nc := embeddedNATS(t)

	// Locked, because Run writes this from its own goroutine and the test
	// reads it from this one. slog does not synchronise the writer it is
	// handed, and a test that raced on its own fixture would be noise in the
	// detector's output for as long as it lived.
	logged := &lockedBuffer{}
	s := New("calculator", "v0.4.0", WithLogger(slog.New(slog.NewTextHandler(logged, nil))))
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	serve(t, s, nc, wire.Subject(addRoute))

	// serve returns as soon as ONE instance answers, which is before the last
	// of them is registered and so before the line is written. Polling for it
	// rather than sleeping keeps the test fast when it passes and honest when
	// it fails.
	var line string
	deadline := time.Now().Add(5 * time.Second)
	for {
		line = logged.String()
		if strings.Contains(line, "serving tools") || time.Now().After(deadline) {
			break
		}
	}

	for _, want := range []string{
		"service=calculator",
		"version=0.4.0",
		fmt.Sprintf("concurrency=%d", DefaultConcurrency),
		"queue_groups=[" + addRef().Service + "]",
	} {
		if !strings.Contains(line, want) {
			t.Errorf("the startup line does not report %s:\n%s", want, line)
		}
	}
}

// A service given no logger writes nothing, rather than deciding on its own
// that a process's stderr is somewhere it may write.
func TestAServiceWithNoLoggerIsSilent(t *testing.T) {
	nc := embeddedNATS(t)
	s := New("calculator", "v0.4.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	// Nothing to assert but that it serves: the point is that Run has no
	// logger to reach for, and there is no package-level one to fall back to.
	// If one were ever added, this is the test whose name says why not.
	serve(t, s, nc, wire.Subject(addRoute))
	if s.log != nil {
		t.Error("a service constructed without WithLogger acquired a logger from somewhere")
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
