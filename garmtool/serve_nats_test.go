package garmtool

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/garm-ai/garm/contracts/wire"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
func serve(t *testing.T, s *Service, nc *nats.Conn, probe string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
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

	msg, err := nc.Request(subject, marshal(t, wrapperspb.String("hello")), 5*time.Second)
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
		if _, err := nc.Request(subject, marshal(t, wrapperspb.String("x")), 5*time.Second); err != nil {
			t.Errorf("calling %s: %v", subject, err)
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

	msg, err := nc.Request(subject, []byte{0xff, 0xff, 0xff, 0xff}, 5*time.Second)
	if err != nil {
		t.Fatalf("calling %s: %v", subject, err)
	}
	if got := msg.Header.Get(micro.ErrorCodeHeader); got != "400" {
		t.Errorf("error code = %q, want 400", got)
	}
}
