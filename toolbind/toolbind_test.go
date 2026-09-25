// The tests are in toolbind_test so they reach the package the way generated
// code does — through the exported seam and nothing else. An in-package test
// could hold the shapes together by touching what a binding cannot see, which
// would prove the wrong thing.
package toolbind_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/garm-ai/tool-go/toolbind"
)

// This package is almost entirely declarations, and the behaviour worth
// pinning is not in them — it is in whether the shapes still fit the code the
// generator emits against them. So the tests below are a hand-written stand-in
// for generated code: a typed handler an author wrote, adapted into a
// toolbind.Handler and registered on a toolbind.Registrar. If either shape
// drifts, this stops compiling, which is the point — the generator lives in
// another repository and will not notice.

// runtime is the runtime half of the seam, recording rather than serving.
type runtime struct {
	refs  []toolbind.ToolRef
	calls map[string]toolbind.Handler
	news  map[string]func() proto.Message
	err   error
}

func newRuntime() *runtime {
	return &runtime{
		calls: map[string]toolbind.Handler{},
		news:  map[string]func() proto.Message{},
	}
}

func (r *runtime) Endpoint(ref toolbind.ToolRef, newRequest func() proto.Message, h toolbind.Handler) error {
	if r.err != nil {
		return r.err
	}
	r.refs = append(r.refs, ref)
	r.calls[ref.Subject] = h
	r.news[ref.Subject] = newRequest
	return nil
}

// upper is the handler an author writes: concrete types, no protobuf plumbing.
func upper(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if in.GetValue() == "" {
		return nil, errors.New("nothing to upper-case")
	}
	return wrapperspb.String("HELLO"), nil
}

// register is the shape protoc-gen-garm-go emits per tool: the ref as a
// literal, a constructor for the request, and the typed handler wrapped.
func register(r toolbind.Registrar, h func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)) error {
	return r.Endpoint(
		toolbind.ToolRef{
			FQN:             "calc.v1.Calculator.Upper",
			Subject:         "calc.v1.Calculator.Upper",
			Method:          "Upper",
			Service:         "calc.v1.Calculator",
			ContractVersion: "v1.4.0",
			DescriptorHash:  "sha256:abc",
		},
		func() proto.Message { return &wrapperspb.StringValue{} },
		func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return h(ctx, req.(*wrapperspb.StringValue))
		},
	)
}

// The seam has to carry a typed call both ways without either half knowing the
// other's types. A runtime that had to be rebuilt per contract, or a binding
// that had to import a runtime, is the thing this package exists to prevent.
func TestATypedHandlerSurvivesTheRoundTripThroughTheSeam(t *testing.T) {
	rt := newRuntime()
	if err := register(rt, upper); err != nil {
		t.Fatalf("registering: %v", err)
	}

	h, ok := rt.calls["calc.v1.Calculator.Upper"]
	if !ok {
		t.Fatal("the binding registered nothing on its own subject")
	}

	// The runtime allocates through the constructor: it never names the
	// concrete type, which is what lets one binary serve any contract.
	req := rt.news["calc.v1.Calculator.Upper"]()
	if err := proto.Unmarshal(mustMarshal(t, wrapperspb.String("hello")), req); err != nil {
		t.Fatalf("unmarshalling into the constructed request: %v", err)
	}

	resp, err := h(context.Background(), req)
	if err != nil {
		t.Fatalf("the handler: %v", err)
	}
	if got := resp.(*wrapperspb.StringValue).GetValue(); got != "HELLO" {
		t.Errorf("value = %q, want %q", got, "HELLO")
	}
}

// A failing handler comes back as an error, which is the half a runtime acts
// on.
//
// The other half is a trap worth naming here rather than discovering in a
// runtime: the response is NOT a nil interface. The wrapper returns a typed
// nil pointer into a proto.Message, so `resp == nil` is false even though the
// handler returned nothing, and proto.Marshal encodes it to zero bytes
// without an error. Every runtime implementing Registrar has to test
// ProtoReflect().IsValid(), not ==nil, or a handler that returned nothing
// becomes a successful empty reply. garmtool does; this is why.
func TestAFailingTypedHandlerComesBackAsAnErrorAndATypedNil(t *testing.T) {
	rt := newRuntime()
	if err := register(rt, upper); err != nil {
		t.Fatalf("registering: %v", err)
	}

	resp, err := rt.calls["calc.v1.Calculator.Upper"](context.Background(), &wrapperspb.StringValue{})
	if err == nil {
		t.Fatal("a handler that failed reported success")
	}
	if resp != nil && resp.ProtoReflect().IsValid() {
		t.Error("a failed call also produced a sendable response; a runtime would have to guess which to believe")
	}
	// And the trap itself, pinned: it really is the typed nil that arrives, so
	// a runtime checking only ==nil is checking the case that never happens.
	if resp == nil {
		t.Error("the seam yielded a nil interface; if that is now true generally, " +
			"say so where runtimes are told to use IsValid instead")
	}
}

// Register returns an error rather than panicking or logging, so a runtime
// that refuses a binding — a duplicate subject, a second contract — can stop
// the process at startup with a reason. Swallowing it here would turn that
// into a service that starts and serves the wrong half.
func TestARefusalFromTheRuntimeReachesTheBinding(t *testing.T) {
	rt := newRuntime()
	rt.err = errors.New("calc.v1.Calculator.Upper is registered twice")
	if err := register(rt, upper); err == nil {
		t.Fatal("the binding did not surface the runtime's refusal")
	}
}

// Each field is generated from the .proto, so anything missing here is a tool
// a daemon cannot route to, reconcile, or record — and the omission is silent
// at every layer below.
func TestABindingCarriesEverythingADaemonRoutesAndReconcilesOn(t *testing.T) {
	rt := newRuntime()
	if err := register(rt, upper); err != nil {
		t.Fatalf("registering: %v", err)
	}
	ref := rt.refs[0]
	for _, c := range []struct{ what, got string }{
		{"FQN, which a manifest pins and a ledger records", ref.FQN},
		{"Subject, which is where the call lands", ref.Subject},
		{"Service, which is the queue group replicas share", ref.Service},
		{"Method", ref.Method},
		{"ContractVersion", ref.ContractVersion},
		{"DescriptorHash, which is what detects drift", ref.DescriptorHash},
	} {
		if c.got == "" {
			t.Errorf("a binding reached the runtime with no %s", c.what)
		}
	}
	// Subject and Service must agree: NATS balances a subject across the
	// replicas sharing its queue group, and a Service naming some other
	// proto service would spread one tool's calls over a different pool.
	if ref.Subject[:len(ref.Service)] != ref.Service {
		t.Errorf("subject %q is not under service %q", ref.Subject, ref.Service)
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	return b
}
