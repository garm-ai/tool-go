package garmtool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/garm-ai/garm/contracts/callctx"
	toolv1 "github.com/garm-ai/garm/contracts/garm/tool/v1"
	"github.com/garm-ai/garm/contracts/wire"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/garm-ai/tool-go/toolbind"
)

// The runtime is only useful to generated code if it is a Registrar, and the
// generator never imports it — so nothing but this line would notice the
// signature drifting apart from the seam.
var _ toolbind.Registrar = (*Service)(nil)

const addRoute = "/calc.v1.Calculator/Add"

func addRef() toolbind.ToolRef {
	return toolbind.ToolRef{
		FQN:             "calc.v1.Calculator.Add",
		Subject:         wire.Subject(addRoute),
		Method:          "Add",
		Service:         wire.QueueGroup(addRoute),
		ContractVersion: "v1.4.0",
		DescriptorHash:  "sha256:abc",
	}
}

func newString() proto.Message { return &wrapperspb.StringValue{} }

func echo(_ context.Context, req proto.Message) (proto.Message, error) {
	return wrapperspb.String(req.(*wrapperspb.StringValue).GetValue()), nil
}

// recorder stands in for a micro.Request so the handler's decisions can be
// read back without a server. Only what the handler touches is implemented;
// the rest panics rather than returning a zero value, because a silently
// empty answer would make a future test pass for the wrong reason.
type recorder struct {
	data    []byte
	headers micro.Headers

	code, description string
	body              []byte
	errored, answered bool
}

func (r *recorder) Respond(b []byte, _ ...micro.RespondOpt) error {
	r.answered, r.body = true, b
	return nil
}

func (r *recorder) Error(code, description string, _ []byte, _ ...micro.RespondOpt) error {
	r.errored, r.code, r.description = true, code, description
	return nil
}

func (r *recorder) Data() []byte                               { return r.data }
func (r *recorder) RespondJSON(any, ...micro.RespondOpt) error { panic("the handler marshals itself") }
func (r *recorder) Subject() string                            { panic("the handler routes by registration") }
func (r *recorder) Reply() string                              { panic("micro owns the reply subject") }

// Headers is what the runtime reads the invocation context off. A nil map is
// a legitimate state — it is what a caller that set no headers produces — and
// Get on one returns "", which is exactly the input the refusal below exists
// for.
func (r *recorder) Headers() micro.Headers { return r.headers }

// anInvocation is what garmd puts on the wire: assertions about the caller,
// never a credential. call_id is required — callctx.Decode refuses a context
// without one, because code that needs the call id must notice its absence
// rather than ledger an empty string.
func anInvocation() *toolv1.InvocationContext {
	return &toolv1.InvocationContext{
		CallId: "call-1",
		Attribution: &toolv1.CallContext{
			Tenant:        "acme",
			CorrelationId: "corr-1",
		},
		Principal: &toolv1.InvocationPrincipal{
			Subject: "user:jdoe",
			Kind:    toolv1.PrincipalKind_PRINCIPAL_KIND_USER,
		},
	}
}

func invocationHeaders(t *testing.T, ic *toolv1.InvocationContext) micro.Headers {
	t.Helper()
	enc, err := callctx.Encode(ic)
	if err != nil {
		t.Fatalf("encoding the invocation context: %v", err)
	}
	return micro.Headers{callctx.Header: []string{enc}}
}

// call drives the handler the way Run does, with one endpoint and one request
// carrying the invocation context garmd sets on every hop.
func call(t *testing.T, h toolbind.Handler, data []byte) *recorder {
	t.Helper()
	return callWithHeaders(t, h, data, invocationHeaders(t, anInvocation()))
}

func callWithHeaders(t *testing.T, h toolbind.Handler, data []byte, hdr micro.Headers) *recorder {
	t.Helper()
	s := New("calc", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, h); err != nil {
		t.Fatalf("registering: %v", err)
	}
	r := &recorder{data: data, headers: hdr}
	s.handle(context.Background(), s.eps[addRef().Subject], r)
	return r
}

func marshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatalf("marshalling the test request: %v", err)
	}
	return b
}

// Two handlers claiming one subject means the second silently wins, and which
// one that is depends on registration order a generator chose. The failure has
// to arrive at startup, where a person is watching.
func TestTwoHandlersOnOneSubjectAreRefused(t *testing.T) {
	s := New("calc", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("the first registration: %v", err)
	}

	// A different FQN on the same subject, because it is the subject that
	// decides where a request lands — matching on FQN would let a rename slip
	// a second subscriber onto a live route.
	second := addRef()
	second.FQN = "calc.v1.Calculator.Plus"
	err := s.Endpoint(second, newString, echo)
	if err == nil {
		t.Fatal("registering a second handler on one subject was allowed")
	}
	if !strings.Contains(err.Error(), second.Subject) {
		t.Errorf("the error does not name the subject at fault: %v", err)
	}
}

// One process advertises ONE descriptor hash and a daemon reconciles against
// it. Serving two contracts would make that advertisement a half-truth, and
// the half it left out is the half that drifted.
func TestBindingsFromTwoContractsAreRefused(t *testing.T) {
	s := New("calc", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("the first registration: %v", err)
	}

	other := addRef()
	other.FQN = "calc.v1.Calculator.Sub"
	other.Subject = wire.Subject("/calc.v1.Calculator/Sub")
	other.DescriptorHash = "sha256:def"
	err := s.Endpoint(other, newString, echo)
	if err == nil {
		t.Fatal("a binding generated from a different contract was accepted")
	}
	for _, want := range []string{other.FQN, "sha256:def", "sha256:abc"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q, so it does not say which contract to rebuild: %v", want, err)
		}
	}
}

// An unhashed binding is one generated by an older generator, not one from a
// second contract. Refusing it would make upgrading the generator a flag day
// for every service; what must not happen is it overwriting the hash the
// service already advertises.
func TestABindingWithNoHashJoinsWithoutChangingWhatIsAdvertised(t *testing.T) {
	s := New("calc", "v0.1.0")
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("the first registration: %v", err)
	}

	unhashed := addRef()
	unhashed.FQN = "calc.v1.Calculator.Sub"
	unhashed.Subject = wire.Subject("/calc.v1.Calculator/Sub")
	unhashed.DescriptorHash = ""
	if err := s.Endpoint(unhashed, newString, echo); err != nil {
		t.Fatalf("an unhashed binding was refused: %v", err)
	}
	if s.hash != "sha256:abc" {
		t.Errorf("the advertised hash became %q; an unhashed binding erased the one that was known", s.hash)
	}
}

// Generated code passes these, so a nil here means the generator emitted
// something wrong — which is worth catching at registration rather than as a
// nil dereference on the first request of the day.
func TestAnEndpointNeedsASubjectAConstructorAndAHandler(t *testing.T) {
	for _, c := range []struct {
		name       string
		subject    string
		newRequest func() proto.Message
		handler    toolbind.Handler
	}{
		{"no subject", "", newString, echo},
		{"no constructor", wire.Subject(addRoute), nil, echo},
		{"no handler", wire.Subject(addRoute), newString, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			ref := addRef()
			ref.Subject = c.subject
			s := New("calc", "v0.1.0")
			if err := s.Endpoint(ref, c.newRequest, c.handler); err == nil {
				t.Fatalf("%s was accepted", c.name)
			}
		})
	}
}

// NATS micro rejects a leading "v" as invalid SemVer, and every Go module
// version carries one — so the obvious thing to pass, the version of the
// module being served, would fail at startup with a message that never
// mentions the v.
func TestALeadingVIsStrippedFromTheVersion(t *testing.T) {
	if got := New("calc", "v1.2.3").version; got != "1.2.3" {
		t.Errorf("version = %q, which micro refuses to start on", got)
	}
	if got := New("calc", "1.2.3").version; got != "1.2.3" {
		t.Errorf("version = %q; a version that was already bare was altered", got)
	}
}

// A process that serves nothing was configured wrong: a binding was not linked
// in, or its Register was never called. Running happily is how that survives
// to production and shows up as a caller timing out.
func TestRunWithNothingRegisteredIsAnError(t *testing.T) {
	// A nil connection is safe here only because the emptiness check comes
	// first; if that stops being true this test panics rather than passing.
	if err := New("calc", "v0.1.0").Run(context.Background(), (*nats.Conn)(nil)); err == nil {
		t.Fatal("a service with nothing to serve started")
	}
}

// The daemon marshalled the request from a descriptor in its catalogue, so a
// parse failure is the two sides holding different schemas — not this
// handler misbehaving. Reporting it as a handler error sends whoever is
// paged to read the wrong code.
func TestARequestThatDoesNotParseSaysTheContractsDisagree(t *testing.T) {
	r := call(t, echo, []byte{0xff, 0xff, 0xff, 0xff})
	if !r.errored {
		t.Fatal("unparseable bytes were answered as a success")
	}
	if r.code != "400" {
		t.Errorf("code = %q; a contract mismatch is the caller's message, not a service fault", r.code)
	}
	if !strings.Contains(r.description, "contract") {
		t.Errorf("description = %q, which does not point at the contract", r.description)
	}
}

// (nil, nil) would otherwise reply with an empty body that unmarshals into a
// zero-valued response — a success no caller can tell apart from a genuine
// empty result.
func TestAHandlerThatReturnsNothingAtAllIsAnError(t *testing.T) {
	nothing := func(context.Context, proto.Message) (proto.Message, error) { return nil, nil }
	r := call(t, nothing, marshal(t, wrapperspb.String("2")))
	if r.answered {
		t.Fatal("a handler that returned nothing produced a successful reply")
	}
	if r.code != "500" {
		t.Errorf("code = %q, want 500", r.code)
	}
}

// The nil that actually arrives is not the nil the obvious check catches.
//
// Generated code adapts a typed handler by returning its result straight into
// a proto.Message, so a handler that returned (*pb.Reply)(nil) reaches the
// runtime as a NON-nil interface holding a nil pointer — and proto.Marshal
// encodes that to zero bytes with no error. A guard written as `resp == nil`
// passes every direct test and misses every real one, replying success with
// an empty body to a handler that answered nothing.
func TestAHandlerThatReturnsATypedNilIsAnErrorToo(t *testing.T) {
	typedNil := func(context.Context, proto.Message) (proto.Message, error) {
		var resp *wrapperspb.StringValue
		return resp, nil
	}
	r := call(t, typedNil, marshal(t, wrapperspb.String("2")))
	if r.answered {
		t.Fatalf("a nil response was replied to as a success with body %v", r.body)
	}
	if r.code != "500" {
		t.Errorf("code = %q, want 500", r.code)
	}
}

// The handler's own words reach the caller, because a tool failing for a
// reason it can state is the common case and "500" alone is not actionable.
func TestAHandlerErrorReachesTheCallerVerbatim(t *testing.T) {
	failing := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, errors.New("divide by zero")
	}
	r := call(t, failing, marshal(t, wrapperspb.String("2")))
	if r.description != "divide by zero" {
		t.Errorf("description = %q; the handler's reason did not survive", r.description)
	}
	if r.code != "500" {
		t.Errorf("code = %q, want 500", r.code)
	}
}

// A response the handler built but protobuf cannot encode must not become a
// truncated or empty success — the caller would treat it as the answer.
func TestAResponseThatCannotBeEncodedIsAnErrorNotAnEmptyReply(t *testing.T) {
	invalid := func(context.Context, proto.Message) (proto.Message, error) {
		return wrapperspb.String(string([]byte{0xff, 0xfe})), nil
	}
	r := call(t, invalid, marshal(t, wrapperspb.String("2")))
	if r.answered {
		t.Fatal("an unencodable response was replied to as a success")
	}
	if r.code != "500" {
		t.Errorf("code = %q, want 500", r.code)
	}
}

// The handler is handed the parsed request, not the bytes, and its response
// goes back encoded. This is the whole hop; everything else in the file is a
// way for it to go wrong.
func TestTheHandlerSeesTheParsedRequestAndItsResponseIsEncoded(t *testing.T) {
	r := call(t, echo, marshal(t, wrapperspb.String("hello")))
	if !r.answered {
		t.Fatalf("no reply; error was %q %q", r.code, r.description)
	}
	var got wrapperspb.StringValue
	if err := proto.Unmarshal(r.body, &got); err != nil {
		t.Fatalf("the reply is not the response message: %v", err)
	}
	if got.GetValue() != "hello" {
		t.Errorf("value = %q, want %q", got.GetValue(), "hello")
	}
}

// Subject and QueueGroup used to be implemented here AND in the daemon, in
// different repositories, with nothing keeping them identical. Re-exporting
// is the fix; this pins that they are still the contract's own functions and
// not local copies that drifted back in.
func TestTheNamingIsTheContractsAndNotACopy(t *testing.T) {
	if Subject(addRoute) != wire.Subject(addRoute) {
		t.Errorf("Subject diverged from the contract: %q vs %q", Subject(addRoute), wire.Subject(addRoute))
	}
	if QueueGroup(addRoute) != wire.QueueGroup(addRoute) {
		t.Errorf("QueueGroup diverged from the contract: %q vs %q", QueueGroup(addRoute), wire.QueueGroup(addRoute))
	}
}

// garmd sets this header on every hop. A request without one did not come
// through the chain — nobody authenticated it, nobody authorised it, nobody
// will ledger it — and a tool that served it anyway would be the unpoliced
// door the whole architecture exists to remove.
func TestARequestWithNoInvocationContextIsRefusedAndTheHandlerIsNeverCalled(t *testing.T) {
	var called bool
	seen := func(context.Context, proto.Message) (proto.Message, error) {
		called = true
		return wrapperspb.String("x"), nil
	}

	r := callWithHeaders(t, seen, marshal(t, wrapperspb.String("hi")), nil)

	if called {
		t.Error("the handler ran for a request carrying no invocation context")
	}
	if !r.errored {
		t.Fatal("a request with no invocation context was answered as a success")
	}
	if r.code != "400" {
		t.Errorf("code = %q, want 400: the caller sent a malformed request, this "+
			"service did not fail", r.code)
	}
	if r.description != "missing invocation context" {
		t.Errorf("description = %q, want %q", r.description, "missing invocation context")
	}
}

// A header that is present and unreadable is the same refusal as an absent
// one. It is attacker-reachable the moment NATS subject permissions are
// misconfigured, so it must never become a half-built context a handler
// trusts.
func TestAnUndecodableInvocationContextIsRefused(t *testing.T) {
	for _, c := range []struct{ name, value string }{
		{"not base64", "!!! not base64 !!!"},
		{"not a protobuf", "/////////w=="},
		{"no call id", func() string {
			enc, err := callctx.Encode(&toolv1.InvocationContext{
				Principal: &toolv1.InvocationPrincipal{Subject: "user:jdoe"},
			})
			if err != nil {
				t.Fatal(err)
			}
			return enc
		}()},
	} {
		t.Run(c.name, func(t *testing.T) {
			var called bool
			seen := func(context.Context, proto.Message) (proto.Message, error) {
				called = true
				return wrapperspb.String("x"), nil
			}

			r := callWithHeaders(t, seen, marshal(t, wrapperspb.String("hi")),
				micro.Headers{callctx.Header: []string{c.value}})

			if called {
				t.Error("the handler ran for an undecodable invocation context")
			}
			if r.code != "400" || r.description != "missing invocation context" {
				t.Errorf("answered %q %q, want 400 \"missing invocation context\"",
					r.code, r.description)
			}
		})
	}
}

// The context is the whole point of the header: a handler that needs to know
// who it is acting for reads it from ctx, and nowhere else. It never carries
// the caller's token.
func TestTheInvocationContextReachesTheHandler(t *testing.T) {
	var got *toolv1.InvocationContext
	seen := func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		got = callctx.FromContext(ctx)
		return wrapperspb.String("x"), nil
	}

	r := call(t, seen, marshal(t, wrapperspb.String("hi")))

	if !r.answered {
		t.Fatalf("no reply; error was %q %q", r.code, r.description)
	}
	if got == nil {
		t.Fatal("callctx.FromContext returned nothing inside the handler")
	}
	if got.GetCallId() != "call-1" {
		t.Errorf("call_id = %q, want call-1", got.GetCallId())
	}
	if got.GetPrincipal().GetSubject() != "user:jdoe" {
		t.Errorf("subject = %q, want user:jdoe", got.GetPrincipal().GetSubject())
	}
	if got.GetPrincipal().GetKind() != toolv1.PrincipalKind_PRINCIPAL_KIND_USER {
		t.Errorf("kind = %v, want USER", got.GetPrincipal().GetKind())
	}
	if got.GetAttribution().GetCorrelationId() != "corr-1" {
		t.Errorf("correlation_id = %q; the ledger cannot join this call to its caller",
			got.GetAttribution().GetCorrelationId())
	}
}

// The deadline is ABSOLUTE and came from the caller, so it bounds the handler
// rather than restarting on this hop. A handler that outlives it is doing work
// for a caller that has already given up.
func TestTheInvocationDeadlineBoundsTheHandlersContext(t *testing.T) {
	want := time.Now().Add(2 * time.Second)
	ic := anInvocation()
	ic.Deadline = timestamppb.New(want)

	var deadline time.Time
	var ok bool
	seen := func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		deadline, ok = ctx.Deadline()
		return wrapperspb.String("x"), nil
	}

	callWithHeaders(t, seen, marshal(t, wrapperspb.String("hi")), invocationHeaders(t, ic))

	if !ok {
		t.Fatal("the handler's context carried no deadline; the caller's bound was dropped")
	}
	if d := deadline.Sub(want); d > time.Second || d < -time.Second {
		t.Errorf("deadline = %s, want %s", deadline, want)
	}
}

// No deadline on the context is a legitimate state — garmd sets one only when
// its own caller did — and must not become an instantly-expired handler.
func TestAnInvocationWithNoDeadlineLeavesTheHandlerUnbounded(t *testing.T) {
	var ok bool
	seen := func(ctx context.Context, _ proto.Message) (proto.Message, error) {
		_, ok = ctx.Deadline()
		return wrapperspb.String("x"), nil
	}

	call(t, seen, marshal(t, wrapperspb.String("hi")))

	if ok {
		t.Error("a deadline was invented for a call that carried none")
	}
}

// The two-argument form is what every existing service's main calls. Adding
// options must not make it a compile error to upgrade.
func TestTheTwoArgumentConstructorStillCompilesAndGetsTheDefault(t *testing.T) {
	if got := New("calc", "v0.1.0").concurrency; got != DefaultConcurrency {
		t.Errorf("concurrency = %d, want the default %d", got, DefaultConcurrency)
	}
	if got := New("calc", "v0.1.0", WithConcurrency(3)).concurrency; got != 3 {
		t.Errorf("concurrency = %d, want 3", got)
	}
}

// A handler with a more specific answer than "this service broke" can now give
// it. The daemon reads the code off the reply header, so before this every
// failure — a missing account, an argument the tool will never accept — was a
// 500, and the caller could not tell a tool that was broken from a tool that
// had answered.
func TestAHandlerCanChooseTheErrorCode(t *testing.T) {
	missing := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, toolbind.CodedError{Code: "404", Message: "no such account"}
	}

	r := call(t, missing, marshal(t, wrapperspb.String("a-1")))

	if r.answered {
		t.Fatal("a handler error was replied to as a success")
	}
	if r.code != "404" {
		t.Errorf("code = %q, want 404: the handler's own classification was dropped", r.code)
	}
	if r.description != "no such account" {
		t.Errorf("description = %q, want the handler's message", r.description)
	}
}

// Wrapping is the ordinary way a handler adds context. Losing the code there
// would mean the feature works only for handlers that return the error bare,
// which is the half nobody writes.
func TestAWrappedCodedErrorKeepsItsCode(t *testing.T) {
	wrapping := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, fmt.Errorf("looking up the account: %w",
			toolbind.CodedError{Code: "404", Message: "no such account"})
	}

	r := call(t, wrapping, marshal(t, wrapperspb.String("a-1")))

	if r.code != "404" {
		t.Errorf("code = %q, want 404", r.code)
	}
	// The CodedError's own message, not the wrapper's. What the handler chose
	// to publish is the coded part; the context it added is for its own logs.
	if r.description != "no such account" {
		t.Errorf("description = %q, want the coded message", r.description)
	}
}

// Everything else is still a 500 carrying the handler's words. A tool failing
// for a reason it can state is the common case, and "500" alone is not
// actionable — but neither is an invented code.
func TestAnUncodedHandlerErrorIsStillAFiveHundred(t *testing.T) {
	failing := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, errors.New("divide by zero")
	}

	r := call(t, failing, marshal(t, wrapperspb.String("2")))

	if r.code != "500" {
		t.Errorf("code = %q, want 500", r.code)
	}
	if r.description != "divide by zero" {
		t.Errorf("description = %q; the handler's reason did not survive", r.description)
	}
}

// An empty code is a handler mistake, and NATS micro's request.Error refuses
// an empty code outright — it returns an error and never replies at all,
// which would leave the caller hanging until its own deadline rather than
// seeing a failure. 500 is the honest fallback: something went wrong and this
// service did not classify it.
func TestACodedErrorWithNoCodeFallsBackToFiveHundred(t *testing.T) {
	blank := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, toolbind.CodedError{Message: "something went wrong"}
	}

	r := call(t, blank, marshal(t, wrapperspb.String("2")))

	if r.code != "500" {
		t.Errorf("code = %q, want 500: an empty code would make micro refuse to "+
			"reply at all, leaving the caller to hang until its own deadline", r.code)
	}
	if r.description != "something went wrong" {
		t.Errorf("description = %q", r.description)
	}
}

// micro's request.Error refuses an empty description exactly as it refuses
// an empty code — no reply at all, and the caller hangs until its own
// deadline. A handler that named a code but left Message unset must still
// get a reply, so the code stands in for the description too.
func TestACodedErrorWithNoMessageStillGetsAReply(t *testing.T) {
	noMessage := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, toolbind.CodedError{Code: "404"}
	}

	r := call(t, noMessage, marshal(t, wrapperspb.String("a-1")))

	if r.code != "404" {
		t.Errorf("code = %q, want 404", r.code)
	}
	if r.description == "" {
		t.Fatal("description is empty: micro would refuse to send this reply at " +
			"all, leaving the caller to hang until its own deadline")
	}
}

// The same guard applies on the 500 path: a handler error whose Error() is
// empty must not become an empty description, or micro refuses the reply
// outright and the caller hangs rather than sees a failure.
func TestAHandlerErrorWithNoMessageStillGetsAReply(t *testing.T) {
	blank := func(context.Context, proto.Message) (proto.Message, error) {
		return nil, errors.New("")
	}

	r := call(t, blank, marshal(t, wrapperspb.String("2")))

	if r.code != "500" {
		t.Errorf("code = %q, want 500", r.code)
	}
	if r.description == "" {
		t.Fatal("description is empty: micro would refuse to send this reply at " +
			"all, leaving the caller to hang until its own deadline")
	}
}

// Zero and negative are configuration mistakes, and the failure they would
// produce is a service that starts cleanly and answers 429 to everything.
// Ignoring them leaves the default, which is the only safe reading.
func TestANonPositiveConcurrencyLeavesTheDefault(t *testing.T) {
	for _, n := range []int{0, -1} {
		if got := New("calc", "v0.1.0", WithConcurrency(n)).concurrency; got != DefaultConcurrency {
			t.Errorf("WithConcurrency(%d) gave %d, want the default %d",
				n, got, DefaultConcurrency)
		}
	}
}

// dispatch is where the stopping flag is read, so this drives that decision
// directly rather than through a real NATS connection: end to end, the
// window between svc.Stop returning and its subscription drain actually
// taking effect is only as wide as NATS's own UNSUB round trip, which on a
// local connection completes before a second request can be sent — not a
// window a black-box test can reliably land in on demand.
func TestADispatchThatFindsTheServiceStoppingRefusesRatherThanRuns(t *testing.T) {
	s := New("calc", "v0.1.0", WithConcurrency(1))
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}
	s.stopping = true

	slots := make(chan struct{}, 1)
	var inFlight sync.WaitGroup
	r := &recorder{
		data:    marshal(t, wrapperspb.String("x")),
		headers: invocationHeaders(t, anInvocation()),
	}

	s.dispatch(context.Background(), s.eps[addRef().Subject], r, slots, &inFlight)

	if !r.errored {
		t.Fatal("a request dispatched while the service was stopping was answered as a success")
	}
	if r.code != "503" {
		t.Errorf("code = %q, want 503", r.code)
	}
	if r.description != "shutting down" {
		t.Errorf("description = %q, want %q", r.description, "shutting down")
	}
	if len(slots) != 0 {
		t.Error("the slot taken to check stopping was never released")
	}

	// inFlight.Add must never have been called: a WaitGroup nothing was
	// added to returns from Wait immediately, so a Wait that blocks here
	// would mean dispatch added to it despite refusing the request.
	done := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inFlight.Add was called for a request dispatch refused")
	}
}

// The ordinary path through dispatch — not stopping, a free slot — is what
// every other test exercises through Run; this pins that dispatch itself,
// called directly, still runs the handler and both releases happen once it
// returns.
func TestADispatchThatAcquiresRunsTheHandlerAndReleasesBoth(t *testing.T) {
	s := New("calc", "v0.1.0", WithConcurrency(1))
	if err := s.Endpoint(addRef(), newString, echo); err != nil {
		t.Fatalf("registering: %v", err)
	}

	slots := make(chan struct{}, 1)
	var inFlight sync.WaitGroup
	r := &recorder{
		data:    marshal(t, wrapperspb.String("hello")),
		headers: invocationHeaders(t, anInvocation()),
	}

	s.dispatch(context.Background(), s.eps[addRef().Subject], r, slots, &inFlight)

	done := make(chan struct{})
	go func() {
		inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inFlight never reached zero; dispatch's goroutine did not finish or never called Done")
	}

	if !r.answered {
		t.Fatalf("no reply; error was %q %q", r.code, r.description)
	}
	if len(slots) != 0 {
		t.Error("the slot dispatch acquired was never released")
	}
}
