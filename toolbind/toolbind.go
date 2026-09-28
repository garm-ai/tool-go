// Package toolbind is the seam a generated tool binding registers against.
//
// Generated code calls Endpoint; a runtime implements Registrar. Neither
// knows the other, which is the point: a binding must not import a runtime,
// or choosing a different one later would mean regenerating.
//
// The shapes here are dictated by what protoc-gen-garm-go emits. This package
// follows the generator rather than the other way round — a runtime that
// invents its own registration API just moves the adapter into every tool
// service that uses it.
//
// Deliberately tiny and dependency-free beyond protobuf. Every tool service
// anyone writes links this, so whatever is added here is added to all of them.
package toolbind

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Handler executes one tool call.
//
// Messages rather than typed arguments, because generated code adapts the
// typed handler an author wrote into this shape. A runtime never needs to know
// the concrete types, which is what lets it serve any contract without being
// rebuilt for it.
type Handler func(ctx context.Context, req proto.Message) (proto.Message, error)

// ToolRef identifies one tool, as its contract declares it.
//
// Everything here is generated from the .proto, so a service and the daemon
// calling it cannot disagree about any of it without one of them having been
// built from a different contract — which is exactly what DescriptorHash
// exists to detect.
type ToolRef struct {
	// FQN is the tool's identity: proto package plus tool name. What a
	// manifest pins and what a ledger records.
	FQN string

	// Subject is where this tool is reachable — the route in dotted form, so
	// neither side keeps a mapping table that could drift from the other's.
	Subject string

	// Method and Service are the proto names. Service is also the queue group
	// replicas share, so NATS balances across instances of one service.
	Method  string
	Service string

	// ContractVersion is the version of the contracts artifact this binding
	// was generated from.
	//
	// DescriptorHash covers the wire shape and nothing else — not comments,
	// not policy annotations — so it changes when a caller would need to care
	// and stays put when only prose did.
	//
	// A runtime advertises both, and a daemon refuses to route to a service
	// whose hash does not match the catalogue it loaded. Published
	// declarations put the contract in one artifact and the handler in
	// another binary, and without this, agreement between them is convention.
	ContractVersion string
	DescriptorHash  string
}

// Registrar is what a runtime provides and a generated binding consumes.
type Registrar interface {
	// Endpoint adds one tool. Called once per tool by generated code, before
	// serving begins.
	//
	// newRequest allocates an empty request for the runtime to unmarshal
	// into: a constructor rather than a descriptor, because the binding knows
	// the concrete type and the runtime should not have to.
	Endpoint(ref ToolRef, newRequest func() proto.Message, h Handler) error
}

// CodedError is how a handler chooses the error code its caller sees.
//
// It is the ONLY way. A runtime reads the code off this and off nothing else:
// there is no registry of error types, no mapping table, and no inspection of
// an error's text. Returning any other error means "500" plus that error's own
// words, which is the right answer for a tool that broke and the wrong one for
// a tool that answered.
//
// It lives HERE, in the seam, rather than in a runtime, because a generated
// binding must be able to name a code without importing one — that is the
// whole reason this package exists, and a handler that had to import garmtool
// to say "404" would make choosing a different runtime later mean
// regenerating.
//
// The codes are the transport's, not a taxonomy of this package's invention:
// they are strings because NATS micro's error code header is a string, and
// which strings mean what is the daemon's contract with its callers. "404" and
// "400" are the two a tool usually needs — the thing asked about does not
// exist, and the arguments will never be acceptable — and both are answers
// rather than failures.
//
// A value type with a value receiver, so `errors.As(err, &coded)` finds one
// whether it was returned bare or wrapped with %w. Wrapping is the ordinary
// way a handler adds context for its own logs, and a code that survived only
// the bare form would work for the half of the handlers nobody writes.
//
// Return it by value, not as *CodedError: errors.As(err, &coded) with a
// CodedError target matches a CodedError, bare or wrapped, but not a
// *CodedError — a handler that returned one of those would find its code
// silently dropped rather than published.
type CodedError struct {
	// Code is the micro error code, e.g. "404". An empty one is treated as
	// unclassified by the runtime and answered "500": NATS micro's
	// request.Error refuses an empty code outright — it returns an error and
	// never calls RespondMsg — so replying with one directly would not be a
	// bad reply, it would be no reply at all, leaving the caller to hang
	// until its own deadline rather than see a failure.
	Code string

	// Message is what the caller is told, and what Error() returns. It is
	// published verbatim, so it must not carry anything the caller is not
	// entitled to see — this side of the hop does no redaction, and the
	// daemon's sanitizer never sees an error.
	//
	// An empty Message is replaced by Code before it reaches the wire: micro
	// refuses an empty description exactly as it refuses an empty code —
	// returning an error and never replying — and a reply must always be
	// sent.
	Message string
}

func (e CodedError) Error() string { return e.Message }
