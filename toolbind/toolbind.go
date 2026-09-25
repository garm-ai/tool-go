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
