// Package toolbind is the seam a generated tool binding registers against.
//
// Generated code calls Register; a runtime implements Registrar. Neither
// knows the other, which is the point: a binding must not import a runtime,
// or picking a different one later would mean regenerating.
//
// It is deliberately tiny and deliberately dependency-free beyond protobuf.
// Everything a tool service links starts here, so anything added to this
// package is added to every tool service anyone writes.
package toolbind

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Handler executes one tool call.
//
// Messages rather than a typed signature, because generated code adapts the
// typed handler an author writes into this shape. A runtime never needs to
// know the concrete types.
type Handler func(ctx context.Context, req proto.Message) (proto.Message, error)

// Method is one registered tool.
type Method struct {
	// FullMethod is the route, "/pkg.Service/Method" — the same string the
	// daemon holds, so the two cannot disagree about what was called.
	FullMethod string

	// NewRequest allocates an empty request for the runtime to unmarshal
	// into. A constructor rather than a descriptor because the generated
	// binding knows the concrete type and the runtime should not have to.
	NewRequest func() proto.Message

	Handle Handler
}

// Registrar is what a runtime provides and a generated binding consumes.
type Registrar interface {
	// Register adds one method to a service. Called once per tool by
	// generated code, before serving begins.
	Register(service string, m Method) error
}
