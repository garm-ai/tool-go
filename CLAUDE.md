# tool-go — the Go runtime for a garm tool service

What a tool service imports so that serving a governed tool is a handler body
and a `main`.

## The invariant

**Nothing here may depend on `garmd`.**

A tool service must not be able to reach the enforcing code and run the chain
itself. That would be a second, unreviewed implementation of governance in the
one place that must not have one — and the reason this is a separate
repository rather than a package is that a repository makes it a fact about
what `go get` can reach, not a rule a refactor can relax.

The daemon authenticated, authorised, checked input and will sanitise the
response. A handler here does the work and nothing else.

## Layout

| | |
|---|---|
| `toolbind/` | The seam a generated binding registers against. Protobuf and nothing else |
| `garmtool/` | The runtime: subscribe, unmarshal, call, marshal, reply, drain |

**`toolbind` is linked by every tool service anyone writes**, so anything added
to it is added to all of them. CI asserts it reaches nothing but protobuf.

Generated code calls `Register`; a runtime implements `Registrar`. Neither
knows the other, so a binding never has to be regenerated to change runtime.

## Working here

```
mise install     the toolchain
mise run test    go test ./... -race
mise run ci      what CI runs
```

## The design record is not in this repository

It lives in **[`garm-ai/spec`](https://github.com/garm-ai/spec)** (private),
beside this one at `../spec/docs/superpowers/`.

- `specs/2026-09-24-tool-service-shell-design.md` — what a tool service is
- `specs/2026-09-24-call-stack-design.md` — what happened before a request got here

**Do not create `docs/superpowers/` here.**

## Not built

Health, drain ordering, structured errors, middleware, cancellation,
idempotency, the conformance suite, and the scaffold templates. `garmtool`
today is the hop and the lifecycle around it.
