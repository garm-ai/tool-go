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
| `garmtool/` | The runtime: subscribe, decode the invocation context, unmarshal, call, marshal, reply, drain |

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

## The concurrency decision, and the two alternatives that were rejected

**A handler replies before the runtime returns to `micro`.** Not a style
choice: `micro/service.go`'s `reqHandler` reads `req.respondError` and stops
its clock the instant `Handle` returns, so a handler that answers later races
that read on every coded refusal and makes every service's reported latency
the cost of a goroutine spawn. `micro` documents no contract about this;
`KNOWN-GAPS.md` records it as the upstream defect it is. Do not reintroduce a
goroutine per request to "fix" throughput.

**Concurrency is `WithConcurrency(n)` micro service instances in one
process.** `nats.go` runs one delivery goroutine per subscription, so more
in-flight requests requires more subscriptions, and n instances in one queue
group is n subscriptions the server balances across. To `garmd` that looks
like n replicas — separate `$SRV.INFO` identities, the same contract — which
its scatter-gather discovery already handles, because it cannot know how many
instances exist anyway.

Two alternatives, both rejected:

- **n endpoints on ONE micro service, same subject and queue group.**
  `micro`'s `addEndpoint` has no duplicate check, so this works and costs one
  identity instead of n. Rejected because `micro` advertises every endpoint
  individually in `$SRV.INFO`, and that document is exactly what `garmd`
  reads to answer "is this tool declared but unreachable". n entries for one
  tool corrupts the one signal discovery has. n instances add responders,
  which is a cost with a number on it, and pollute nothing.
- **One request at a time per process, concurrency from replicas only.**
  Correct, and simpler, and a tax: fifty containers to get fifty concurrent
  calls is not a design.

**`DefaultConcurrency` is 4, and the number is arithmetic.** Every instance
answers `$SRV.INFO`; `garmd` collects those into a channel buffered at 64
that drops when full. agentd builds one Service per agent, so the reference
plane is already ten or eleven services — at 16 that is ~170 responders into
64 slots. Raising it is a `garmd` change first. The doc comment on the
constant carries the full sum; keep them in step.

## Lifecycle

`Run(ctx, nc)` serves until `ctx` is done, then drains: in-flight handlers
run under a context of their own, not `ctx`, for up to `DefaultDrainTimeout`
(30s) before that context is cancelled too and `Run` returns. Synchronous
handling did not make the drain path dead: `nats.go`'s `Subscription.Drain`
sends UNSUB and returns without waiting, so requests are still delivered
after every `Stop` has returned — which is what `stopping` refuses with 503
and what the `inFlight` WaitGroup makes `Run` outlive. A `*Service` is
single-use — `Run` marks it stopping once shutdown begins and never clears
the mark, so a second `Run` on the same value refuses every request
immediately rather than serving; call `New` again.

## Configuration is logged, defaults included

`WithLogger` takes an `*slog.Logger` and `Run` writes one line naming the
concurrency, name, version and queue groups actually in force. A default is
logged exactly like a passed value: nobody should have to read this package
to find out which number is running. A caller that supplies no logger gets
silence — never this library's own handler on the process's stderr.

## Not built

`testkit`, health beyond what `micro` gives for free, middleware,
idempotency, the conformance suite, and the scaffold templates. `garmtool`
today is the hop — invocation context, synchronous handling, drain — and the
lifecycle around it. See `KNOWN-GAPS.md`, which is gaps only.

A handler can name its own error code via `toolbind.CodedError{Code,
Message}` — there is no taxonomy behind the code beyond that. Which strings
mean what is garmd's contract with its callers, not something this package or
its runtime defines.
