# tool-go

**The Go runtime for a garm tool service.** What your service imports so that
serving a governed tool is a handler body and a `main`, and nothing else.

## What is here

| | |
|---|---|
| `garmtool/` | The NATS runtime: serve, lifecycle, drain, the invocation context, coded errors, and a bounded worker pool per endpoint. |
| `toolbind/` | The seam a generated binding registers against. Protobuf and nothing else. |

### Planned, not built

- `testkit/` — the harness for testing your own tool service.
- `templates/` — what `garm new toolservice --lang go` writes.
- `conformance/` — runs the suite published by [`garm`](../garm) against your service.

These do not exist yet. See `KNOWN-GAPS.md`.

## Using it

A `.proto` tool declaration generates a `Serve<Service>(toolbind.Registrar,
<Service>Handler) error` and a `<Service>Handler` interface with one method
per tool, in plain proto signatures. You implement the interface; `main`
wires it to `garmtool`:

```go
func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	svc := garmtool.New("calc", "0.1.0", garmtool.WithConcurrency(32))
	if err := calcv1.ServeCalculator(svc, calculator{}); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := svc.Run(ctx, nc); err != nil {
		log.Fatal(err)
	}
}
```

- **`garmtool.New(name, version, ...Option)`** builds a `*Service`, a
  `toolbind.Registrar`. `garmtool.WithConcurrency(n)` is the only `Option`:
  there is no setter, so a pool's size is fixed at construction, per endpoint
  (default 16 — see below).
- **`ServeCalculator`** (generated) calls `Endpoint` once per tool the
  `.proto` declares, each wired with the `Subject` it answers on
  (`Service.Method`, dotted, so nothing here or in `garmd` keeps a separate
  routing table) and the `QueueGroup` NATS balances replicas across (the
  proto service name). Both are re-exported from `garm/contracts/wire`
  rather than reimplemented, so this side and `garmd` cannot disagree about
  where a call goes.
- **`svc.Run(ctx, nc)`** serves until `ctx` is done, then drains (below). It
  blocks; call it last.

## What happens on each request

- **The invocation context is decoded first.** `Garm-Invocation`
  (`garm/contracts/callctx`) is read off the request headers before the body
  is touched. Absent, undecodable, or decodable but carrying no `call_id` is
  `400 missing invocation context`, and the handler is never called — a
  request carrying none of these did not come through the chain. A context
  that does decode lands on `ctx` for `callctx.FromContext`; when it carries
  a deadline, that deadline — absolute, not relative to this hop — bounds the
  handler's `ctx` too. A context with no deadline leaves the handler's `ctx`
  otherwise unbounded by this package.
- **Then the pool.** Each endpoint dispatches through a slot pool sized by
  `garmtool.WithConcurrency(n)` (`DefaultConcurrency = 16`), per endpoint
  rather than per service. A slot is acquired first: a full pool answers
  `429 overloaded` immediately, never queued, regardless of whether `Run`
  has begun shutting down. Only once a slot is acquired does shutdown get
  checked — a request that gets a free slot while `Run` is shutting down is
  answered `503 shutting down` instead of being dispatched.
- **Then the message and the handler.** A request body that fails to
  unmarshal into the tool's request type is `400 request does not match the
  contract this service implements`. Otherwise the handler runs, and its
  return is read as a contract:
  - a `toolbind.CodedError` — bare, or wrapped with `%w` — publishes the
    code and message it names (found via `errors.As`, so wrapping for a
    handler's own logs does not lose it); an empty `Message` is replaced by
    `Code`, and an empty `Code` is treated as unclassified instead of
    forwarded as-is;
  - any other non-nil error is `500` plus that error's own text, or `500
    internal error` if the text is empty;
  - `(nil, nil)`, or a typed-nil response (generated code can produce
    `(*pb.Reply)(nil), nil` — a non-nil interface holding a nil pointer), is
    `500 handler returned no response and no error` rather than an empty
    successful reply the caller could not tell from a real one;
  - a response that fails to marshal is `500 response could not be
    marshalled`;
  - otherwise the response is sent back as the reply.

## Shutdown and lifecycle

- `Run(ctx, nc)` serves until `ctx` is done, then drains: NATS stops
  delivering new requests to this instance while handlers already dispatched
  keep running, under a context of their own rather than `ctx` — so the
  cancellation that starts shutdown does not also cut off a call already in
  flight. Those handlers get up to `DefaultDrainTimeout` (30s) to finish on
  their own; only past that deadline is their context cancelled too, and
  `Run` returns once they exit.
- **A `Service` is single-use.** `Run` marks it stopping once shutdown
  begins and never clears the mark, so calling `Run` a second time on the
  same `*Service` refuses every request immediately rather than serving.
  Build a new one with `New` for a second run.

## Why this is its own repository

Because the boundary has to be real. A tool service must not be able to import
the enforcing code and run the policy chain itself — that is a second,
unreviewed implementation of governance, in the place it matters most.

Inside one repository that is a rule a refactor can quietly relax. As a
separate module in a separate repository, it is a fact about what `go get` can
reach.

The same boundary means your build does not inherit garm's dependency tree.

## What is deliberately not here

- **Enforcement.** Your service receives a request that has already passed the
  chain. It does not re-check, and it is not given the means to.
- **The annotations or the generator** — [`garm`](../garm).
- **The garm-side test harness.** [`garmd`](../garmd) has its own, for testing
  the chain. This one is for testing your handler.

## Status

Early, and useful. `toolbind` plus a NATS runtime that serves registered tools,
decodes the `Garm-Invocation` context onto the handler's `ctx` and refuses a
request without one, bounds each endpoint with a worker pool
(`garmtool.WithConcurrency`, default 16), lets a handler name its own error
code with `toolbind.CodedError`, and drains on shutdown.

Health, middleware, idempotency and the conformance suite are not built. See
`KNOWN-GAPS.md`.

MIT licensed.
