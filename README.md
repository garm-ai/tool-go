# tool-go

**The Go runtime for a garm tool service.** What your service imports so that
serving a governed tool is a handler body and a `main`, and nothing else.

## What is here

| | |
|---|---|
| `garmtool/` | The NATS runtime: serve, lifecycle, drain, the invocation context, coded errors, and the concurrency model. |
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

	svc := garmtool.New("calc", "0.1.0",
		garmtool.WithConcurrency(4),
		garmtool.WithLogger(slog.New(slog.NewTextHandler(os.Stderr, nil))),
	)
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
  `toolbind.Registrar`. There is no setter: what the options say is fixed at
  construction.
  - `garmtool.WithConcurrency(n)` — how many requests this process handles at
    once, per tool. Default 4, and the number has arithmetic behind it (see
    below).
  - `garmtool.WithLogger(l *slog.Logger)` — where the service reports the
    configuration it is actually running with, one line at startup, defaults
    included. A service that supplies none gets silence: a library writing to
    your process's stderr uninvited is one you then have to silence.
- **`ServeCalculator`** (generated) calls `Endpoint` once per tool the
  `.proto` declares, each wired with the `Subject` it answers on
  (`Service.Method`, dotted, so nothing here or in `garmd` keeps a separate
  routing table) and the `QueueGroup` NATS balances replicas across (the
  proto service name). Both are re-exported from
  `github.com/garm-ai/contracts/wire` rather than reimplemented, so this side
  and `garmd` cannot disagree about where a call goes.
- **`svc.Run(ctx, nc)`** serves until `ctx` is done, then drains (below). It
  blocks; call it last.

## What happens on each request

- **The invocation context is decoded first.** `Garm-Invocation`
  (`github.com/garm-ai/contracts/callctx`) is read off the request headers
  before the body is touched. Absent, undecodable, or decodable but carrying no `call_id` is
  `400 missing invocation context`, and the handler is never called — a
  request carrying none of these did not come through the chain. A context
  that does decode lands on `ctx` for `callctx.FromContext`; when it carries
  a deadline, that deadline — absolute, not relative to this hop — bounds the
  handler's `ctx` too. A context with no deadline leaves the handler's `ctx`
  otherwise unbounded by this package.
- **Then shutdown, then the backlog.** A request delivered after `Run` has
  begun shutting down is answered `503 shutting down` rather than handled —
  NATS goes on delivering what was already pending for a moment after the
  subscriptions are drained. A request whose absolute deadline had already
  passed before this process reached it is answered `429 overloaded: queued
  past its deadline` and the handler is never called: it waited in NATS's
  pending queue, its caller has given up, and running it would spend
  capacity — and take a real effect, for a non-idempotent tool — on nobody's
  behalf. An invocation carrying no deadline cannot be shed this way; see
  `KNOWN-GAPS.md`.
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

## Concurrency, and what it costs

**A handler runs synchronously and has replied before the runtime returns to
`micro`.** It has to: `nats.go/micro` reads the request's error field and
stops its clock the moment the handler returns, so anything that answers
later races that read on every coded refusal and reports a latency that is
the cost of a goroutine. That is an upstream defect — `KNOWN-GAPS.md` names
it — and handling synchronously is how this side stops paying for it.

**So concurrency comes from subscriptions.** `nats.go` runs exactly one
delivery goroutine per subscription, so one subscription is one request at a
time and nothing inside a process changes that. `WithConcurrency(n)`
registers **n micro service instances**, each carrying every tool, all in the
same queue group: the server balances across them and n calls run at once.
It is per tool, not per process — each instance subscribes separately to each
tool — so a slow tool cannot make a fast one unavailable.

**Raising `n` is not free, and the cost is not goroutines.** Every instance
is a separate responder on `$SRV.INFO`, `$SRV.PING` and `$SRV.STATS`, and
`garmd`'s discovery collects INFO replies in a channel buffered at 64 that
drops silently when it is full. Instances × services × replicas has to stay
under that. `DefaultConcurrency = 4` is what keeps the reference plane —
around ten or eleven services once agentd's per-agent services, bankd, webd
and tasksd are counted — inside the ceiling. Raise it deliberately for a tool
that spends its time waiting on I/O; past that, **throughput is a deployment
question**: run more processes behind the queue group, which already works
and needs no flag.

To a daemon, n instances look exactly like n replicas — separate `$SRV.INFO`
identities advertising the same contract — which is a case discovery already
handles. `$SRV.STATS`, correspondingly, is per instance: an operator reading
one reply is reading one nth of the traffic.

## Shutdown and lifecycle

- `Run(ctx, nc)` serves until `ctx` is done, then drains: NATS stops
  delivering new requests to this process while handlers already running keep
  going, under a context of their own rather than `ctx` — so the cancellation
  that starts shutdown does not also cut off a call already in flight. Those
  handlers get up to `DefaultDrainTimeout` (30s) to finish on their own; only
  past that deadline is their context cancelled too, and `Run` returns once
  they exit. Every instance is stopped, and `Run` outliving them is not
  automatic: `nats.go`'s subscription drain sends UNSUB and returns without
  waiting, so requests are still delivered after `Stop` has returned.
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
What it does inherit is the contract — `github.com/garm-ai/contracts`, the
wire types and the subject naming both sides agree on, and nothing else.

## What is deliberately not here

- **Enforcement.** Your service receives a request that has already passed the
  chain. It does not re-check, and it is not given the means to.
- **The annotations or the generator** — [`garm`](../garm).
- **The garm-side test harness.** [`garmd`](../garmd) has its own, for testing
  the chain. This one is for testing your handler.

## Status

Early, and useful. `toolbind` plus a NATS runtime that serves registered tools,
decodes the `Garm-Invocation` context onto the handler's `ctx` and refuses a
request without one, handles `garmtool.WithConcurrency` requests at once
(default 4), lets a handler name its own error code with
`toolbind.CodedError`, and drains on shutdown.

**Upgrading from v0.5.0 or earlier moves you onto
`github.com/garm-ai/contracts`.** A service still importing
`github.com/garm-ai/garm/contracts/...` will link both copies of
`garm/tool/v1` and panic at init on a duplicate proto registration — it
builds, then dies on startup. Move your own imports in the same change.

Health, middleware, idempotency and the conformance suite are not built. See
`KNOWN-GAPS.md`.

MIT licensed.
