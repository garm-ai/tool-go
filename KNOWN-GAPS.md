# Known gaps

## Built

- `toolbind` — the seam a generated binding registers against. Protobuf and
  nothing else, because every tool service anyone writes links it.
- `garmtool` — the NATS runtime: subscribe, decode the invocation context,
  unmarshal, call the handler, marshal, reply, drain on shutdown. One
  descriptor hash per process, advertised on `$SRV.INFO` so a daemon can
  reconcile what is running against the catalogue it loaded.
- **The invocation context is decoded and refused.** `Garm-Invocation`
  (`garm/contracts/callctx`) is read off every request; absent, undecodable,
  or decodable but carrying no `call_id` is `400 missing invocation context`
  and the handler is never reached, because a request carrying none of these
  did not come through the chain. A context that does decode is on `ctx` for
  `callctx.FromContext`; when it carries a deadline, that absolute deadline
  bounds the handler. A context with no deadline leaves the handler
  otherwise unbounded by this package.
- **Each endpoint has a bounded pool.** `garmtool.WithConcurrency(n)`, default
  16, per endpoint rather than per service. A slot is acquired first: at
  capacity an endpoint answers `429 overloaded` rather than queueing,
  regardless of whether `Run` has begun shutting down — a caller told it is
  overloaded retries or scales the service out, and a caller left waiting
  learns the same thing from its own timeout with no way to tell an
  overloaded service from a hung one. Only once a slot is acquired is
  shutdown checked: a request that gets a free slot while `Run` is shutting
  down is answered `503 shutting down` instead of being dispatched. `Run`
  drains the pool before returning.
- **A handler chooses its own error code.** `toolbind.CodedError{Code, Message}`
  is the only way, and it lives in the seam rather than in the runtime so a
  generated binding can name a code without importing one. The runtime finds it
  with `errors.As`, so a handler that wrapped it with `%w` still publishes the
  code it chose; everything else — an empty code included — is `500` plus the
  handler's own words.

## Known limitation: `$SRV.STATS` is not meaningful for a pooled endpoint

`nats.go/micro`'s `service.reqHandler` reads `endpoint.stats` and
`req.respondError` immediately after `Handle` returns, to record
`ProcessingTime`, `NumRequests`, `NumErrors` and `LastError`. `Handle` here is
`garmtool`'s `micro.HandlerFunc`, and since the concurrency pool dispatches the
actual work to a goroutine and returns from `Handle` immediately, `reqHandler`
takes that snapshot while the handler is still running, not once it has
answered. Two consequences follow, and neither is a bug in this package —
`reqHandler` was written for a `Handle` that runs the request synchronously,
which is exactly the assumption the pool breaks:

- `ProcessingTime` (and the `AverageProcessingTime` derived from it) measures
  how long dispatch took — a slot acquisition and a goroutine spawn — not how
  long the handler ran. It will read as near-zero regardless of handler
  latency.
- `NumErrors` and `LastError` are only ever incremented for the errors
  `Handle` itself can produce before returning: the synchronous `429`
  (capacity) and `503` (shutting down) refusals. The `400`s and `500`s
  `handle` answers from inside the pooled goroutine — malformed invocation
  context, a request that does not parse, a handler error, an unencodable
  response — happen after `reqHandler` has already read `req.respondError`,
  so none of them are counted, and reading `req.respondError` from the
  goroutine while `reqHandler` reads it concurrently on the main path is
  itself unsynchronised. A handler-chosen code from `toolbind.CodedError` is
  the same case: it also answers from inside that goroutine, so it is just as
  invisible to `$SRV.STATS` as an ordinary handler error is.

So `$SRV.STATS` for an endpoint under this pool is correct only for its
synchronous refusals (`429`, `503`); everything about how long a handler ran
or how it failed is invisible there. Anyone building `garmd`'s reconciliation
or observability against `$SRV.STATS` needs to know this before relying on it
— which is the reason this is recorded here rather than only in a commit that
scrolls out of view. This remains true until stats are computed from where the
handler actually finishes, not from where `Handle` returns.

## Not built

**Health, middleware, idempotency.** There is no per-call middleware chain and
no idempotency key handling, so a non-idempotent tool retried by a caller runs
twice.

**There is no error taxonomy, only a way to name one.** `CodedError` publishes
whatever string a handler puts in it, and nothing here says which codes mean
what — that is the daemon's contract with its callers. A handler that invents a
code nobody maps gets it forwarded verbatim, and the caller reads it as an
unknown failure.

**The pool sheds, it does not queue, and it does not measure.** There is no
metric for how often an endpoint refused, so an operator learns about a
too-small pool from callers rather than from a dashboard. The number to watch
is `429`s per endpoint; nothing here emits it yet.

**`testkit`, `templates` and `conformance` do not exist.** The README's
"Planned, not built" list describes where they will live. Testing a handler
today means calling it directly or running a service against an embedded
broker, as this repository's own tests do.

**Producer/consumer agreement with `garmd` is untested here.** That a real
`garmd` and this runtime agree about the subject, the descriptor hash and the
invocation context needs both sides, and neither may import the other. It
belongs in a cross-repository test, not in a fake here and not in a skip.
