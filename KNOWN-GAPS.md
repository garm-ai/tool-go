# Known gaps

## One request at a time, per endpoint

A tool endpoint currently handles exactly one request at a time. Measured:
eight concurrent calls to a 150 ms handler peaked at a concurrency of **one**.

It is not a bug so much as an inherited default. `micro` dispatches from a NATS
async subscription, and a subscription delivers to its handler sequentially, so
a handler that blocks blocks the endpoint. Nothing here spawns a goroutine or
holds a worker pool.

The consequence is a throughput ceiling that scales with handler latency rather
than with the machine: a 150 ms handler serves about seven requests a second
per endpoint, per process, whatever the box underneath. For a tool that calls a
database or a payment scheme, that is the number that matters and it is
currently invisible — nothing in the API or the docs says it.

Two things follow for anyone reading this before it is fixed. Scale by running
more instances of the service, since the queue group already balances across
them. And do not assume a handler is single-threaded just because it is today:
a worker pool here would be a small change, and code that relied on the
serialisation would break silently rather than loudly.

The fix is a bounded pool with a configurable size. The question worth
answering first is the default, because raising it changes behaviour for every
existing service at once — and unbounded concurrency would move the overload
from this process to whatever the handler calls.

**Fixed.** `garmtool.WithConcurrency(n)` bounds each endpoint's pool (default
`DefaultConcurrency = 16`); at capacity an endpoint answers `429 overloaded`
rather than queueing.

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
  itself unsynchronised.

So `$SRV.STATS` for an endpoint under this pool is correct only for its
synchronous refusals (`429`, `503`); everything about how long a handler ran
or how it failed is invisible there. Anyone building `garmd`'s reconciliation
or observability against `$SRV.STATS` needs to know this before relying on it
— which is the reason this is recorded here rather than only in a commit that
scrolls out of view. Task 3's rewrite of the transport carries this forward
until stats are computed from where the handler actually finishes, not from
where `Handle` returns.
