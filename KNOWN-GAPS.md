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
