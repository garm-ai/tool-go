# Known gaps

What this module does not do, and what it does on purpose that will surprise
you. Not an inventory of what works — the code says that, and a file that
repeats it goes stale in a way the code cannot.

## Not ours, and worth filing

**`nats.go/micro` requires a handler to reply before it returns, and says so
nowhere.** `micro/service.go`'s `reqHandler` calls `Handler.Handle(req)` and
then reads `req.respondError` and stops its clock the instant that returns.
A handler that has not replied yet leaves that read looking at a field
another goroutine is about to write — a data race on every coded refusal —
and makes `ProcessingTime` the cost of dispatch rather than of the call. No
README, doc comment or type in `micro` states the requirement, while
spawning a goroutine from a subscription callback is the conventional shape
for plain `nats.go`, whose async subscriptions are one-at-a-time by design.
So `micro` silently breaks a pattern its own library encourages. This
repository works around it by handling synchronously (see `Run`); the defect
is upstream's and is worth filing against `nats.go`. Nothing has been filed.

## What this does not protect you from

**A backlog is only shed when the caller set a deadline.** Capacity is no
longer observable: one subscription handles one request at a time and neither
`micro` nor `nats.go` exposes that subscription's pending depth to a handler,
so the old "the pool is full, 429" has no equivalent. What is left is that a
request whose absolute deadline has already passed when this process reaches
it is refused `429` rather than run. `garmd` sets a deadline on every hop, so
that covers the calls the platform makes — but a caller that sets none gets
no shedding at all: the backlog grows in the subscription's pending queue
until NATS drops it as a slow consumer, with **no reply**, and that caller
learns from its own timeout.

**Nothing counts the refusals.** There is no metric for how often a service
shed, so an operator learns about an undersized deployment from callers
rather than from a dashboard. The number to watch is `429`s per tool.

**Concurrency has a ceiling that belongs to `garmd`, not here.** Every unit
of `WithConcurrency` is a micro service instance and every instance answers
`$SRV.INFO`. `garmd`'s discovery collects those replies in a channel buffered
at 64 and drops silently when it is full
(`garmd/internal/transport/nats/nats.go`, `Services()`). Instances × services
× replicas must stay under that. `DefaultConcurrency` is 4 because the
reference plane is already ten or eleven services; raising it is a `garmd`
change first.

**`$SRV.STATS` is per instance, not per process.** A service running n
instances answers a stats request n times, each with its own slice of the
traffic. An operator reading one reply is reading one nth of the picture.

**Upgrading past v0.5.0 is not a one-line bump for a consumer.** This module
is on `github.com/garm-ai/contracts`. A service still importing
`github.com/garm-ai/garm/contracts/...` links both copies of
`garm/tool/v1`, and `protoregistry` **panics at init** —
`proto: file "garm/tool/v1/attribution.proto" is already registered`. It
builds; it dies on startup. The consumer's own imports have to move in the
same change.

## Not built

**Health, middleware, idempotency.** There is no per-call middleware chain and
no idempotency key handling, so a non-idempotent tool retried by a caller runs
twice.

**There is no error taxonomy, only a way to name one.** `CodedError` publishes
whatever string a handler puts in it, and nothing here says which codes mean
what — that is the daemon's contract with its callers. A handler that invents a
code nobody maps gets it forwarded verbatim, and the caller reads it as an
unknown failure.

**`testkit`, `templates` and `conformance` do not exist.** The README's
"Planned, not built" list describes where they will live. Testing a handler
today means calling it directly or running a service against an embedded
broker, as this repository's own tests do.

**Producer/consumer agreement with `garmd` is untested here.** That a real
`garmd` and this runtime agree about the subject, the descriptor hash and the
invocation context needs both sides, and neither may import the other. It
belongs in a cross-repository test, not in a fake here and not in a skip.
