# tool-go

**The Go runtime for a garm tool service.** What your service imports so that
serving a governed tool is a handler body and a `main`, and nothing else.

## What is here

| | |
|---|---|
| `garmtool/` | Serve, lifecycle, health, drain, structured errors, middleware, cancellation, idempotency. |
| `testkit/` | The harness for testing your own tool service. |
| `templates/` | What `garm new toolservice --lang go` writes. |
| `conformance/` | Runs the suite published by [`garm`](../garm) against your service. |

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

Early. `toolbind` and a NATS runtime that serves registered tools and drains
on shutdown. Health, structured errors, middleware, cancellation, idempotency
and the conformance suite are not built.

MIT licensed.
