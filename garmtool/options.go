package garmtool

import (
	"log/slog"
	"time"
)

// DefaultDrainTimeout bounds how long Run waits, once it has started
// shutting down, for handlers already running to finish on their own.
//
// Not exposed as an Option: the callers who would need something other than
// "generous" are the ones whose handler ignores context cancellation
// entirely, and a config knob would invite tuning that around rather than
// fixing the handler. After the timeout, Run cancels the handlers' context
// too, and returns once they exit.
const DefaultDrainTimeout = 30 * time.Second

// DefaultConcurrency is how many requests one tool served by this process
// handles at once when nothing says otherwise.
//
// Four, and the number is an arithmetic result rather than a taste. Every
// unit of concurrency is a whole micro service instance — its own nuid
// identity, its own subscriptions, its own nine control-verb subscriptions —
// because that is the only way to get a second request in flight without
// returning from micro's handler before replying (see Run). One of those
// nine answers `$SRV.INFO`, and garmd's discovery is a scatter-gather that
// asks every service in the plane at once and collects the replies in a
// channel buffered at 64, dropping silently when it is full
// (`garmd/internal/transport/nats/nats.go`, `Services()`).
//
// So the cost multiplies, and it multiplies PER SERVICE rather than per
// process: agentd builds one Service per agent, so the reference bank plane
// — five agents, bankd's four domains, webd and tasksd — is already ten or
// eleven of them. At sixteen that is around 170 responders answering one
// discovery round into a 64-slot channel; at four it is around 40, which
// fits. A default that breaks discovery in the plane we ship is not a
// default. Raising garmd's buffer would move the ceiling, and until it does
// this number is what keeps discovery honest.
//
// This only decides what a caller that says nothing gets — tasksd, agentd and
// webd all pass their own. A tool whose handler is mostly waiting on I/O
// should raise it deliberately: webd, which spends a call fetching a page, is
// the obvious one.
const DefaultConcurrency = 4

// Option configures a Service at construction.
//
// A variadic option rather than a field or a second constructor, so that
// New(name, version) — what every service written against the previous
// release calls — keeps compiling unchanged.
type Option func(*Service)

// WithConcurrency bounds how many requests this process handles at once.
//
// n micro service instances, not n goroutines. A handler runs synchronously
// in the goroutine NATS gives its subscription — anything else races micro's
// own statistics, see Run — and nats.go runs exactly one delivery goroutine
// per subscription, so one subscription is one request at a time and nothing
// inside a process can change that. n instances means n subscriptions in the
// same queue group, and the server balances across them.
//
// PER TOOL, not per process: each instance subscribes to every registered
// tool, and each of those subscriptions has a delivery goroutine of its own.
// So a service with four tools at n=4 can have four calls to its slow tool
// running without its fast tools being unavailable, which is the failure mode
// of a shared pool and not of this.
//
// n is not free, and the cost is not goroutines — it is that each instance is
// a separate responder on `$SRV.INFO`, `$SRV.PING` and `$SRV.STATS`. See
// DefaultConcurrency for the arithmetic, and for why the answer to needing a
// great deal more throughput is more processes behind the queue group rather
// than a very large n.
//
// A non-positive n leaves the default. The alternative readings are both
// worse: zero as "no concurrency" is a service that starts cleanly and
// subscribes to nothing, and zero as "unbounded" is a flag whose most likely
// typo removes the bound.
func WithConcurrency(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.concurrency = n
		}
	}
}

// WithLogger gives the service somewhere to report the configuration it is
// actually running with.
//
// Taken rather than invented, and a caller that supplies none gets silence
// rather than stderr: a library that writes to a process's stderr on its own
// initiative is one every service then has to work out how to silence, and a
// tool service's log stream belongs to the service.
//
// What it is used for is one line at startup naming the effective
// concurrency, the service name, the version and the queue groups — a default
// logged exactly like a passed value, so nobody has to read this file to find
// out which number is in force.
func WithLogger(l *slog.Logger) Option {
	return func(s *Service) { s.log = l }
}
