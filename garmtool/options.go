package garmtool

// DefaultConcurrency is how many handler calls one endpoint runs at once when
// nothing says otherwise.
//
// Sixteen rather than one, and rather than unbounded. One was the inherited
// default — a NATS subscription delivers sequentially — and it made throughput
// a function of handler latency: a 150 ms handler served about seven requests
// a second per endpoint whatever the box underneath, with nothing in the API
// saying so. Unbounded would move the overload from this process to whatever
// the handler calls, which is usually a database that cannot shed.
const DefaultConcurrency = 16

// Option configures a Service at construction.
//
// A variadic option rather than a field or a second constructor, so that
// New(name, version) — what every service written against the previous
// release calls — keeps compiling unchanged.
type Option func(*Service)

// WithConcurrency bounds how many handler calls one endpoint runs at once.
//
// PER ENDPOINT, not per service: a service whose slow tool exhausted a shared
// pool would make its fast tools unavailable, which is the failure mode of a
// shared pool and not of this one.
//
// A non-positive n leaves the default. The alternative readings are both
// worse: zero as "no concurrency" is a service that starts cleanly and answers
// 429 to everything, and zero as "unbounded" is a flag whose most likely typo
// removes the bound.
func WithConcurrency(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.concurrency = n
		}
	}
}
