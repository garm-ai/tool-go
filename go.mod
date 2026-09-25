module github.com/garm-ai/tool-go

go 1.26.0

require (
	github.com/garm-ai/garm v0.4.0
	// Tests only. The bugs that produced a service which started cleanly and
	// answered nothing were all invisible from inside the process, so the
	// round-trip tests talk to a real broker — embedded, on a port the kernel
	// picks. Nothing outside a _test.go file may import it: a tool service's
	// build must not inherit a server to link a runtime. CI checks.
	github.com/nats-io/nats-server/v2 v2.15.0
	github.com/nats-io/nats.go v1.54.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.8.0-default-no-op // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.2 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)
