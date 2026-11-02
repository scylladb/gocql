module gocql-driver-comparison

go 1.25.0

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/gocql/gocql v0.0.0-00010101000000-000000000000
	github.com/scylladb/gocql/v2 v2.0.0-00010101000000-000000000000
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	golang.org/x/sync v0.22.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => ../../.codex-cache/driver-comparison/old-source

replace github.com/scylladb/gocql/v2 => ../..
