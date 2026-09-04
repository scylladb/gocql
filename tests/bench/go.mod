module github.com/gocql/gocql/bench_test

go 1.25.0

require (
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/gocql/gocql v1.7.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	golang.org/x/sync v0.22.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => ../..

// The root module requires github.com/scylladb/gocql/lz4 so its integration suite can
// build LZ4Compressor, and the replace above brings that requirement into this module's
// graph -- but not the root's own `replace ... => ./lz4`, which applies only while the
// root is the main module. Without this second replace, `go mod tidy -C tests/bench`
// resolves the published release of a module that is sitting in the tree: it needs the
// network, and it computes this module's graph from the released lz4's requirements
// while the root computes from ./lz4.
replace github.com/scylladb/gocql/lz4 => ../../lz4
