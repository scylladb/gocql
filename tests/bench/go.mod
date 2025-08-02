module github.com/scylladb/gocql/bench_test

go 1.24

require (
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/scylladb/gocql/v2 v2.0.0
)

require (
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/scylladb/gocql/v2 => ../..
