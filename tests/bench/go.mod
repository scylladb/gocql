module github.com/gocql/gocql/bench_test

go 1.22

require (
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/gocql/gocql v1.7.0
)

require (
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => ../..
