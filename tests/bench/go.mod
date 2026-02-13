module github.com/gocql/gocql/bench_test

go 1.25

require (
	github.com/brianvoe/gofakeit/v6 v6.28.0
	github.com/gocql/gocql v1.7.0
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/gocql/gocql => ../..
