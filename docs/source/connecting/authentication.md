# Authentication

Set `ClusterConfig.Authenticator` for credentials shared by all hosts:

```go
cluster := gocql.NewCluster("db.example.com")
cluster.Authenticator = gocql.PasswordAuthenticator{
	Username: os.Getenv("SCYLLA_USERNAME"),
	Password: os.Getenv("SCYLLA_PASSWORD"),
}
```

Do not hard-code credentials in source control. Load them from a secret manager
or another protected runtime source.

By default, `PasswordAuthenticator` accepts any authenticator class announced
by the server. Restrict it when the expected server authenticator is known:

```go
cluster.Authenticator = gocql.PasswordAuthenticator{
	Username: os.Getenv("SCYLLA_USERNAME"),
	Password: os.Getenv("SCYLLA_PASSWORD"),
	AllowedAuthenticators: []string{
		"org.apache.cassandra.auth.PasswordAuthenticator",
	},
}
```

Use `ClusterConfig.AuthProvider` when credentials or authentication logic vary
by host. `Authenticator` and `AuthProvider` cannot both be set.

Authentication credentials are only protected in transit when TLS is enabled.
See [TLS](tls.md).
