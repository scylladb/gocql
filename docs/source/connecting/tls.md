# TLS

TLS encrypts traffic and can verify server identity. `SslOptions` without
explicit verification is insecure for historical compatibility. Always enable
server verification in production.

## Verified TLS

Use a trusted CA bundle and hostname verification:

```go
cluster := gocql.NewCluster("db.example.com")
cluster.SslOpts = &gocql.SslOptions{
	CaPath:                 "/etc/scylla/ca.crt",
	EnableHostVerification: true,
	Config: &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "db.example.com",
	},
}
```

Set `tls.Config.ServerName` to a DNS name or IP address in the server
certificate. Contact-point DNS names are resolved before the driver dials, so
without an explicit server name the driver verifies the certificate against the
resolved IP address. Configure a private CA in `CaPath`; do not disable
verification to work around an untrusted or mismatched certificate.

Applications can construct the CA pool themselves when they need complete
control over Go's standard TLS configuration:

```go
roots := x509.NewCertPool()
caPEM, err := os.ReadFile("/etc/scylla/ca.crt")
if err != nil {
	return err
}
if !roots.AppendCertsFromPEM(caPEM) {
	return errors.New("no CA certificates found")
}

cluster.SslOpts = &gocql.SslOptions{
	Config: &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "db.example.com",
	},
}
```

Leave `tls.Config.InsecureSkipVerify` as `false`. Setting
`EnableHostVerification` also forces verification on if the supplied
`tls.Config` requested otherwise.

## Mutual TLS

When the server requires a client certificate, provide both certificate and
private-key files:

```go
cluster.SslOpts = &gocql.SslOptions{
	CaPath:                 "/etc/scylla/ca.crt",
	CertPath:               "/etc/scylla/client.crt",
	KeyPath:                "/etc/scylla/client.key",
	EnableHostVerification: true,
	Config: &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "db.example.com",
	},
}
```

`CertPath` and `KeyPath` must be supplied together. Protect the private key
with restrictive filesystem permissions.

## Custom dialers

`ClusterConfig.Dialer` handles TCP establishment; the driver still performs
TLS configured by `SslOpts`.

`ClusterConfig.HostDialer` handles the entire connection, including TLS.
`SslOpts` does not configure TLS on a custom `HostDialer`, so its
implementation must perform certificate and hostname verification itself.

## Troubleshooting

- `x509: certificate signed by unknown authority`: configure the self-signed
  root CA in `CaPath` or `tls.Config.RootCAs`, and ensure that the server sends
  any intermediate CA certificates needed to complete the chain.
- `x509: certificate is valid for ...`: set `tls.Config.ServerName` to a DNS
  name or IP address covered by the certificate, or issue a corrected
  certificate.
- Client certificate errors: verify that `CertPath` and `KeyPath` form a pair
  trusted by the server.
