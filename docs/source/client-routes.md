# Client routes and cloud

Set `Config.ClientRoutes` to typed `ClientRoutesOptions` before Connect. It
supplies connection identities and hostname overrides for route discovery.
The driver uses typed internal metadata queries and SNI-aware per-host hooks;
route refresh and connection lifecycle obey context cancellation.

For cloud bundles import `github.com/scylladb/gocql/v2/scyllacloud` and call
`NewCloudCluster(bundlePath, options...)`. The returned Config is passed directly
to gocql.Connect. Builder options are applied before capturing the dialer in the
installed DialHost/TLSForHost hooks. Changing Config.Dialer afterward does not
replace the captured dialer. Cloud source-port sharding is disabled because SNI
routes through the proxy; pool sizing remains explicit.
