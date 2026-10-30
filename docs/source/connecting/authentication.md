# Authentication

Set `config.Auth = gocql.PasswordAuth(username, password)` before Connect.
Custom `AuthProvider.Start` creates an `AuthExchange` per connection; providers
must support concurrent calls, and exchanges implement typed challenge and
success handling with context cancellation. Authentication bytes use typed
nullable values. Configuration and logs should not expose credentials.

Authentication is independent of encryption; configure TLS when the deployment
requires encrypted credentials and transport.
