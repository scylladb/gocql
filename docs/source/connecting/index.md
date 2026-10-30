# Connecting

`DefaultConfig(hosts...)` supplies a validated configuration for `Connect`.
Contact points may include native ports. `Config.Port` supplies the port when
one is absent. `Protocol` supports 3/4/5; automatic negotiation caps at 4, so
select 5 explicitly for a server that supports it.

Set `ConnectTimeout`, `DNSTimeout`, `ReadTimeout`, `WriteTimeout`, socket options,
`Pool`, `Metadata`, identity and the default profile before connecting. Connect
captures configuration before workers and connection services start. Callbacks
and provider objects must support concurrent use; subsequent caller mutation
must not change the captured physical connection settings.

Use `Pool.ConnectionsPerHost` for a host override or `ConnectionsPerShard` for
shard sizing. A RunStream occupies one configured connection through its idle
time and pages. Budget capacity accordingly. Cancellation interrupts work;
Session.Close rejects new work, cancels requests and joins writers/workers.
Bindings still require their own Close/Wait before borrowed input reuse.

`Dialer` receives a typed `DialTarget`; `DialHost` and `TLSForHost` customize
host setup. Hooks honor context cancellation and the selected endpoint/source.
`DNSResolver`, `AddressTranslator`, `HostFilter` and `ReconnectPolicy` are
independent extension points. Synchronous callbacks must be prompt and must
not reenter blocking Close/Wait.

```{toctree}
:maxdepth: 1

authentication
compression
tls
```

`ConvictionPolicy.AddFailure(error, HostSnapshot)` returns whether a failed host
should be marked down; `Reset(HostSnapshot)` clears application policy state.
The host snapshot is detached read-only metadata that can be retained. Custom
policies must support concurrent callbacks. The default SimpleConvictionPolicy
always convicts and its Reset is a no-op.
