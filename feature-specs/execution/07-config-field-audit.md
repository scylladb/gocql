# Step 07 exposed-field audit

This audit covers every field in Config, PoolOptions, MetadataOptions,
ProtocolLimits, IdentityOptions, ClientRoutesOptions, Profile, Options,
PrepareOptions and StreamOptions. Callback/interface objects remain shared and
must support concurrent calls; synchronous callbacks must return promptly and
must not reenter blocking Close/Wait. Their declarations are snapshotted, not
arbitrarily cloned. Observer and logger panics are contained.

## Configuration

| Fields | Default, validation and applied seam |
| --- | --- |
| ContactPoints, Port, Protocol, CQLVersion, Keyspace | Contact descriptors copied; 9042, auto/3/4/5, 3.0.0, empty namespace. Endpoints and protocol validate before setup. Keyspace establishes connection namespace; no mutable USE. |
| ConnectTimeout, DNSTimeout | 5 s each; zero disables only that local bound, negatives reject. Setup includes DNS/dial/TLS/startup/auth/register; hooks inherit its caller context. |
| ReadTimeout, WriteTimeout | Disabled; negative rejects. Independent socket/writer bounds implemented by transport 04. |
| SocketKeepalive, NoDelay, ReceiveBuffer, SendBuffer, Linger | No probes, no-delay true, buffer/linger unset. Explicit zero preserved. Negative buffers reject; linger requires nonnegative whole int32 seconds. Unsupported socket interfaces/settings fail explicitly (04). |
| KeepaliveInterval, KeepaliveTimeout, CoalesceWait | 30 s / 30 s / 0; zero disables heartbeat/local timeout/additional wait. Negatives reject. Existing heartbeat, exclusive admission and span writer apply them. |
| LocalAddress, Dialer, DialHost, DNSResolver | Invalid source means OS choice; hooks nil use native defaults. DialHost takes precedence, raw endpoint/source contract checked; TLS remains driver owned. Unix/source/shard incompatibilities explicitly reject (04). |
| TLS, TLSForHost, Auth | Nil means no optional TLS/auth. Defensive TLS snapshot includes mutable fields. Per-host TLS inherits nil result, verified required routes cannot downgrade; provider has per-connection exchange, context and cleanup (04/auth tests). |
| Compression | Raw; enum validates, v5 Snappy rejects. Negotiated v3/v4 raw/LZ4/Snappy buffered and v5 raw/LZ4 spans apply. Streaming rejects negotiated v3/v4 frame compression before query. |
| Pool.ConnectionsPerHost, ConnectionsPerShard, MaxExcessPerShard | 0 / 1 / 2; nonnegative, at least one positive count. Positive host count overrides shard count. Existing counted picker, reconnect and bounded repairs apply (04). |
| Pool.MaxRequestsPerConnection, MaxOrphanedStreams | 32768 / 1024; capacity 1..32768, orphans nonnegative. Orphan effective budget caps at capacity-1, zero poisons on first orphan (04). |
| Pool.ShardAwarePort, SourcePortStart, SourcePortEnd | Enabled, 49152..65535; enabled range validates >=1024 and ordered. Per-host mode disables targeting; transport honors explicit sources/NAT fallback. |
| Reconnect, Conviction, AddressTranslator, HostFilter | Default bounded jitter reconnect; other hooks inherit existing defaults/nil behavior. Negative reconnect delay rejects; host eligibility and pinning are checked before acquisition; snapshots/provider concurrency retained. |
| ClientRoutes.ConnectionIDs, HostnameOverrides, TLS, SNIProxyAddress | Nil routes disabled. IDs/hosts/maps copied and validated, verified TLS required for TLS routes/proxy. Translator and shard-source incompatibilities reject. Typed route discovery/sticky selection/fences remain 05. |
| Identity.DriverName, DriverVersion, ApplicationName, ApplicationVersion, ClientID; ReportConfiguration | Driver identity defaults to repository driver constants; optional application fields empty; reporting enabled. Startup properties and reporting switch apply, not just retained fields. |
| Metadata.Mode, Keyspaces, InitialDiscovery | Full / all / true. Scope enum, duplicates and empty names validate. Keyspace slice copied; publication and typed discovery obey 05 fences. |
| Metadata.PeriodicRefresh, RequestTimeout | 60 s / 31 s. Zero disables periodic work/local per-page bound; negatives reject. Internal pages derive fresh bounded contexts including decode, while caller/setup/agreement remain outer bounds. |
| Metadata.ServerTimeout | Unset chooses 30 s for Scylla targets and omits otherwise; explicit zero omits. Positive milliseconds must be representable, and unsupported targets fail before query. Unified bootstrap/topology/schema/agreement/routes execution applies it. |
| Metadata.AgreementTimeout, AgreementPoll | 60 s / 200 ms; strictly positive. Caller can shorten. AutomaticAgreement / RefreshAfterAgreement default true and govern successful DDL postwork without replay. |
| Metadata.TopologyEvents, StatusEvents, SchemaEvents, ClientRouteEvents | True; registration, dispatch and publication honor flags (05). |
| Limits.MaxFrameBytes, MaxDecompressedBytes, MaxMetadataDepth | 256 MiB / 256 MiB / 64; positive, <=int32 byte range and <=128 depth. Transport and incremental readers enforce. |
| Limits.MaxPagingStates, MaxPagingStateBytes | 65536 / 16 MiB; positive. Exact hash-indexed equality history is incrementally allocated and bounded across automatic buffered/stream pages. |
| PreparedCacheSize, Partitioners | 1000 / Murmur3+CDC registered. Size nonnegative, zero disables LRU retention; maps copied, names and providers validate. Prepared handles retain captured partitioner and dependency fences (03/06). |
| DefaultProfile | Required validated handle. Config retains its explicit replacement identity; field reassignment on caller config cannot change session. Each operation captures one immutable version. |
| TimestampGenerator | Concurrency-safe unique microsecond generator, nil disables. Exactly one invocation per eligible logical ordinary noncounter execution, then reused unchanged for pages/retries/repairs. Explicit timestamp including zero overrides it. |
| Observer, LifecycleObserver, Logger | Nil disables custom callbacks. Values captured; request/attempt events constructed only as needed for observation/result accounting. Calls are outside iterator/driver publication locks; observer/logger panics cannot corrupt requests. |

## Execution

| Fields | Snapshot, zero and applicability |
| --- | --- |
| Profile.Consistency, SerialConsistency | LOCAL_QUORUM / present LOCAL_SERIAL. Enum validates; serial level limited to SERIAL/LOCAL_SERIAL. Regular serial makes operation conditional; optional serial alone does not. |
| Profile.Timeout, PageTimeout, PageSize, Prefetch | 30 s / 0 / 5000 / 1. Durations nonnegative, size positive, prefetch 0 or 1. Zero durations disable local bounds; prefetch 0 creates no worker. One bounded pager worker is reused for enabled continuations. |
| Profile.Retry, Routing, Speculation | Safe three-attempt retry, built-in token/shuffle policy, no speculation. Captured interfaces used on all attempts; unknown/conditional operations never speculate. Routing's host/shard/leader strictness survives acquisition. |
| Options.Profile, Consistency, SerialConsistency | Profile selects one version; Set=false inherits. Nested serial Option distinguishes inherit/omit/set. Overrides validate once. |
| Options.Timeout, PageTimeout, PageSize, Prefetch | Set=false inherits; explicit duration zero and prefetch zero disable locally. Page size zero rejects. Explicit nonzero prefetch rejects for manual/unpaged/stream execution; explicit page size rejects for unpaged/BATCH, whose protocols cannot apply it. Inherited paging defaults are inapplicable to those APIs. |
| Options.Idempotent, Timestamp, NowInSeconds | False / absent / absent; explicit timestamp/now zero remains present. Now requires v5. Bytes-written classification is conservative; safe policy retries a proven zero-write QueryError without requiring idempotence. |
| Options.Keyspace, Trace, IncludeMetadata | Empty inherits; false/false. Prepared incompatible namespace rejects; v5 applies namespace flag. v3/v4 different namespace requires checked qualification of every statement table reference. Trace request/header and actual response IDs retained; metadata inclusion overrides validated cache skipping. |
| Options.CustomPayload | Entry descriptors copied exactly once, bytes remain borrowed and source leases caller retained. Duplicate/oversized keys and stale views reject. Per-operation owner drains all accepted writers, losers and batch members before binding Wait allows reuse. Returned result payloads are independent owned escapes. |
| Options.HostID, ShardID | Unset follows routing; present empty host rejects, shard requires host. Host pin survives custom policies; strict shard absence/unavailability rejects without same-host fallback. |
| Options.Retry, Observer | Nil inherits policy/config observer; nonnil captured exactly once and used through retries/pages. |
| PrepareOptions.Keyspace, HostID, Trace | Namespace inherits; pinned host validates, trace applies. Wrong-session internal connection rejects. Session.Close rejects cached prepares too. |
| Statement.Kind, PreparedInfo.Kind, RoutingInfo.Kind | Unknown/ordinary/conditional. No conditional CQL inference. Negotiated prepared metadata refines unknown; confirmed conditional overrides an ordinary declaration. Bind/Batch retain classification; serial consistency promotes effective kind. |
| StreamOptions.Options, Limits | Independent StreamRowDecoder, same canonical Bound input/admission. Inherited prefetch/speculation disabled; explicit nonzero prefetch rejects. Limits copied once: 1 MiB metadata, 32 KiB read-ahead/discard, 262158 transform; zeros select defaults, invalid bounds reject. Capacity is caller owned and never silently grown. |

Profiles/options/canonical inputs never change when their caller replaces defaults
or edits descriptor arrays after admission. Manual execution and explicit
unsupported settings return typed errors before execution submission. Required
stream modes and types are implemented; capability errors cover genuinely
unsupported frame-compressed receive rather than substituting for required modes.
