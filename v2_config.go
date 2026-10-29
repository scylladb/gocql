package gocql

import (
	"context"
	"crypto/tls"
	lz4 "github.com/scylladb/gocql/lz4"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync/atomic"
	"time"
)

type Config struct {
	// DialHost opens a raw socket using immutable host metadata; TLS is applied afterward.
	DialHost                                          func(context.Context, DialTarget) (net.Conn, error)
	ReadTimeout                                       time.Duration
	ContactPoints                                     []string
	Port                                              uint16
	Protocol                                          int
	Keyspace, CQLVersion                              string
	ConnectTimeout, DNSTimeout                        time.Duration
	WriteTimeout, KeepaliveInterval, KeepaliveTimeout time.Duration
	SocketKeepalive                                   time.Duration
	NoDelay                                           bool
	ReceiveBuffer, SendBuffer                         Option[int]
	Linger                                            Option[time.Duration]
	LocalAddress                                      netip.Addr
	Dialer                                            Dialer
	DNSResolver                                       DNSResolver
	TLS                                               *tls.Config
	TLSForHost                                        func(context.Context, HostSnapshot) (*tls.Config, error)
	Auth                                              AuthProvider
	Compression                                       Compression
	CoalesceWait                                      time.Duration
	Pool                                              PoolOptions
	Reconnect                                         ReconnectPolicy
	Conviction                                        ConvictionPolicy
	AddressTranslator                                 AddressTranslator
	HostFilter                                        func(HostSnapshot) bool
	ClientRoutes                                      *ClientRoutesOptions
	Identity                                          IdentityOptions
	ReportConfiguration                               bool
	Metadata                                          MetadataOptions
	Limits                                            ProtocolLimits
	PreparedCacheSize                                 int
	DefaultProfile                                    *ProfileHandle
	Partitioners                                      map[string]TokenPartitioner
	TimestampGenerator                                func() int64
	Observer                                          Observer
	LifecycleObserver                                 LifecycleObserver
	Logger                                            StdLogger
}
type Compression uint8

const (
	NoCompression Compression = iota
	LZ4Compression
	SnappyCompression
)

type PoolOptions struct {
	ConnectionsPerHost, ConnectionsPerShard int
	MaxExcessPerShard                       int
	MaxRequestsPerConnection                int
	MaxOrphanedStreams                      int
	ShardAwarePort                          bool
	SourcePortStart, SourcePortEnd          uint16
}
type ReconnectPolicy interface {
	Delay(attempt int) (time.Duration, bool)
}
type ProtocolLimits struct {
	MaxFrameBytes, MaxDecompressedBytes int64
	MaxMetadataDepth                    int
	MaxPagingStates                     int
	MaxPagingStateBytes                 int64
}
type MetadataOptions struct {
	Mode                                                          MetadataMode
	Keyspaces                                                     []string
	InitialDiscovery                                              bool
	PeriodicRefresh                                               time.Duration
	RequestTimeout                                                time.Duration
	ServerTimeout                                                 Option[time.Duration]
	AgreementTimeout, AgreementPoll                               time.Duration
	AutomaticAgreement, RefreshAfterAgreement                     bool
	TopologyEvents, StatusEvents, SchemaEvents, ClientRouteEvents bool
}
type MetadataMode uint8

const (
	MetadataDisabled MetadataMode = iota
	MetadataMinimal
	MetadataFull
)

type IdentityOptions struct{ DriverName, DriverVersion, ApplicationName, ApplicationVersion, ClientID string }
type ClientRoutesOptions struct {
	ConnectionIDs     []string
	HostnameOverrides map[string]string
	TLS               bool
	SNIProxyAddress   string
}

func DefaultConfig(contactPoints ...string) Config {
	profile := defaultProfile()
	profile.Routing, _ = NewRoutingPolicy(RoutingOptions{TokenAware: true, ShuffleReplicas: true})
	handle, _ := NewProfile(profile)
	return Config{ContactPoints: append([]string(nil), contactPoints...), Port: 9042, CQLVersion: "3.0.0", ConnectTimeout: 5 * time.Second, DNSTimeout: 5 * time.Second, KeepaliveInterval: 30 * time.Second, KeepaliveTimeout: 30 * time.Second, NoDelay: true, Pool: PoolOptions{ConnectionsPerShard: 1, MaxExcessPerShard: 2, MaxRequestsPerConnection: 32768, MaxOrphanedStreams: 1024, ShardAwarePort: true, SourcePortStart: 49152, SourcePortEnd: 65535}, Metadata: MetadataOptions{Mode: MetadataFull, InitialDiscovery: true, RequestTimeout: 60 * time.Second, AgreementTimeout: 60 * time.Second, AgreementPoll: 200 * time.Millisecond, AutomaticAgreement: true, RefreshAfterAgreement: true, TopologyEvents: true, StatusEvents: true, SchemaEvents: true, ClientRouteEvents: true}, Limits: ProtocolLimits{MaxFrameBytes: 256 << 20, MaxDecompressedBytes: 256 << 20, MaxMetadataDepth: 64, MaxPagingStates: 65536, MaxPagingStateBytes: 16 << 20}, PreparedCacheSize: 1000, Partitioners: map[string]TokenPartitioner{"org.apache.cassandra.dht.Murmur3Partitioner": Murmur3TokenPartitioner, "Murmur3Partitioner": Murmur3TokenPartitioner, "com.scylladb.dht.CDCPartitioner": CDCTokenPartitioner, "CDCPartitioner": CDCTokenPartitioner}, DefaultProfile: handle, Reconnect: defaultReconnectPolicy{}, ReportConfiguration: true, TimestampGenerator: newTimestampGenerator()}
}
func newTimestampGenerator() func() int64 {
	var last atomic.Int64
	return func() int64 {
		for {
			prev := last.Load()
			now := time.Now().UnixMicro()
			if now <= prev {
				now = prev + 1
			}
			if last.CompareAndSwap(prev, now) {
				return now
			}
		}
	}
}
func validateConfig(config Config) error {
	if _, err := unixContact(config); err != nil {
		return err
	}
	if len(config.ContactPoints) == 0 {
		return typeValidation("contact points", "at least one contact point is required")
	}
	if config.Protocol != 0 && config.Protocol != 3 && config.Protocol != 4 && config.Protocol != 5 {
		return typeValidation("protocol", "supported versions are 3, 4 and 5")
	}
	if config.Port == 0 {
		return typeValidation("port", "must not be zero")
	}
	if config.ReadTimeout < 0 || config.ConnectTimeout < 0 || config.DNSTimeout < 0 || config.WriteTimeout < 0 || config.KeepaliveInterval < 0 || config.KeepaliveTimeout < 0 || config.SocketKeepalive < 0 || config.CoalesceWait < 0 {
		return typeValidation("timeouts", "must not be negative")
	}
	if config.Protocol == 5 && config.Compression == SnappyCompression {
		return &UnsupportedSettingError{Setting: "compression", Reason: "native protocol5 supports raw or LZ4 segments"}
	}
	if config.Compression > SnappyCompression {
		return typeValidation("compression", "unknown algorithm")
	}
	if config.DefaultProfile == nil {
		return typeValidation("profile", "default profile is required; use DefaultConfig")
	}
	if err := validateProfile(config.DefaultProfile.Snapshot()); err != nil {
		return err
	}
	if config.Limits.MaxFrameBytes <= 0 || config.Limits.MaxDecompressedBytes <= 0 || config.Limits.MaxMetadataDepth <= 0 {
		return typeValidation("protocol limits", "must be positive")
	}
	if config.Limits.MaxPagingStates <= 0 || config.Limits.MaxPagingStateBytes <= 0 {
		return typeValidation("paging limits", "must be positive")
	}
	if config.Limits.MaxMetadataDepth > 128 || config.Limits.MaxFrameBytes > 1<<31-1 || config.Limits.MaxDecompressedBytes > 1<<31-1 {
		return typeValidation("protocol limits", "limits exceed supported protocol range")
	}
	if config.Linger.Set && (config.Linger.Value < 0 || config.Linger.Value%time.Second != 0 || config.Linger.Value/time.Second > math.MaxInt32) {
		return typeValidation("linger", "must be nonnegative whole seconds within int32 range")
	}
	if config.ReceiveBuffer.Set && config.ReceiveBuffer.Value < 0 || config.SendBuffer.Set && config.SendBuffer.Value < 0 {
		return typeValidation("socket buffers", "must not be negative")
	}
	if config.Metadata.Mode > MetadataFull {
		return typeValidation("metadata mode", "unknown mode")
	}
	if config.Metadata.PeriodicRefresh < 0 || config.Metadata.RequestTimeout < 0 || config.Metadata.AgreementTimeout < 0 || config.Metadata.AgreementPoll < 0 || config.Metadata.ServerTimeout.Set && config.Metadata.ServerTimeout.Value < 0 {
		return typeValidation("metadata timeouts", "must not be negative")
	}
	if routes := config.ClientRoutes; routes != nil {
		if len(routes.ConnectionIDs) == 0 {
			return typeValidation("client routes", "connection IDs are required")
		}
		ids := make(map[string]bool, len(routes.ConnectionIDs))
		for _, id := range routes.ConnectionIDs {
			if id == "" || ids[id] {
				return typeValidation("client routes", "empty or duplicate connection ID")
			}
			ids[id] = true
		}
		for id, host := range routes.HostnameOverrides {
			if !ids[id] || host == "" {
				return typeValidation("client routes", "override requires configured connection ID and hostname")
			}
		}
		if config.AddressTranslator != nil || config.Pool.ShardAwarePort {
			return typeValidation("client routes", "user address translator and source-port shard dialing are incompatible")
		}
		if routes.TLS && (config.TLS == nil && config.TLSForHost == nil || config.TLS != nil && config.TLS.InsecureSkipVerify && config.TLS.VerifyConnection == nil && config.TLS.VerifyPeerCertificate == nil) {
			return typeValidation("client routes TLS", "verified TLS configuration required")
		}
		if routes.SNIProxyAddress != "" && !routes.TLS {
			return typeValidation("client routes proxy", "requires verified TLS")
		}
	}
	keys := make(map[string]bool, len(config.Metadata.Keyspaces))
	for _, keyspace := range config.Metadata.Keyspaces {
		if keyspace == "" || keys[keyspace] {
			return typeValidation("metadata keyspaces", "empty or duplicate keyspace")
		}
		keys[keyspace] = true
	}
	for name, partitioner := range config.Partitioners {
		if name == "" || partitioner == nil {
			return typeValidation("partitioners", "empty name or nil partitioner")
		}
	}
	if config.PreparedCacheSize < 0 {
		return typeValidation("prepared cache", "must not be negative")
	}
	if config.Pool.ConnectionsPerHost < 0 || config.Pool.ConnectionsPerShard < 0 || config.Pool.MaxExcessPerShard < 0 || config.Pool.MaxRequestsPerConnection <= 0 || config.Pool.MaxRequestsPerConnection > 32768 || config.Pool.MaxOrphanedStreams < 0 {
		return typeValidation("pool", "invalid pool limits")
	}
	if config.Pool.ConnectionsPerHost == 0 && config.Pool.ConnectionsPerShard == 0 {
		return typeValidation("pool", "host or shard connection count must be positive")
	}
	if config.Pool.ShardAwarePort && (config.Pool.SourcePortStart < 1024 || config.Pool.SourcePortStart > config.Pool.SourcePortEnd) {
		return typeValidation("source ports", "invalid range")
	}
	return nil
}
func snapshotConfig(config Config) Config {
	config.ContactPoints = append([]string(nil), config.ContactPoints...)
	config.Metadata.Keyspaces = append([]string(nil), config.Metadata.Keyspaces...)
	if config.TLS != nil {
		config.TLS = cloneV2TLSConfig(config.TLS)
	}
	if config.ClientRoutes != nil {
		routes := *config.ClientRoutes
		routes.ConnectionIDs = append([]string(nil), routes.ConnectionIDs...)
		routes.HostnameOverrides = maps.Clone(routes.HostnameOverrides)
		config.ClientRoutes = &routes
	}
	if config.Partitioners != nil {
		registry := make(map[string]TokenPartitioner, len(config.Partitioners))
		for key, value := range config.Partitioners {
			registry[key] = value
		}
		config.Partitioners = registry
	}
	return config
}
func Connect(ctx context.Context, config Config) (*Session, error) {
	if ctx == nil {
		return nil, typeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	config = snapshotConfig(config)
	contacts := config.ContactPoints
	if path, _ := unixContact(config); path != "" {
		contacts = []string{"127.0.0.1"}
	}
	legacy := NewCluster(contacts...)
	profile := config.DefaultProfile.Snapshot()
	legacy.Port = int(config.Port)
	legacy.ProtoVersion = config.Protocol
	legacy.Keyspace = config.Keyspace
	legacy.CQLVersion = config.CQLVersion
	legacy.ConnectTimeout = config.ConnectTimeout
	legacy.WriteTimeout = config.WriteTimeout
	legacy.ReadTimeout = config.ReadTimeout
	legacy.SocketKeepalive = config.SocketKeepalive
	legacy.WriteCoalesceWaitTime = config.CoalesceWait
	legacy.Timeout = profile.Timeout
	legacy.Consistency = profile.Consistency
	legacy.PageSize = int(profile.PageSize)
	legacy.MaxPreparedStmts = config.PreparedCacheSize
	legacy.MaxRequestsPerConn = config.Pool.MaxRequestsPerConnection
	legacy.MaxExcessShardConnectionsRate = float32(config.Pool.MaxExcessPerShard)
	legacy.DisableShardAwarePort = !config.Pool.ShardAwarePort || config.Pool.ConnectionsPerHost > 0
	legacy.NumConns = config.Pool.ConnectionsPerHost
	if legacy.NumConns == 0 {
		legacy.NumConns = config.Pool.ConnectionsPerShard
	}
	legacy.DisableInitialHostLookup = !config.Metadata.InitialDiscovery
	legacy.MetadataSchemaRequestTimeout = config.Metadata.RequestTimeout
	legacy.MaxWaitSchemaAgreement = config.Metadata.AgreementTimeout
	legacy.Events.DisableNodeStatusEvents = !config.Metadata.StatusEvents
	legacy.Events.DisableTopologyEvents = !config.Metadata.TopologyEvents
	legacy.Events.DisableSchemaEvents = !config.Metadata.SchemaEvents
	legacy.disableClientRouteEvents = !config.Metadata.ClientRouteEvents
	legacy.DisableDriverConfigReporting = !config.ReportConfiguration
	legacy.AddressTranslator = config.AddressTranslator
	legacy.Logger = config.Logger
	if config.Conviction != nil {
		legacy.ConvictionPolicy = config.Conviction
	}
	legacy.ReconnectionPolicy = &NoReconnectionPolicy{}
	legacy.ReconnectInterval = 0
	if config.Reconnect != nil {
		delay, again := config.Reconnect.Delay(0)
		if delay < 0 {
			return nil, typeValidation("reconnect delay", "must not be negative")
		}
		if again {
			legacy.ReconnectInterval = max(delay, time.Millisecond)
		}
	}
	legacy.DriverName = config.Identity.DriverName
	legacy.DriverVersion = config.Identity.DriverVersion
	legacy.ApplicationInfo = NewStaticApplicationInfo(config.Identity.ApplicationName, config.Identity.ApplicationVersion, config.Identity.ClientID)
	if config.TLS != nil {
		legacy.SslOpts = &SslOptions{Config: config.TLS, EnableHostVerification: !config.TLS.InsecureSkipVerify}
	}
	switch config.Compression {
	case LZ4Compression:
		legacy.Compressor = lz4.LZ4Compressor{}
	case SnappyCompression:
		legacy.Compressor = SnappyCompressor{}
	}
	if config.HostFilter != nil {
		legacy.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
			snapshot, err := snapshotV2Host(nil, host)
			return err == nil && config.HostFilter(snapshot)
		})
	}
	hostDialer := &v2TLSHostDialer{config: config, legacy: legacy}
	legacy.HostDialer = hostDialer
	var typedSession *Session
	if path, _ := unixContact(config); path != "" {
		hostDialer.unixHostID = func() UUID {
			if typedSession != nil && typedSession.control != nil {
				if current := typedSession.control.getConn(); current != nil && current.host != nil {
					return current.host.hostUUID()
				}
			}
			return UUID{}
		}
		previous := legacy.HostFilter
		legacy.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
			if local := hostDialer.unixHostID(); !local.IsEmpty() && !host.hostUUID().IsEmpty() && host.hostUUID() != local {
				return false
			}
			return previous == nil || previous.Accept(host)
		})
	}
	resolver := &v2DNSResolver{base: config.DNSResolver, timeout: config.DNSTimeout, setupTimeout: config.ConnectTimeout}
	resolver.startup.Store(&contextBox{ctx: ctx})
	legacy.DNSResolver = resolver
	if config.ClientRoutes != nil {
		endpoints := make(ClientRoutesEndpointList, 0, len(config.ClientRoutes.ConnectionIDs))
		for _, id := range config.ClientRoutes.ConnectionIDs {
			endpoints = append(endpoints, ClientRoutesEndpoint{ConnectionID: id, ConnectionAddr: config.ClientRoutes.HostnameOverrides[id]})
		}
		legacy.ClientRoutesConfig = &ClientRoutesConfig{TableName: "system.client_routes", Endpoints: endpoints}
	}
	s, err := newSessionCommon(*legacy)
	if err != nil {
		return nil, err
	}
	typedSession = s
	rt := s.typedRuntime()
	rt.profile = config.DefaultProfile
	rt.config = config
	s.connCfg.TypedAuth = config.Auth
	s.connCfg.TypedLimits = config.Limits
	s.connCfg.TypedConfig = &rt.config
	s.configureV2Peripheral(config)
	if s.clientRoutesHandler != nil {
		s.clientRoutesHandler.pickTLSPorts = config.ClientRoutes.TLS
	}
	stop := context.AfterFunc(ctx, s.cancel)
	err = s.init()
	stopped := stop()
	resolver.clearStartup()
	if !stopped || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
	}
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	if err = s.initV2Peripheral(config); err != nil {
		_ = s.Close()
		return nil, err
	}
	s.readyCh <- struct{}{}
	close(s.readyCh)
	return s, nil
}
func OpenSingleHost(ctx context.Context, config Config, hostID UUID) (*Session, error) {
	if hostID.IsEmpty() {
		return nil, typeValidation("host ID", "empty UUID")
	}
	previous := config.HostFilter
	config.HostFilter = func(host HostSnapshot) bool { return host.ID == hostID && (previous == nil || previous(host)) }
	return Connect(ctx, config)
}

type contextBox struct{ ctx context.Context }
type v2DNSResolver struct {
	base                  DNSResolver
	timeout, setupTimeout time.Duration
	startup               atomic.Pointer[contextBox]
}

func (r *v2DNSResolver) baseContext() context.Context {
	if box := r.startup.Load(); box != nil {
		return box.ctx
	}
	return context.Background()
}
func (r *v2DNSResolver) LookupIP(host string) ([]net.IP, error) {
	return r.lookupIP(r.baseContext(), host)
}
func (r *v2DNSResolver) lookupSetup(host string) ([]net.IP, time.Time, error) {
	ctx := r.baseContext()
	cancel := func() {}
	if r.setupTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, r.setupTimeout)
	}
	defer cancel()
	deadline, _ := ctx.Deadline()
	ips, err := r.lookupIP(ctx, host)
	return ips, deadline, err
}
func (r *v2DNSResolver) lookupIP(ctx context.Context, host string) ([]net.IP, error) {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	if r.base == nil {
		return net.DefaultResolver.LookupIP(ctx, "ip", host)
	}
	type outcome struct {
		ips []net.IP
		err error
	}
	done := make(chan outcome, 1)
	go func() { ips, err := r.base.LookupIP(host); done <- outcome{ips, err} }()
	select {
	case result := <-done:
		return result.ips, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (r *v2DNSResolver) clearStartup() { r.startup.Store(nil) }

type defaultReconnectPolicy struct{}

func (defaultReconnectPolicy) Delay(attempt int) (time.Duration, bool) {
	delay := time.Second << min(max(attempt, 0), 5)
	return delay/2 + time.Duration(rand.Int64N(int64(delay/2))), true
}

// reconnectTypedDownedHosts keeps independent bounded backoff state for each host.
func (s *Session) reconnectTypedDownedHosts(policy ReconnectPolicy, initial time.Duration) {
	type retryState struct {
		attempt int
		next    time.Time
		stopped bool
	}
	retries := make(map[UUID]retryState)
	tick := min(max(initial, time.Millisecond), time.Second)
	timer := time.NewTicker(tick)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-timer.C:
			for _, host := range s.hostSource.getHostsList() {
				id := host.hostUUID()
				if host.IsUp() {
					delete(retries, id)
					continue
				}
				state := retries[id]
				if state.stopped || now.Before(state.next) {
					continue
				}
				s.pool.addHost(host)
				state.attempt++
				delay, again := policy.Delay(state.attempt)
				state.stopped = !again || delay < 0
				state.next = now.Add(max(delay, time.Millisecond))
				retries[id] = state
			}
		}
	}
}

func (r *v2DNSResolver) LookupIPContext(ctx context.Context, host string) ([]net.IP, error) {
	return r.lookupIP(ctx, host)
}
