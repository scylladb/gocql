// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	implSettings "github.com/scylladb/gocql/v2/internal/settings"

	lz4 "github.com/scylladb/gocql/lz4"

	logging "github.com/scylladb/gocql/v2/internal/logging"
	"time"
)

func InternalConnectCluster(ctx context.Context, config Config, initialize func(*Session, Config) *Config) (*Session, error) {
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
	if config.Logger != nil {
		config.Logger = logging.InternalV2SafeLogger{InternalLogger: config.Logger}
	}
	contacts := config.ContactPoints
	if path, _ := unixContact(config); path != "" {
		contacts = []string{"127.0.0.1"}
	}
	legacy := InternalNewClusterSettings(contacts...)
	profile := config.DefaultProfile.Snapshot()
	legacy.privateStorage.InternalDefaultProfile = profile
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
	legacy.privateStorage.InternalDisableClientRouteEvents = !config.Metadata.ClientRouteEvents
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
			snapshot, err := InternalSnapshotV2Host(nil, host)
			return err == nil && config.HostFilter(snapshot)
		})
	}
	hostDialer := &v2TLSHostDialer{InternalConfig: implSettings.InternalConnectivityConfig(config)}
	legacy.HostDialer = hostDialer
	var typedSession *Session
	if path, _ := unixContact(config); path != "" {
		hostDialer.InternalUnixHostID = func() UUID {
			if typedSession != nil && typedSession.privateStorage.InternalControl != nil {
				if current := typedSession.privateStorage.InternalControl.InternalGetConn(); current != nil && current.InternalHost != nil {
					return implEndpoint.InternalHostInfoHostUUID(current.InternalHost)
				}
			}
			return UUID{}
		}
		previous := legacy.HostFilter
		legacy.HostFilter = HostFilterFunc(func(host *HostInfo) bool {
			if local := hostDialer.InternalUnixHostID(); !local.IsEmpty() && !implEndpoint.InternalHostInfoHostUUID(host).IsEmpty() && implEndpoint.InternalHostInfoHostUUID(host) != local {
				return false
			}
			return previous == nil || previous.Accept(host)
		})
	}
	resolver := &v2DNSResolver{InternalBase: config.DNSResolver, InternalTimeout: config.DNSTimeout, InternalSetupTimeout: config.ConnectTimeout}
	resolver.InternalStartup.Store(&contextBox{InternalCtx: ctx})
	legacy.DNSResolver = resolver
	if config.ClientRoutes != nil {
		endpoints := make(ClientRoutesEndpointList, 0, len(config.ClientRoutes.ConnectionIDs))
		for _, id := range config.ClientRoutes.ConnectionIDs {
			endpoints = append(endpoints, ClientRoutesEndpoint{ConnectionID: id, ConnectionAddr: config.ClientRoutes.HostnameOverrides[id]})
		}
		legacy.ClientRoutesConfig = &ClientRoutesConfig{TableName: "system.client_routes", Endpoints: endpoints}
	}
	s, err := newSessionCommonWithRuntime(*legacy, &config, initialize)
	if err != nil {
		return nil, err
	}
	typedSession = s

	stop := context.AfterFunc(ctx, s.privateStorage.InternalCancel)
	err = s.init()
	stopped := stop()
	resolver.InternalClearStartup()
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
	s.privateStorage.readyCh <- struct{}{}
	close(s.privateStorage.readyCh)
	return s, nil
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
		case <-s.privateStorage.InternalCtx.Done():
			return
		case now := <-timer.C:
			for _, host := range s.privateStorage.InternalHostSource.InternalGetHostsList() {
				id := implEndpoint.InternalHostInfoHostUUID(host)
				if host.IsUp() {
					delete(retries, id)
					continue
				}
				state := retries[id]
				if state.stopped || now.Before(state.next) {
					continue
				}
				s.privateStorage.InternalPool.InternalAddHost(host)
				state.attempt++
				delay, again := policy.Delay(state.attempt)
				state.stopped = !again || delay < 0
				state.next = now.Add(max(delay, time.Millisecond))
				retries[id] = state
			}
		}
	}
}
