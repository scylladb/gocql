/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql/internal/debug"
	"github.com/gocql/gocql/internal/eventbus"
)

const defaultDriverName = "ScyllaDB GoCQL Driver"

// PoolConfig configures the connection pool used by the driver, it defaults to
// using a round-robin host selection policy and a round-robin connection selection
// policy for each host.
type PoolConfig struct {
	// HostSelectionPolicy sets the policy for selecting which host to use for a
	// given query (default: RoundRobinHostPolicy())
	// It is not supported to use a single HostSelectionPolicy in multiple sessions
	// (even if you close the old session before using in a new session).
	HostSelectionPolicy HostSelectionPolicy
}

func (p PoolConfig) buildPool(session *Session) *policyConnPool {
	return newPolicyConnPool(session)
}

// ClusterConfig is a struct to configure the default cluster implementation
// of gocql. It has a variety of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that require a
// different setup must implement their own cluster.
type ClusterConfig struct {
	BatchObserver                 BatchObserver
	Dialer                        Dialer
	ApplicationInfo               ApplicationInfo
	DNSResolver                   DNSResolver
	Logger                        StdLogger
	HostDialer                    HostDialer
	StreamObserver                StreamObserver
	FrameHeaderObserver           FrameHeaderObserver
	ConnectObserver               ConnectObserver
	QueryObserver                 QueryObserver
	AddressTranslator             AddressTranslator
	HostFilter                    HostFilter
	Compressor                    Compressor
	Authenticator                 Authenticator
	actualSslOpts                 atomic.Value
	PoolConfig                    PoolConfig
	RetryPolicy                   RetryPolicy
	ConvictionPolicy              ConvictionPolicy
	ReconnectionPolicy            ReconnectionPolicy
	InitialReconnectionPolicy     ReconnectionPolicy
	WarningsHandlerBuilder        WarningHandlerBuilder
	SslOpts                       *SslOptions
	AuthProvider                  func(h *HostInfo) (Authenticator, error)
	PortMuxConfig                 PortMuxConfig
	DriverVersion                 string
	DriverName                    string
	Keyspace                      string
	CQLVersion                    string
	Hosts                         []string
	WriteCoalesceWaitTime         time.Duration
	WriteTimeout                  time.Duration
	SocketKeepalive               time.Duration
	ReconnectInterval             time.Duration
	MaxWaitSchemaAgreement        time.Duration
	ProtoVersion                  int
	MaxRequestsPerConn            int
	Timeout                       time.Duration
	MetadataSchemaRequestTimeout  time.Duration
	ConnectTimeout                time.Duration
	Port                          int
	NumConns                      int
	MaxPreparedStmts              int
	PageSize                      int
	MaxRoutingKeyInfo             int
	ReadTimeout                   time.Duration
	EventBusConfig                eventbus.EventBusConfig
	MaxExcessShardConnectionsRate float32
	SerialConsistency             Consistency
	Consistency                   Consistency
	Events                        struct {
		DisableNodeStatusEvents bool
		DisableTopologyEvents   bool
		DisableSchemaEvents     bool
	}
	DefaultIdempotence       bool
	DefaultTimestamp         bool
	DisableSkipMetadata      bool
	DisableShardAwarePort    bool
	DisableInitialHostLookup bool
	disableControlConn       bool
	disableInit              bool
	IgnorePeerAddr           bool // disable registering for status events (node up/down)
	// disable registering for schema events (keyspace/table/function removed/created/updated)
}

type DNSResolver interface {
	LookupIP(host string) ([]net.IP, error)
}

type ApplicationInfo interface {
	UpdateStartupOptions(map[string]string)
}

type StaticApplicationInfo struct {
	applicationName    string
	applicationVersion string
	clientID           string
}

func NewStaticApplicationInfo(name, version, clientID string) *StaticApplicationInfo {
	return &StaticApplicationInfo{
		applicationName:    name,
		applicationVersion: version,
		clientID:           clientID,
	}
}

func (i *StaticApplicationInfo) UpdateStartupOptions(opts map[string]string) {
	if i.applicationName != "" {
		opts["APPLICATION_NAME"] = i.applicationName
	}
	if i.applicationVersion != "" {
		opts["APPLICATION_VERSION"] = i.applicationVersion
	}
	if i.clientID != "" {
		opts["CLIENT_ID"] = i.clientID
	}
}

type SimpleDNSResolver struct {
	hostLookupPreferV4 bool
}

func NewSimpleDNSResolver(hostLookupPreferV4 bool) *SimpleDNSResolver {
	return &SimpleDNSResolver{
		hostLookupPreferV4,
	}
}

func (r SimpleDNSResolver) LookupIP(host string) ([]net.IP, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	// Filter to v4 addresses if any present
	if r.hostLookupPreferV4 {
		var preferredIPs []net.IP
		for _, v := range ips {
			if v4 := v.To4(); v4 != nil {
				preferredIPs = append(preferredIPs, v4)
			}
		}
		if len(preferredIPs) != 0 {
			ips = preferredIPs
		}
	}
	return ips, nil
}

var defaultDnsResolver = NewSimpleDNSResolver(os.Getenv("GOCQL_HOST_LOOKUP_PREFER_V4") == "true")

type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// NewCluster generates a new config for the default cluster implementation.
//
// The supplied hosts are used to initially connect to the cluster then the rest of
// the ring will be automatically discovered. It is recommended to use the value set in
// the Cassandra config for broadcast_address or listen_address, an IP address not
// a domain name. This is because events from Cassandra will use the configured IP
// address, which is used to index connected hosts. If the domain name specified
// resolves to more than 1 IP address then the driver may connect multiple times to
// the same host, and will not mark the node being down or up from events.
func NewCluster(hosts ...string) *ClusterConfig {
	logger := &defaultLogger{}
	cfg := &ClusterConfig{
		Hosts:                         hosts,
		CQLVersion:                    "3.0.0",
		Timeout:                       11 * time.Second,
		ConnectTimeout:                60 * time.Second,
		ReadTimeout:                   11 * time.Second,
		WriteTimeout:                  11 * time.Second,
		Port:                          9042,
		MaxExcessShardConnectionsRate: 2,
		NumConns:                      2,
		Consistency:                   Quorum,
		MaxPreparedStmts:              defaultMaxPreparedStmts,
		MaxRoutingKeyInfo:             1000,
		PageSize:                      5000,
		DefaultTimestamp:              true,
		DriverName:                    defaultDriverName,
		DriverVersion:                 defaultDriverVersion,
		MaxWaitSchemaAgreement:        60 * time.Second,
		ReconnectInterval:             60 * time.Second,
		ConvictionPolicy:              &SimpleConvictionPolicy{},
		ReconnectionPolicy:            &ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second},
		InitialReconnectionPolicy:     &NoReconnectionPolicy{},
		SocketKeepalive:               15 * time.Second,
		WriteCoalesceWaitTime:         200 * time.Microsecond,
		MetadataSchemaRequestTimeout:  60 * time.Second,
		DisableSkipMetadata:           true,
		WarningsHandlerBuilder:        DefaultWarningHandlerBuilder,
		Logger:                        logger,
		DNSResolver:                   defaultDnsResolver,
		EventBusConfig: eventbus.EventBusConfig{
			InputEventsQueueSize: 10240,
		},
	}

	return cfg
}

func (cfg *ClusterConfig) logger() StdLogger {
	if cfg.Logger == nil {
		return &defaultLogger{}
	}
	return cfg.Logger
}

// CreateSession initializes the cluster based on this config and returns a
// session object that can be used to interact with the database.
func (cfg *ClusterConfig) CreateSession() (*Session, error) {
	return NewSession(*cfg)
}

func (cfg *ClusterConfig) CreateSessionNonBlocking() (*Session, error) {
	return NewSessionNonBlocking(*cfg)
}

type addressTranslateFn func(hostID string, addr net.IP, port int) (net.IP, int)

// translateAddressPort is a helper method that will use the given AddressTranslator
// if defined, to translate the given address and port into a possibly new address
// and port, If no AddressTranslator or if an error occurs, the given address and
// port will be returned.
func (cfg *ClusterConfig) translateAddressPort(hostID string, addr net.IP, port int) (net.IP, int) {
	if cfg.AddressTranslator == nil || len(addr) == 0 {
		return addr, port
	}
	translatorV2, ok := cfg.AddressTranslator.(AddressTranslatorV2)
	if !ok {
		newAddr, newPort := cfg.AddressTranslator.Translate(addr, port)
		if debug.Enabled {
			cfg.logger().Printf("gocql: translating address '%v:%d' to '%v:%d'", addr, port, newAddr, newPort)
		}
		return newAddr, newPort
	}
	newAddr, newPort := translatorV2.TranslateWithHostID(hostID, addr, port)
	if debug.Enabled {
		cfg.logger().Printf("gocql: translating address '%v:%d' to '%v:%d'", addr, port, newAddr, newPort)
	}
	return newAddr, newPort
}

func (cfg *ClusterConfig) filterHost(host *HostInfo) bool {
	return !(cfg.HostFilter == nil || cfg.HostFilter.Accept(host))
}

func (cfg *ClusterConfig) ValidateAndInitSSL() error {
	if cfg.SslOpts == nil {
		return nil
	}
	actualTLSConfig, err := setupTLSConfig(cfg.SslOpts)
	if err != nil {
		return fmt.Errorf("failed to initialize ssl configuration: %s", err.Error())
	}

	cfg.actualSslOpts.Store(actualTLSConfig)
	return nil
}

func (cfg *ClusterConfig) getActualTLSConfig() *tls.Config {
	val, ok := cfg.actualSslOpts.Load().(*tls.Config)
	if !ok {
		return nil
	}
	return val.Clone()
}

type ClusterOption func(*ClusterConfig)

func (cfg *ClusterConfig) WithOptions(opts ...ClusterOption) *ClusterConfig {
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

type PortMuxOption func(*PortMuxConfig)

func WithMaxResolverConcurrency(val int) func(*PortMuxConfig) {
	return func(cfg *PortMuxConfig) {
		cfg.MaxResolverConcurrency = val
	}
}

func WithResolveHealthyEndpointPeriod(val time.Duration) func(*PortMuxConfig) {
	return func(cfg *PortMuxConfig) {
		cfg.ResolveHealthyEndpointPeriod = val
	}
}

func WithEndpoints(endpoints ...PrivateLinkEndpoint) func(*PortMuxConfig) {
	return func(cfg *PortMuxConfig) {
		cfg.Endpoints = endpoints
	}
}

func WithTable(tableName string) func(*PortMuxConfig) {
	return func(cfg *PortMuxConfig) {
		cfg.TableName = tableName
	}
}

func WithPortMux(opts ...PortMuxOption) func(*ClusterConfig) {
	pmCfg := PortMuxConfig{
		Enabled: true,
		// Don't resolve healthy nodes by default
		ResolveHealthyEndpointPeriod: 0,
		MaxResolverConcurrency:       1,
		TableName:                    "system.client_routes",
		DNSResolver: newSimplePortMuxResolver(
			time.Minute,
			time.Millisecond*500,
			defaultDnsResolver,
		),
	}
	for _, opt := range opts {
		opt(&pmCfg)
	}
	return func(cfg *ClusterConfig) {
		cfg.PortMuxConfig = pmCfg
		if len(cfg.Hosts) == 0 {
			for _, ep := range pmCfg.Endpoints {
				if ep.connectionAddr != "" {
					cfg.Hosts = append(cfg.Hosts, ep.connectionAddr)
				}
			}
		}
		// TODO: cfg.ControlConnectionOnlyToInitialNodes
	}
}

func (cfg *ClusterConfig) Validate() error {
	if len(cfg.Hosts) == 0 {
		return ErrNoHosts
	}

	if cfg.Authenticator != nil && cfg.AuthProvider != nil {
		return errors.New("Can't use both Authenticator and AuthProvider in cluster config.")
	}

	if cfg.InitialReconnectionPolicy == nil {
		return errors.New("InitialReconnectionPolicy is nil")
	}

	if cfg.InitialReconnectionPolicy.GetMaxRetries() <= 0 {
		return errors.New("InitialReconnectionPolicy.GetMaxRetries returns negative number")
	}

	if cfg.ReconnectionPolicy == nil {
		return errors.New("ReconnectionPolicy is nil")
	}

	if cfg.InitialReconnectionPolicy.GetMaxRetries() <= 0 {
		return errors.New("ReconnectionPolicy.GetMaxRetries returns negative number")
	}

	if cfg.PageSize < 0 {
		return errors.New("PageSize should be positive number or zero")
	}

	if cfg.MaxRoutingKeyInfo < 0 {
		return errors.New("MaxRoutingKeyInfo should be positive number or zero")
	}

	if cfg.MaxPreparedStmts < 0 {
		return errors.New("MaxPreparedStmts should be positive number or zero")
	}

	if cfg.SocketKeepalive < 0 {
		return errors.New("SocketKeepalive should be positive time.Duration or zero")
	}

	if cfg.MaxRequestsPerConn < 0 {
		return errors.New("MaxRequestsPerConn should be positive number or zero")
	}

	if cfg.NumConns < 0 {
		return errors.New("NumConns should be positive non-zero number or zero")
	}

	if cfg.Port <= 0 || cfg.Port > 65535 {
		return errors.New("Port should be a valid port number: a number between 1 and 65535")
	}

	if cfg.WriteTimeout < 0 {
		return errors.New("WriteTimeout should be positive time.Duration or zero")
	}

	if cfg.Timeout < 0 {
		return errors.New("Timeout should be positive time.Duration or zero")
	}

	if cfg.ConnectTimeout < 0 {
		return errors.New("ConnectTimeout should be positive time.Duration or zero")
	}

	if cfg.MetadataSchemaRequestTimeout < 0 {
		return errors.New("MetadataSchemaRequestTimeout should be positive time.Duration or zero")
	}

	if cfg.WriteCoalesceWaitTime < 0 {
		return errors.New("WriteCoalesceWaitTime should be positive time.Duration or zero")
	}

	if cfg.ReconnectInterval < 0 {
		return errors.New("ReconnectInterval should be positive time.Duration or zero")
	}

	if cfg.MaxWaitSchemaAgreement < 0 {
		return errors.New("MaxWaitSchemaAgreement should be positive time.Duration or zero")
	}

	if cfg.ProtoVersion < 0 {
		return errors.New("ProtoVersion should be positive number or zero")
	}

	if !cfg.DisableSkipMetadata {
		cfg.Logger.Println("warning: enabling skipping metadata can lead to unpredictable results when executing query and altering columns involved in the query.")
	}

	if cfg.SerialConsistency > 0 && !cfg.SerialConsistency.IsSerial() {
		return fmt.Errorf("the default SerialConsistency level is not allowed to be anything else but SERIAL or LOCAL_SERIAL. Recived value: %v", cfg.SerialConsistency)
	}

	if cfg.DNSResolver == nil {
		return fmt.Errorf("DNSResolver is empty")
	}

	if cfg.MaxExcessShardConnectionsRate < 0 {
		return fmt.Errorf("MaxExcessShardConnectionsRate should be positive number or zero")
	}

	if err := cfg.PortMuxConfig.Validate(); err != nil {
		return fmt.Errorf("PortMuxConfig is invalid: %v", err)
	}

	return cfg.ValidateAndInitSSL()
}

var (
	ErrNoHosts              = errors.New("no hosts provided")
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	ErrHostQueryFailed      = errors.New("unable to populate Hosts")
)

func setupTLSConfig(sslOpts *SslOptions) (*tls.Config, error) {
	//  Config.InsecureSkipVerify | EnableHostVerification | Result
	//  Config is nil             | true                   | verify host
	//  Config is nil             | false                  | do not verify host
	//  false                     | false                  | verify host
	//  true                      | false                  | do not verify host
	//  false                     | true                   | verify host
	//  true                      | true                   | verify host
	var tlsConfig *tls.Config
	if sslOpts.Config == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: !sslOpts.EnableHostVerification,
			// Ticket max size is 16371 bytes, so it can grow up to 16mb max.
			ClientSessionCache: tls.NewLRUClientSessionCache(1024),
		}
	} else {
		// use clone to avoid race.
		tlsConfig = sslOpts.Config.Clone()
	}

	if tlsConfig.InsecureSkipVerify && sslOpts.EnableHostVerification {
		tlsConfig.InsecureSkipVerify = false
	}

	// ca cert is optional
	if sslOpts.CaPath != "" {
		if tlsConfig.RootCAs == nil {
			tlsConfig.RootCAs = x509.NewCertPool()
		}

		pem, err := ioutil.ReadFile(sslOpts.CaPath)
		if err != nil {
			return nil, fmt.Errorf("unable to open CA certs: %v", err)
		}

		if !tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("failed parsing or CA certs")
		}
	}

	if sslOpts.CertPath != "" || sslOpts.KeyPath != "" {
		mycert, err := tls.LoadX509KeyPair(sslOpts.CertPath, sslOpts.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("unable to load X509 key pair: %v", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, mycert)
	}

	return tlsConfig, nil
}
