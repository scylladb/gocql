package gocql

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql/events"
	"github.com/gocql/gocql/internal/debug"
	"github.com/gocql/gocql/internal/eventbus"
)

type ClientRoutesEndpoint struct {
	// Scylla Cloud ConnectionID to read from `system.client_routes`
	ConnectionID string

	// Ip Address or DNS name of the AWS endpoint
	// Could stay empty, in this case driver will pick it up from system.client_routes table
	ConnectionAddr string
}

func (e ClientRoutesEndpoint) Validate() error {
	if e.ConnectionID == "" {
		return errors.New("missing ConnectionID")
	}
	return nil
}

type ClientRoutesEndpointList []ClientRoutesEndpoint

func (l *ClientRoutesEndpointList) GetAllConnectionIDs() []string {
	var ids []string
	for _, endpoint := range *l {
		ids = append(ids, endpoint.ConnectionID)
	}
	return ids
}

func (l *ClientRoutesEndpointList) Validate() error {
	for id, endpoint := range *l {
		if err := endpoint.Validate(); err != nil {
			return fmt.Errorf("endpoint #%d is invalid: %w", id, err)
		}
	}
	return nil
}

type ClientRoutesConfig struct {
	TableName string
	Endpoints ClientRoutesEndpointList
	// Deprecated:
	ResolveHealthyEndpointPeriod time.Duration
	// Deprecated:
	ResolverCacheDuration time.Duration
	// Deprecated:
	MaxResolverConcurrency int

	// Deprecated: BlockUnknownEndpoints no longer has any effect. Unknown
	// endpoints are always blocked. This field will be removed in a future
	// release.
	BlockUnknownEndpoints bool

	// EnableShardAwareness controls whether the driver should use shard-aware
	// connections when using ClientRoutes (PrivateLink).
	//
	// By default this is false because NAT typically breaks shard-awareness.
	// Shard-aware routing relies on the driver knowing the source port of connections,
	// which NAT devices modify, making it impossible for the server to route
	// requests to the correct shard.
	//
	// However, in some deployments shard-awareness can still work:
	//   - When using PROXY Protocol v2, the original source port is preserved
	//     in the protocol header. See https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt
	//   - When using direct connections without NAT (e.g., VPC peering)
	//   - When the load balancer/proxy is shard-aware itself
	//
	// Set this to true only if your network setup preserves or correctly handles
	// the source port information needed for shard-aware routing.
	EnableShardAwareness bool
}

func (cfg *ClientRoutesConfig) Validate() error {
	if cfg == nil {
		return nil
	}
	if len(cfg.Endpoints) == 0 {
		return errors.New("no endpoints specified")
	}

	if err := cfg.Endpoints.Validate(); err != nil {
		return fmt.Errorf("failed to validate endpoints: %w", err)
	}
	return nil
}

type clientRoute struct {
	connectionID string
	hostID       string
	address      string
	port         uint16
}

func (r clientRoute) String() string {
	return fmt.Sprintf(
		"clientRoute{connectionID=%s, hostID=%s, address=%s, port=%d}",
		r.connectionID,
		r.hostID,
		r.address,
		r.port,
	)
}

// clientRouteMap groups routes by hostID → connectionID → clientRoute.
// The outer key is the host, the inner key is the connection.
// This layout matches the AddressTranslator lookup pattern: find routes for a
// host first, then pick the right connection.
type clientRouteMap map[string]map[string]clientRoute

// merge upserts entries from incoming into the map.
// Before upserting it prunes stale entries within the query scope defined by
// scopeConnectionIDs and scopeHostIDs:
//   - Both non-empty (partial update): prune entries matching BOTH lists.
//   - Only scopeConnectionIDs (full refresh): prune all entries for those connections.
//
// scopeConnectionIDs must not be empty.
func (m clientRouteMap) merge(incoming []clientRoute, scopeConnectionIDs, scopeHostIDs []string) {
	if len(scopeConnectionIDs) == 0 {
		panic("clientRouteMap.merge: scopeConnectionIDs must not be empty")
	}

	if len(scopeHostIDs) > 0 {
		// Partial update: prune entries matching BOTH connection and host.
		for _, hostID := range scopeHostIDs {
			conns := m[hostID]
			if conns == nil {
				continue
			}
			for _, connID := range scopeConnectionIDs {
				delete(conns, connID)
			}
			if len(conns) == 0 {
				delete(m, hostID)
			}
		}
	} else {
		// Full refresh: prune all entries for the given connections.
		for hostID, conns := range m {
			for _, connID := range scopeConnectionIDs {
				delete(conns, connID)
			}
			if len(conns) == 0 {
				delete(m, hostID)
			}
		}
	}

	for _, inc := range incoming {
		conns := m[inc.hostID]
		if conns == nil {
			conns = make(map[string]clientRoute)
			m[inc.hostID] = conns
		}
		conns[inc.connectionID] = inc
	}
}

type ClientRoutesHandler struct {
	log           StdLogger
	c             controlConnection
	resolver      DNSResolver
	sub           *eventbus.Subscriber[events.Event]
	routes        clientRouteMap
	addrOverrides map[string]string // connectionID → user-supplied ConnectionAddr
	updateTasks   chan updateTask
	closeChan     chan struct{}
	cfg           ClientRoutesConfig
	mu            sync.RWMutex
	pickTLSPorts  bool
	initialized   bool
}

var _ AddressTranslatorV2 = (*ClientRoutesHandler)(nil)

// Translate implements old AddressTranslator interface
// should not be uses since driver prefer AddressTranslatorV2 API if it is implemented
func (p *ClientRoutesHandler) Translate(addr net.IP, port int) (net.IP, int) {
	panic("should never be called")
}

// TranslateHost implements AddressTranslatorV2 interface.
// It resolves DNS on every call rather than caching resolved addresses.
// If the user provided a ConnectionAddr for the route's connectionID,
// that address is used instead of the one from the system.client_routes table.
func (p *ClientRoutesHandler) TranslateHost(host AddressTranslatorHostInfo, addr AddressPort) (AddressPort, error) {
	hostID := host.HostID()
	if hostID == "" {
		return addr, nil
	}

	p.mu.RLock()
	var route clientRoute
	var found bool
	for _, r := range p.routes[hostID] {
		route = r
		found = true
		break
	}
	p.mu.RUnlock()

	if !found {
		return addr, fmt.Errorf("no address found for host %s", hostID)
	}

	resolveAddr := route.address
	if override, ok := p.addrOverrides[route.connectionID]; ok {
		resolveAddr = override
	}

	if route.port == 0 {
		return addr, fmt.Errorf("record %s/%s has target port empty", route.hostID, route.connectionID)
	}

	ips, err := p.resolver.LookupIP(resolveAddr)
	if err != nil {
		return addr, fmt.Errorf("failed to resolve address for host %s: %v", hostID, err)
	}
	if len(ips) == 0 {
		return addr, fmt.Errorf("no addresses returned for host %s (address=%s)", hostID, resolveAddr)
	}

	if route.port == 0 {
		return addr, fmt.Errorf("record %s/%s has target port empty", route.hostID, route.connectionID)
	}

	return AddressPort{Address: ips[0], Port: route.port}, nil
}

type updateTask struct {
	result        chan error
	connectionIDs []string
	hostIDs       []string
}

func (p *ClientRoutesHandler) Initialize(s *Session) error {
	if p.initialized {
		return errors.New("already initialized")
	}
	connectionIDs := make([]string, 0, len(p.cfg.Endpoints))
	for _, ep := range p.cfg.Endpoints {
		if ep.ConnectionID != "" {
			connectionIDs = append(connectionIDs, ep.ConnectionID)
		}
	}
	p.c = s.control
	p.sub = s.eventBus.Subscribe("port-mux", 1024, func(event events.Event) bool {
		switch event.Type() {
		case events.SessionEventTypeControlConnectionRecreated, events.ClusterEventTypeClientRoutesChanged:
			return true
		default:
			return false
		}
	})
	p.startUpdateWorker()
	p.startReadingEvents()
	err := p.updateHostPortMappingSync(connectionIDs, nil)
	if err != nil {
		p.log.Printf("error updating host ports: %v\n", err)
	}
	return nil
}

func (p *ClientRoutesHandler) Stop() {
	if p.closeChan != nil {
		close(p.closeChan)
	}
	if p.sub != nil {
		p.sub.Stop()
	}
	// updateTasks is intentionally NOT closed here; the worker goroutine exits
	// by selecting on closeChan, which avoids the race between close(updateTasks)
	// and concurrent sends to it.
}

func (p *ClientRoutesHandler) updateHostPortMappingAsync(connectionIDs []string, hostIDs []string) {
	select {
	case p.updateTasks <- updateTask{
		connectionIDs: connectionIDs,
		hostIDs:       hostIDs,
	}:
	case <-p.closeChan:
		// Stop() was called; drop the update safely.
	}
}

func (p *ClientRoutesHandler) updateHostPortMappingSync(connectionIDs []string, hostIDs []string) error {
	result := make(chan error, 1)
	select {
	case p.updateTasks <- updateTask{
		connectionIDs: connectionIDs,
		hostIDs:       hostIDs,
		result:        result,
	}:
	case <-p.closeChan:
		return errors.New("client routes handler stopped")
	}
	return <-result
}

func (p *ClientRoutesHandler) startReadingEvents() {
	connectionIDs := p.cfg.Endpoints.GetAllConnectionIDs()

	go func() {
		for event := range p.sub.Events() {
			switch evt := event.(type) {
			case *events.ClientRoutesChangedEvent:
				if debug.Enabled {
					if len(evt.ConnectionIDs) == 0 {
						p.log.Printf("got CLIENT_ROUTES_CHANGE event with no connection IDs")
						continue
					}
					if len(evt.HostIDs) == 0 {
						p.log.Printf("got CLIENT_ROUTES_CHANGE event with no host IDs")
						continue
					}
				}
				var newConnectionIDs []string
				for _, connectionID := range evt.ConnectionIDs {
					if connectionID == "" {
						continue
					}
					if slices.ContainsFunc(p.cfg.Endpoints, func(ep ClientRoutesEndpoint) bool {
						return ep.ConnectionID == connectionID
					}) {
						newConnectionIDs = append(newConnectionIDs, connectionID)
					}
				}
				if len(newConnectionIDs) != 0 {
					p.updateHostPortMappingAsync(newConnectionIDs, evt.HostIDs)
				}
			case *events.ControlConnectionRecreatedEvent:
				p.updateHostPortMappingAsync(connectionIDs, nil)
			}
		}
	}()
}

func (p *ClientRoutesHandler) startUpdateWorker() {
	go func() {
		for {
			select {
			case task := <-p.updateTasks:
				err := p.updateHostPortMapping(task.connectionIDs, task.hostIDs)
				if err != nil {
					if debug.Enabled {
						p.log.Printf("failed to update host port mapping: %v", err)
					}
				}
				if task.result != nil {
					task.result <- err
					close(task.result)
				}
			case <-p.closeChan:
				return
			}
		}
	}()
}

func (p *ClientRoutesHandler) updateHostPortMapping(connectionIDs []string, hostIDs []string) error {
	incoming, err := getHostPortMappingFromCluster(p.c, p.cfg.TableName, connectionIDs, hostIDs, p.pickTLSPorts)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.routes.merge(incoming, connectionIDs, hostIDs)
	p.mu.Unlock()

	return nil
}


func NewClientRoutesAddressTranslator(
	cfg ClientRoutesConfig,
	resolver DNSResolver,
	pickTLSPorts bool,
	log StdLogger,
) *ClientRoutesHandler {
	if resolver == nil {
		resolver = defaultDnsResolver
	}
	overrides := make(map[string]string, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		if ep.ConnectionAddr != "" {
			overrides[ep.ConnectionID] = ep.ConnectionAddr
		}
	}
	return &ClientRoutesHandler{
		cfg:           cfg,
		log:           log,
		pickTLSPorts:  pickTLSPorts,
		closeChan:     make(chan struct{}),
		updateTasks:   make(chan updateTask, 1024),
		resolver:      resolver,
		routes:        make(clientRouteMap),
		addrOverrides: overrides,
	}
}

var _ AddressTranslator = &ClientRoutesHandler{}

func getHostPortMappingFromCluster(c controlConnection, table string, connectionIDs []string, hostIDs []string, pickTLSPorts bool) ([]clientRoute, error) {
	var res []clientRoute

	stmt := []string{fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s", table)}
	var bounds []any
	if len(connectionIDs) != 0 {
		var inClause []string
		for _, connectionID := range connectionIDs {
			bounds = append(bounds, connectionID)
			inClause = append(inClause, "?")
		}
		if len(stmt) == 1 {
			stmt = append(stmt, "where")
		}
		stmt = append(stmt, fmt.Sprintf("connection_id in (%s)", strings.Join(inClause, ",")))
	}

	if len(hostIDs) != 0 {
		var inClause []string
		for _, hostID := range hostIDs {
			bounds = append(bounds, hostID)
			inClause = append(inClause, "?")
		}
		if len(stmt) == 1 {
			stmt = append(stmt, "where")
		} else {
			stmt = append(stmt, "and")
		}
		stmt = append(stmt, fmt.Sprintf("host_id in (%s)", strings.Join(inClause, ",")))
	}

	isFullScan := len(hostIDs) == 0 || len(connectionIDs) == 0
	if isFullScan {
		stmt = append(stmt, "allow filtering")
	}

	iter := c.query(strings.Join(stmt, " "), bounds...)
	var (
		connectionID  string
		hostID        string
		address       string
		cqlPort       uint16
		secureCQLPort uint16
	)
	for iter.Scan(&connectionID, &hostID, &address, &cqlPort, &secureCQLPort) {
		port := cqlPort
		if pickTLSPorts {
			port = secureCQLPort
		}
		res = append(res, clientRoute{
			connectionID: connectionID,
			hostID:       hostID,
			address:      address,
			port:         port,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error reading %s table: %v", table, err)
	}
	return res, nil
}
