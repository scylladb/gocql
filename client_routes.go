package gocql

import (
	"errors"
	"fmt"
	"net"
	"slices"
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

// deleteByPairs removes entries identified by (connectionID, hostID) pairs.
func (m clientRouteMap) deleteByPairs(pairs []pair) {
	for _, p := range pairs {
		conns := m[p.hostID]
		if conns == nil {
			continue
		}
		delete(conns, p.connectionID)
		if len(conns) == 0 {
			delete(m, p.hostID)
		}
	}
}

// deleteByConnectionIDs removes all entries for the given connectionIDs across all hosts.
func (m clientRouteMap) deleteByConnectionIDs(connectionIDs []string) {
	for hostID, conns := range m {
		for _, connID := range connectionIDs {
			delete(conns, connID)
		}
		if len(conns) == 0 {
			delete(m, hostID)
		}
	}
}

func (m clientRouteMap) populateRecords(incoming []clientRoute) {
	for _, inc := range incoming {
		conns := m[inc.hostID]
		if conns == nil {
			conns = make(map[string]clientRoute)
			m[inc.hostID] = conns
		}
		conns[inc.connectionID] = inc
	}
}

// clientRouteTable is the single source of truth for route data.
// It owns both the full route index and the per-host sticky selection so that
// all consistency-maintaining logic lives in one place.
type clientRouteTable struct {
	routes      clientRouteMap         // hostID → connectionID → clientRoute
	stickyRoute map[string]clientRoute // hostID → preferred route
}

func newClientRouteTable() clientRouteTable {
	return clientRouteTable{
		routes:      make(clientRouteMap),
		stickyRoute: make(map[string]clientRoute),
	}
}

// pruneStickyRoutes removes sticky entries whose route is no longer present in routes.
func (t *clientRouteTable) pruneStickyRoutes() {
	for hostID, route := range t.stickyRoute {
		if _, ok := t.routes[hostID][route.connectionID]; !ok {
			delete(t.stickyRoute, hostID)
		}
	}
}

// preferred returns the sticky route for hostID if one exists and is still valid,
// otherwise picks an arbitrary route and records it as the new sticky entry.
func (t *clientRouteTable) preferred(hostID string) (clientRoute, bool) {
	if route, ok := t.stickyRoute[hostID]; ok {
		if _, exists := t.routes[hostID][route.connectionID]; exists {
			return route, true
		}
		// Stale sticky entry; clear it and fall back to a live route.
		delete(t.stickyRoute, hostID)
	}
	for _, route := range t.routes[hostID] {
		t.stickyRoute[hostID] = route
		return route, true
	}
	return clientRoute{}, false
}

type ClientRoutesHandler struct {
	log           StdLogger
	c             controlConnection
	resolver      DNSResolver
	sub           *eventbus.Subscriber[events.Event]
	routeTable    clientRouteTable
	addrOverrides map[string]string // connectionID → user-supplied ConnectionAddr
	updateTasks   chan updateTask
	closeChan     chan struct{}
	cfg           ClientRoutesConfig
	mu            sync.Mutex
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

	p.mu.Lock()
	route, found := p.routeTable.preferred(hostID)
	p.mu.Unlock()

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

	return AddressPort{Address: ips[0], Port: route.port}, nil
}

type pair struct {
	connectionID string
	hostID       string
}

type updateTask struct {
	result chan error
	// Exactly one of pairs or connectionIDs must be set.
	// pairs: scoped update — delete and re-query specific (connectionID, hostID) pairs.
	// connectionIDs: full refresh — delete all entries for these connections and re-query.
	// If both are nil, updateHostPortMapping returns an error.
	// If both are set, pairs takes precedence.
	pairs         []pair
	connectionIDs []string
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
	err := p.updateHostPortMappingSync(updateTask{connectionIDs: connectionIDs})
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

func (p *ClientRoutesHandler) updateHostPortMappingAsync(task updateTask) {
	select {
	case p.updateTasks <- task:
	case <-p.closeChan:
		// Stop() was called; drop the update safely.
	}
}

func (p *ClientRoutesHandler) updateHostPortMappingSync(task updateTask) error {
	task.result = make(chan error, 1)
	select {
	case p.updateTasks <- task:
	case <-p.closeChan:
		return errors.New("client routes handler stopped")
	}
	return <-task.result
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
				pairs := getPairsFromEvent(evt, connectionIDs)
				if len(pairs) != 0 {
					p.updateHostPortMappingAsync(updateTask{pairs: pairs})
				}
			case *events.ControlConnectionRecreatedEvent:
				p.updateHostPortMappingAsync(updateTask{connectionIDs: connectionIDs})
			}
		}
	}()
}

func getPairsFromEvent(evt *events.ClientRoutesChangedEvent, allowedConnectionIDs []string) (pairs []pair) {
	if len(evt.ConnectionIDs) != len(evt.HostIDs) {
		return nil
	}
	for n, connID := range evt.ConnectionIDs {
		if !slices.Contains(allowedConnectionIDs, connID) {
			continue
		}
		pairs = append(pairs, pair{
			connectionID: connID,
			hostID:       evt.HostIDs[n],
		})
	}
	return pairs
}

func (p *ClientRoutesHandler) startUpdateWorker() {
	go func() {
		for {
			select {
			case task := <-p.updateTasks:
				err := p.updateHostPortMapping(task)
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

func (p *ClientRoutesHandler) updateHostPortMapping(task updateTask) error {
	var incoming []clientRoute
	var err error

	switch {
	case task.pairs != nil:
		incoming, err = getHostPortMappingForPairs(p.c, p.cfg.TableName, task.pairs, p.pickTLSPorts)
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.routeTable.routes.deleteByPairs(task.pairs)
	case task.connectionIDs != nil:
		incoming, err = getHostPortMappingForConnectionIDs(p.c, p.cfg.TableName, task.connectionIDs, p.pickTLSPorts)
		if err != nil {
			return err
		}
		p.mu.Lock()
		p.routeTable.routes.deleteByConnectionIDs(task.connectionIDs)
	default:
		return errors.New("updateTask has neither pairs nor connectionIDs")
	}

	p.routeTable.routes.populateRecords(incoming)
	p.routeTable.pruneStickyRoutes()
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
		routeTable:    newClientRouteTable(),
		addrOverrides: overrides,
	}
}

var _ AddressTranslator = &ClientRoutesHandler{}

func getHostPortMappingForConnectionIDs(c controlConnection, table string, connIDs []string, pickTLSPorts bool) ([]clientRoute, error) {
	if len(connIDs) == 0 {
		return nil, errors.New("connIDs cannot be empty")
	}

	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ?", table)
	return readClientRoutesTable(c, table, stmt, []any{connIDs}, pickTLSPorts)
}

func getHostPortMappingForPairs(c controlConnection, table string, pairs []pair, pickTLSPorts bool) ([]clientRoute, error) {
	if len(pairs) == 0 {
		return nil, errors.New("pairs cannot be empty")
	}

	connIDs := make([]string, len(pairs))
	hostIDs := make([]string, len(pairs))
	for i, p := range pairs {
		connIDs[i] = p.connectionID
		hostIDs[i] = p.hostID
	}

	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ? and host_id in ?", table)
	return readClientRoutesTable(c, table, stmt, []any{connIDs, hostIDs}, pickTLSPorts)
}

func readClientRoutesTable(c controlConnection, table, stmt string, bounds []any, pickTLSPorts bool) ([]clientRoute, error) {
	iter := c.query(stmt, bounds...)
	var (
		connectionID  string
		hostID        string
		address       string
		cqlPort       uint16
		secureCQLPort uint16
	)
	var res []clientRoute
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
