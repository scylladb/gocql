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

func (l *ClientRoutesEndpointList) GetConnectionAddr(connectionID string) string {
	for _, endpoint := range *l {
		if endpoint.ConnectionID == connectionID {
			return endpoint.ConnectionAddr
		}
	}
	return ""
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
	ConnectionID  string
	HostID        string
	Address       string
	CQLPort       uint16
	SecureCQLPort uint16
}

// Similar returns true if both records targets same host and connection id
func (r clientRoute) Similar(o clientRoute) bool {
	return r.ConnectionID == o.ConnectionID && r.HostID == o.HostID
}

// Equal returns true if both records are exactly the same
func (r clientRoute) Equal(o clientRoute) bool {
	return r == o
}

func (r clientRoute) String() string {
	return fmt.Sprintf(
		"clientRoute{ConnectionID=%s, HostID=%s, Address=%s, CQLPort=%d, SecureCQLPort=%d}",
		r.ConnectionID,
		r.HostID,
		r.Address,
		r.CQLPort,
		r.SecureCQLPort,
	)
}

type clientRouteList []clientRoute

func (l *clientRouteList) Len() int {
	return len(*l)
}

// Merge upserts entries from incoming into the list.
// Existing entries matching by (ConnectionID, HostID) are updated in place;
// new entries are appended.
func (l *clientRouteList) Merge(incoming clientRouteList) {
	for _, inc := range incoming {
		found := false
		for id, existing := range *l {
			if existing.Similar(inc) {
				found = true
				if !existing.Equal(inc) {
					(*l)[id] = inc
				}
				break
			}
		}
		if !found {
			*l = append(*l, inc)
		}
	}
}

func (l *clientRouteList) FindByHostID(hostID string) *clientRoute {
	for i := range *l {
		if (*l)[i].HostID == hostID {
			return &(*l)[i]
		}
	}
	return nil
}

type ClientRoutesHandler struct {
	log         StdLogger
	c           controlConnection
	resolver    DNSResolver
	sub         *eventbus.Subscriber[events.Event]
	routes      clientRouteList
	updateTasks chan updateTask
	closeChan   chan struct{}
	cfg         ClientRoutesConfig
	mu          sync.RWMutex
	pickTLSPorts bool
	initialized  bool
}

var _ AddressTranslatorV2 = (*ClientRoutesHandler)(nil)

// Translate implements old AddressTranslator interface
// should not be uses since driver prefer AddressTranslatorV2 API if it is implemented
func (p *ClientRoutesHandler) Translate(addr net.IP, port int) (net.IP, int) {
	panic("should never be called")
}

func pickProperPort(pickTLSPorts bool, rec *clientRoute) uint16 {
	if pickTLSPorts {
		return rec.SecureCQLPort
	}
	return rec.CQLPort
}

// TranslateHost implements AddressTranslatorV2 interface.
// It resolves DNS on every call rather than caching resolved addresses.
func (p *ClientRoutesHandler) TranslateHost(host AddressTranslatorHostInfo, addr AddressPort) (AddressPort, error) {
	hostID := host.HostID()
	if hostID == "" {
		return addr, nil
	}

	p.mu.RLock()
	rec := p.routes.FindByHostID(hostID)
	var route clientRoute
	found := rec != nil
	if found {
		route = *rec
	}
	p.mu.RUnlock()

	if !found {
		return addr, fmt.Errorf("no address found for host %s", hostID)
	}

	ips, err := p.resolver.LookupIP(route.Address)
	if err != nil {
		return addr, fmt.Errorf("failed to resolve address for host %s: %v", hostID, err)
	}
	if len(ips) == 0 {
		return addr, fmt.Errorf("no addresses returned for host %s (address=%s)", hostID, route.Address)
	}

	port := pickProperPort(p.pickTLSPorts, &route)
	if port == 0 {
		return addr, fmt.Errorf("record %s/%s has target port empty", route.HostID, route.ConnectionID)
	}

	return AddressPort{Address: ips[0], Port: port}, nil
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
	if p.updateTasks != nil {
		close(p.updateTasks)
	}
	if p.closeChan != nil {
		close(p.closeChan)
	}
	if p.sub != nil {
		p.sub.Stop()
	}
}

func (p *ClientRoutesHandler) updateHostPortMappingAsync(connectionIDs []string, hostIDs []string) {
	p.updateTasks <- updateTask{
		connectionIDs: connectionIDs,
		hostIDs:       hostIDs,
	}
}

func (p *ClientRoutesHandler) updateHostPortMappingSync(connectionIDs []string, hostIDs []string) error {
	result := make(chan error, 1)
	p.updateTasks <- updateTask{
		connectionIDs: connectionIDs,
		hostIDs:       hostIDs,
		result:        result,
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
		for task := range p.updateTasks {
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
		}
	}()
}

func (p *ClientRoutesHandler) updateHostPortMapping(connectionIDs []string, hostIDs []string) error {
	incoming, err := getHostPortMappingFromCluster(p.c, p.cfg.TableName, connectionIDs, hostIDs)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.routes.Merge(incoming)
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
	return &ClientRoutesHandler{
		cfg:          cfg,
		log:          log,
		pickTLSPorts: pickTLSPorts,
		closeChan:    make(chan struct{}),
		updateTasks:  make(chan updateTask, 1024),
		resolver:     resolver,
		routes:       make(clientRouteList, 0),
	}
}

var _ AddressTranslator = &ClientRoutesHandler{}

func getHostPortMappingFromCluster(c controlConnection, table string, connectionIDs []string, hostIDs []string) (clientRouteList, error) {
	var res clientRouteList

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
	var rec clientRoute
	for iter.Scan(&rec.ConnectionID, &rec.HostID, &rec.Address, &rec.CQLPort, &rec.SecureCQLPort) {
		res = append(res, rec)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error reading %s table: %v", table, err)
	}
	return res, nil
}
