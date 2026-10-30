package gocql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/debug"
	"github.com/scylladb/gocql/v2/internal/eventbus"
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

func (l ClientRoutesEndpointList) GetAllConnectionIDs() []string {
	ids := make([]string, 0, len(l))
	for _, endpoint := range l {
		ids = append(ids, endpoint.ConnectionID)
	}
	return ids
}

func (l ClientRoutesEndpointList) Validate() error {
	for id, endpoint := range l {
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

	// EnableShardAwareness controls advanced shard awareness for ClientRoutes:
	// choosing a connection's source port so Scylla assigns it to a desired shard.
	// Per-shard connection pooling and token-to-shard routing remain enabled when
	// this option is false.
	//
	// By default this is false because PrivateLink paths commonly use NAT.
	//
	// Set this to true only when the client-route endpoint forwards to Scylla's
	// Proxy Protocol v2 shard-aware CQL listener and the proxy supplies the original
	// client source port in the Proxy Protocol v2 header. clusterSettings's
	// DisableShardAwarePort setting takes precedence.
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

type clientRouteCacheEntry struct {
	current   *clientRoute
	allRoutes []clientRoute
}

func (r *clientRouteCacheEntry) routeIndex(connectionID string) int {
	return slices.IndexFunc(r.allRoutes, func(route clientRoute) bool {
		return route.connectionID == connectionID
	})
}

func (r *clientRouteCacheEntry) CurrentConnectionID() string {
	if r.current == nil {
		return ""
	}
	return r.current.connectionID
}

func (r *clientRouteCacheEntry) BindCurrent(connectionID string) {
	if idx := r.routeIndex(connectionID); idx >= 0 {
		r.current = &r.allRoutes[idx]
		return
	}
	r.current = nil
}

func (r *clientRouteCacheEntry) DeleteByConnectionID(connectionID string) {
	current := r.CurrentConnectionID()
	r.allRoutes = slices.DeleteFunc(r.allRoutes, func(route clientRoute) bool {
		return route.connectionID == connectionID
	})
	r.BindCurrent(current)
}

func (r *clientRouteCacheEntry) Upsert(route clientRoute) {
	current := r.CurrentConnectionID()
	defer func() { r.BindCurrent(current) }()
	if idx := r.routeIndex(route.connectionID); idx >= 0 {
		r.allRoutes[idx] = route
	} else {
		r.allRoutes = append(r.allRoutes, route)
	}
}

func (r *clientRouteCacheEntry) preferredRoute() (clientRoute, bool) {
	if r.current != nil {
		return *r.current, true
	}

	if len(r.allRoutes) == 0 {
		return clientRoute{}, false
	}

	r.current = &r.allRoutes[0]
	return *r.current, true
}

// clientRouteCache groups routes by hostID and owns synchronization for route selection and updates.
type clientRouteCache struct {
	routes map[string]clientRouteCacheEntry
	mu     sync.Mutex
}

func newClientRouteCache() clientRouteCache {
	return clientRouteCache{routes: make(map[string]clientRouteCacheEntry)}
}

// deleteByPairs removes entries identified by (connectionID, hostID) pairs.
func (c *clientRouteCache) deleteByPairsLocked(pairs []pair) {
	for _, p := range pairs {
		entry, ok := c.routes[p.hostID]
		if !ok {
			continue
		}
		entry.DeleteByConnectionID(p.connectionID)
		if len(entry.allRoutes) == 0 {
			delete(c.routes, p.hostID)
			continue
		}
		c.routes[p.hostID] = entry
	}
}

// deleteByConnectionIDs removes all entries for the given connectionIDs across all hosts.
func (c *clientRouteCache) deleteByConnectionIDsLocked(connectionIDs []string) {
	for hostID, entry := range c.routes {
		for _, connID := range connectionIDs {
			entry.DeleteByConnectionID(connID)
		}
		if len(entry.allRoutes) == 0 {
			delete(c.routes, hostID)
			continue
		}
		c.routes[hostID] = entry
	}
}

func (c *clientRouteCache) upsertRecordsLocked(incoming []clientRoute) {
	for _, inc := range incoming {
		entry := c.routes[inc.hostID]
		entry.Upsert(inc)
		c.routes[inc.hostID] = entry
	}
}

// preferredRoute returns the current route for hostID if one exists and is still valid,
// otherwise picks the first available route and records it as current.
func (c *clientRouteCache) preferredRouteLocked(hostID string) (clientRoute, bool) {
	entry, ok := c.routes[hostID]
	if !ok {
		return clientRoute{}, false
	}

	route, ok := entry.preferredRoute()
	if !ok {
		delete(c.routes, hostID)
		return clientRoute{}, false
	}
	c.routes[hostID] = entry
	return route, true
}

func (c *clientRouteCache) snapshotCurrentConnectionIDsLocked() map[string]string {
	currentIDs := make(map[string]string, len(c.routes))
	for hostID, entry := range c.routes {
		if currentID := entry.CurrentConnectionID(); currentID != "" {
			currentIDs[hostID] = currentID
		}
	}
	return currentIDs
}

func (c *clientRouteCache) rebindCurrentConnectionIDsLocked(currentIDs map[string]string) {
	for hostID, currentID := range currentIDs {
		entry, ok := c.routes[hostID]
		if !ok {
			continue
		}
		entry.BindCurrent(currentID)
		if len(entry.allRoutes) == 0 {
			delete(c.routes, hostID)
			continue
		}
		c.routes[hostID] = entry
	}
}

func (c *clientRouteCache) PreferredRoute(hostID string) (clientRoute, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.preferredRouteLocked(hostID)
}

func (c *clientRouteCache) ReplaceByPairs(pairs []pair, incoming []clientRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()
	currentIDs := c.snapshotCurrentConnectionIDsLocked()
	c.deleteByPairsLocked(pairs)
	c.upsertRecordsLocked(incoming)
	c.rebindCurrentConnectionIDsLocked(currentIDs)
}

func (c *clientRouteCache) ReplaceByConnectionIDs(connectionIDs []string, incoming []clientRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()
	currentIDs := c.snapshotCurrentConnectionIDsLocked()
	c.deleteByConnectionIDsLocked(connectionIDs)
	c.upsertRecordsLocked(incoming)
	c.rebindCurrentConnectionIDsLocked(currentIDs)
}

type ClientRoutesHandler struct {
	needsResync     atomic.Bool
	session         *Session
	workers         sync.WaitGroup
	stopOnce        sync.Once
	queueMu         sync.Mutex
	connectionEpoch map[string]uint64
	pairEpoch       map[pair]uint64
	log             StdLogger
	c               controlConnection
	resolver        DNSResolver
	sub             *eventbus.Subscriber[events.Event]
	routeCache      clientRouteCache
	addrOverrides   map[string]string // connectionID → user-supplied ConnectionAddr
	updateTasks     chan updateTask
	closeChan       chan struct{}
	cfg             ClientRoutesConfig
	pickTLSPorts    bool
	initialized     atomic.Bool
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

	var causes []error
	for _, route := range p.orderedRoutes(hostID) {
		endpoints, err := p.resolveRoute(context.Background(), route)
		if err != nil {
			causes = append(causes, err)
			continue
		}
		if p.bindRoute(route) {
			return endpoints[0], nil
		}
	}
	return AddressPort{}, fmt.Errorf("no usable route for host %s: %w", hostID, errors.Join(causes...))
}

type pair struct {
	connectionID string
	hostID       string
}

type updateTask struct {
	result  chan error
	release func()
	// Exactly one of pairs or connectionIDs must be set.
	// pairs: scoped update — delete and re-query specific (connectionID, hostID) pairs.
	// connectionIDs: full refresh — delete all entries for these connections and re-query.
	// If both are nil, updateHostPortMapping returns an error.
	// If both are set, pairs takes precedence.
	pairs         []pair
	connectionIDs []string
}

func (p *ClientRoutesHandler) Initialize(s *Session) error {
	p.queueMu.Lock()
	if p.initialized.Load() {
		p.queueMu.Unlock()
		return errors.New("already initialized")
	}
	select {
	case <-p.closeChan:
		p.queueMu.Unlock()
		return ErrClosed
	default:
	}
	p.session = s
	p.c = s.control
	p.startUpdateWorker()
	p.initialized.Store(true)
	p.queueMu.Unlock()
	err := p.updateHostPortMappingSync(updateTask{connectionIDs: p.cfg.Endpoints.GetAllConnectionIDs()})
	if err != nil && p.log != nil {
		p.log.Printf("error updating host ports: %v", err)
	}
	return err
}

func (p *ClientRoutesHandler) Stop() {
	p.stopOnce.Do(func() {
		p.queueMu.Lock()
		if p.closeChan != nil {
			close(p.closeChan)
		}
		p.queueMu.Unlock()
		if p.sub != nil {
			p.sub.Stop()
		}
	})
	p.workers.Wait()
}

func (p *ClientRoutesHandler) updateHostPortMappingAsync(task updateTask) bool {
	p.invalidateRouteTask(task)
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	select {
	case <-p.closeChan:
		return false
	default:
	}
	select {
	case p.updateTasks <- task:
		return true
	default:
		p.needsResync.Store(true)
		return false
	}
}
func (p *ClientRoutesHandler) updateHostPortMappingSync(task updateTask) error {
	select {
	case <-p.closeChan:
		return ErrClosed
	default:
	}
	return p.updateHostPortMappingContext(v2SchemaContext(p.session), task)
}

func (p *ClientRoutesHandler) startReadingEvents(connectionIDs []string) {
	p.workers.Add(1)
	go func() {
		defer p.workers.Done()
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
					}
				}
				filteredConnectionIDs := filterAllowedConnectionIDs(evt.ConnectionIDs, connectionIDs)
				if len(filteredConnectionIDs) == 0 {
					continue
				}
				if len(evt.HostIDs) == 0 {
					p.updateHostPortMappingAsync(updateTask{connectionIDs: filteredConnectionIDs})
					continue
				}
				pairs := getPairsFromEvent(filteredConnectionIDs, evt.HostIDs)
				if len(pairs) > 0 {
					p.updateHostPortMappingAsync(updateTask{pairs: pairs})
				} else {
					p.updateHostPortMappingAsync(updateTask{connectionIDs: filteredConnectionIDs})
				}
			case *events.ControlConnectionRecreatedEvent:
				p.updateHostPortMappingAsync(updateTask{connectionIDs: connectionIDs})
			}
		}
	}()
}

func getPairsFromEvent(connectionIDs, hostIDs []string) (pairs []pair) {
	if len(connectionIDs) == 0 || len(hostIDs) == 0 || len(connectionIDs) > 1024/len(hostIDs) {
		return nil
	}
	pairs = make([]pair, 0, len(connectionIDs)*len(hostIDs))
	for _, connID := range connectionIDs {
		for _, hostID := range hostIDs {
			pairs = append(pairs, pair{
				connectionID: connID,
				hostID:       hostID,
			})
		}
	}
	return pairs
}

func filterAllowedConnectionIDs(connectionIDs, allowed []string) []string {
	canonical := make(map[string]string, len(allowed))
	for _, id := range allowed {
		canonical[id] = id
	}
	filtered := make([]string, 0, min(len(connectionIDs), len(allowed)))
	for _, id := range connectionIDs {
		if id != "" {
			if configured, ok := canonical[id]; ok {
				filtered = append(filtered, configured)
			}
		}
	}
	return filtered
}

func (p *ClientRoutesHandler) startUpdateWorker() {
	p.workers.Add(1)
	go func() {
		defer p.workers.Done()
		defer func() {
			for {
				select {
				case task := <-p.updateTasks:
					if task.release != nil {
						task.release()
					}
					if task.result != nil {
						task.result <- ErrClosed
					}
				default:
					return
				}
			}
		}()
		for {
			select {
			case task := <-p.updateTasks:
				err := func() error {
					if task.release != nil {
						defer task.release()
					}
					return p.updateHostPortMapping(task)
				}()
				if err != nil {
					if debug.Enabled {
						p.log.Printf("failed to update host port mapping: %v", err)
					}
				}
				if p.needsResync.Swap(false) {
					_ = p.refreshContext(v2SchemaContext(p.session))
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
	ctx := v2SchemaContext(p.session)
	return p.updateHostPortMappingContext(ctx, task)
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
		routeCache:    newClientRouteCache(),
		addrOverrides: overrides,
	}
}

var _ AddressTranslator = &ClientRoutesHandler{}

func getHostPortMappingForConnectionIDs(c controlConnection, table string, ids []string, tls bool) ([]clientRoute, error) {
	return getV2RoutesConnections(v2SchemaContext(c.getSession()), c, table, ids, tls)
}
func getHostPortMappingForPairs(c controlConnection, table string, pairs []pair, tls bool) ([]clientRoute, error) {
	return getV2RoutesPairs(v2SchemaContext(c.getSession()), c, table, pairs, tls)
}
