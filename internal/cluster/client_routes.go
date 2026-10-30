// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"

	"time"

	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/debug"
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

type InternalClientRoute struct {
	InternalConnectionID string
	InternalHostID       string
	InternalAddress      string
	InternalPort         uint16
}

func (r InternalClientRoute) String() string {
	return fmt.Sprintf(
		"clientRoute{connectionID=%s, hostID=%s, address=%s, port=%d}",
		r.InternalConnectionID,
		r.InternalHostID,
		r.InternalAddress,
		r.InternalPort,
	)
}

type InternalClientRouteCacheEntry struct {
	current           *InternalClientRoute
	InternalAllRoutes []InternalClientRoute
}

func (r *InternalClientRouteCacheEntry) routeIndex(connectionID string) int {
	return slices.IndexFunc(r.InternalAllRoutes, func(route InternalClientRoute) bool {
		return route.InternalConnectionID == connectionID
	})
}

func (r *InternalClientRouteCacheEntry) CurrentConnectionID() string {
	if r.current == nil {
		return ""
	}
	return r.current.InternalConnectionID
}

func (r *InternalClientRouteCacheEntry) BindCurrent(connectionID string) {
	if idx := r.routeIndex(connectionID); idx >= 0 {
		r.current = &r.InternalAllRoutes[idx]
		return
	}
	r.current = nil
}

func (r *InternalClientRouteCacheEntry) DeleteByConnectionID(connectionID string) {
	current := r.CurrentConnectionID()
	r.InternalAllRoutes = slices.DeleteFunc(r.InternalAllRoutes, func(route InternalClientRoute) bool {
		return route.InternalConnectionID == connectionID
	})
	r.BindCurrent(current)
}

func (r *InternalClientRouteCacheEntry) Upsert(route InternalClientRoute) {
	current := r.CurrentConnectionID()
	defer func() { r.BindCurrent(current) }()
	if idx := r.routeIndex(route.InternalConnectionID); idx >= 0 {
		r.InternalAllRoutes[idx] = route
	} else {
		r.InternalAllRoutes = append(r.InternalAllRoutes, route)
	}
}

func (r *InternalClientRouteCacheEntry) preferredRoute() (InternalClientRoute, bool) {
	if r.current != nil {
		return *r.current, true
	}

	if len(r.InternalAllRoutes) == 0 {
		return InternalClientRoute{}, false
	}

	r.current = &r.InternalAllRoutes[0]
	return *r.current, true
}

// clientRouteCache groups routes by hostID and owns synchronization for route selection and updates.
type InternalClientRouteCache struct {
	InternalRoutes map[string]InternalClientRouteCacheEntry
	mu             sync.Mutex
}

func InternalNewClientRouteCache() InternalClientRouteCache {
	return InternalClientRouteCache{InternalRoutes: make(map[string]InternalClientRouteCacheEntry)}
}

// deleteByPairs removes entries identified by (connectionID, hostID) pairs.
func (c *InternalClientRouteCache) deleteByPairsLocked(pairs []InternalPair) {
	for _, p := range pairs {
		entry, ok := c.InternalRoutes[p.InternalHostID]
		if !ok {
			continue
		}
		entry.DeleteByConnectionID(p.InternalConnectionID)
		if len(entry.InternalAllRoutes) == 0 {
			delete(c.InternalRoutes, p.InternalHostID)
			continue
		}
		c.InternalRoutes[p.InternalHostID] = entry
	}
}

// deleteByConnectionIDs removes all entries for the given connectionIDs across all hosts.
func (c *InternalClientRouteCache) deleteByConnectionIDsLocked(connectionIDs []string) {
	for hostID, entry := range c.InternalRoutes {
		for _, connID := range connectionIDs {
			entry.DeleteByConnectionID(connID)
		}
		if len(entry.InternalAllRoutes) == 0 {
			delete(c.InternalRoutes, hostID)
			continue
		}
		c.InternalRoutes[hostID] = entry
	}
}

func (c *InternalClientRouteCache) upsertRecordsLocked(incoming []InternalClientRoute) {
	for _, inc := range incoming {
		entry := c.InternalRoutes[inc.InternalHostID]
		entry.Upsert(inc)
		c.InternalRoutes[inc.InternalHostID] = entry
	}
}

// preferredRoute returns the current route for hostID if one exists and is still valid,
// otherwise picks the first available route and records it as current.
func (c *InternalClientRouteCache) preferredRouteLocked(hostID string) (InternalClientRoute, bool) {
	entry, ok := c.InternalRoutes[hostID]
	if !ok {
		return InternalClientRoute{}, false
	}

	route, ok := entry.preferredRoute()
	if !ok {
		delete(c.InternalRoutes, hostID)
		return InternalClientRoute{}, false
	}
	c.InternalRoutes[hostID] = entry
	return route, true
}

func (c *InternalClientRouteCache) snapshotCurrentConnectionIDsLocked() map[string]string {
	currentIDs := make(map[string]string, len(c.InternalRoutes))
	for hostID, entry := range c.InternalRoutes {
		if currentID := entry.CurrentConnectionID(); currentID != "" {
			currentIDs[hostID] = currentID
		}
	}
	return currentIDs
}

func (c *InternalClientRouteCache) rebindCurrentConnectionIDsLocked(currentIDs map[string]string) {
	for hostID, currentID := range currentIDs {
		entry, ok := c.InternalRoutes[hostID]
		if !ok {
			continue
		}
		entry.BindCurrent(currentID)
		if len(entry.InternalAllRoutes) == 0 {
			delete(c.InternalRoutes, hostID)
			continue
		}
		c.InternalRoutes[hostID] = entry
	}
}

func (c *InternalClientRouteCache) PreferredRoute(hostID string) (InternalClientRoute, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.preferredRouteLocked(hostID)
}

func (c *InternalClientRouteCache) ReplaceByPairs(pairs []InternalPair, incoming []InternalClientRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()
	currentIDs := c.snapshotCurrentConnectionIDsLocked()
	c.deleteByPairsLocked(pairs)
	c.upsertRecordsLocked(incoming)
	c.rebindCurrentConnectionIDsLocked(currentIDs)
}

func (c *InternalClientRouteCache) ReplaceByConnectionIDs(connectionIDs []string, incoming []InternalClientRoute) {
	c.mu.Lock()
	defer c.mu.Unlock()
	currentIDs := c.snapshotCurrentConnectionIDsLocked()
	c.deleteByConnectionIDsLocked(connectionIDs)
	c.upsertRecordsLocked(incoming)
	c.rebindCurrentConnectionIDsLocked(currentIDs)
}

type ClientRoutesHandler struct {
	privateStorage ClientRoutesHandlerStorage

	// connectionID → user-supplied ConnectionAddr

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
	for _, route := range InternalClientRoutesHandlerOrderedRoutes(p, hostID) {
		endpoints, err := p.resolveRoute(context.Background(), route)
		if err != nil {
			causes = append(causes, err)
			continue
		}
		if InternalClientRoutesHandlerBindRoute(p, route) {
			return endpoints[0], nil
		}
	}
	return AddressPort{}, fmt.Errorf("no usable route for host %s: %w", hostID, errors.Join(causes...))
}

type InternalPair struct {
	InternalConnectionID string
	InternalHostID       string
}

type InternalUpdateTask struct {
	result          chan error
	InternalRelease func()
	// Exactly one of pairs or connectionIDs must be set.
	// pairs: scoped update — delete and re-query specific (connectionID, hostID) pairs.
	// connectionIDs: full refresh — delete all entries for these connections and re-query.
	// If both are nil, updateHostPortMapping returns an error.
	// If both are set, pairs takes precedence.
	InternalPairs         []InternalPair
	InternalConnectionIDs []string
}

func (p *ClientRoutesHandler) Initialize(s *Session) error {
	p.privateStorage.queueMu.Lock()
	if p.privateStorage.InternalInitialized.Load() {
		p.privateStorage.queueMu.Unlock()
		return errors.New("already initialized")
	}
	select {
	case <-p.privateStorage.InternalCloseChan:
		p.privateStorage.queueMu.Unlock()
		return ErrClosed
	default:
	}
	p.privateStorage.InternalSession = s
	p.privateStorage.InternalC = s.privateStorage.InternalControl
	InternalClientRoutesHandlerStartUpdateWorker(p)
	p.privateStorage.InternalInitialized.Store(true)
	p.privateStorage.queueMu.Unlock()
	err := InternalClientRoutesHandlerUpdateHostPortMappingSync(p, InternalUpdateTask{InternalConnectionIDs: p.privateStorage.InternalCfg.Endpoints.GetAllConnectionIDs()})
	if err != nil && p.privateStorage.InternalLog != nil {
		p.privateStorage.InternalLog.Printf("error updating host ports: %v", err)
	}
	return err
}

func (p *ClientRoutesHandler) Stop() {
	p.privateStorage.stopOnce.Do(func() {
		p.privateStorage.queueMu.Lock()
		if p.privateStorage.InternalCloseChan != nil {
			close(p.privateStorage.InternalCloseChan)
		}
		p.privateStorage.queueMu.Unlock()
		if p.privateStorage.InternalSub != nil {
			p.privateStorage.InternalSub.Stop()
		}
	})
	p.privateStorage.InternalWorkers.Wait()
}

func InternalClientRoutesHandlerUpdateHostPortMappingAsync(p *ClientRoutesHandler, task InternalUpdateTask) bool {
	InternalClientRoutesHandlerInvalidateRouteTask(p, task)
	p.privateStorage.queueMu.Lock()
	defer p.privateStorage.queueMu.Unlock()
	select {
	case <-p.privateStorage.InternalCloseChan:
		return false
	default:
	}
	select {
	case p.privateStorage.InternalUpdateTasks <- task:
		return true
	default:
		p.privateStorage.needsResync.Store(true)
		return false
	}
}
func InternalClientRoutesHandlerUpdateHostPortMappingSync(p *ClientRoutesHandler, task InternalUpdateTask) error {
	select {
	case <-p.privateStorage.InternalCloseChan:
		return ErrClosed
	default:
	}
	return p.updateHostPortMappingContext(v2SchemaContext(InternalNewSchemaSource(p.privateStorage.InternalSession)), task)
}

func InternalClientRoutesHandlerStartReadingEvents(p *ClientRoutesHandler, connectionIDs []string) {
	p.privateStorage.InternalWorkers.Add(1)
	go func() {
		defer p.privateStorage.InternalWorkers.Done()
		for event := range p.privateStorage.InternalSub.Events() {
			switch evt := event.(type) {
			case *events.ClientRoutesChangedEvent:
				if debug.Enabled {
					if len(evt.ConnectionIDs) == 0 {
						p.privateStorage.InternalLog.Printf("got CLIENT_ROUTES_CHANGE event with no connection IDs")
						continue
					}
					if len(evt.HostIDs) == 0 {
						p.privateStorage.InternalLog.Printf("got CLIENT_ROUTES_CHANGE event with no host IDs")
					}
				}
				filteredConnectionIDs := InternalFilterAllowedConnectionIDs(evt.ConnectionIDs, connectionIDs)
				if len(filteredConnectionIDs) == 0 {
					continue
				}
				if len(evt.HostIDs) == 0 {
					InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, InternalUpdateTask{InternalConnectionIDs: filteredConnectionIDs})
					continue
				}
				pairs := InternalGetPairsFromEvent(filteredConnectionIDs, evt.HostIDs)
				if len(pairs) > 0 {
					InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, InternalUpdateTask{InternalPairs: pairs})
				} else {
					InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, InternalUpdateTask{InternalConnectionIDs: filteredConnectionIDs})
				}
			case *events.ControlConnectionRecreatedEvent:
				InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, InternalUpdateTask{InternalConnectionIDs: connectionIDs})
			}
		}
	}()
}

func InternalGetPairsFromEvent(connectionIDs, hostIDs []string) (pairs []InternalPair) {
	if len(connectionIDs) == 0 || len(hostIDs) == 0 || len(connectionIDs) > 1024/len(hostIDs) {
		return nil
	}
	pairs = make([]InternalPair, 0, len(connectionIDs)*len(hostIDs))
	for _, connID := range connectionIDs {
		for _, hostID := range hostIDs {
			pairs = append(pairs, InternalPair{
				InternalConnectionID: connID,
				InternalHostID:       hostID,
			})
		}
	}
	return pairs
}

func InternalFilterAllowedConnectionIDs(connectionIDs, allowed []string) []string {
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

func InternalClientRoutesHandlerStartUpdateWorker(p *ClientRoutesHandler) {
	p.privateStorage.InternalWorkers.Add(1)
	go func() {
		defer p.privateStorage.InternalWorkers.Done()
		defer func() {
			for {
				select {
				case task := <-p.privateStorage.InternalUpdateTasks:
					if task.InternalRelease != nil {
						task.InternalRelease()
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
			case task := <-p.privateStorage.InternalUpdateTasks:
				err := func() error {
					if task.InternalRelease != nil {
						defer task.InternalRelease()
					}
					return p.updateHostPortMapping(task)
				}()
				if err != nil {
					if debug.Enabled {
						p.privateStorage.InternalLog.Printf("failed to update host port mapping: %v", err)
					}
				}
				if p.privateStorage.needsResync.Swap(false) {
					_ = p.refreshContext(v2SchemaContext(InternalNewSchemaSource(p.privateStorage.InternalSession)))
				}
				if task.result != nil {
					task.result <- err
					close(task.result)
				}
			case <-p.privateStorage.InternalCloseChan:
				return
			}
		}
	}()
}

func (p *ClientRoutesHandler) updateHostPortMapping(task InternalUpdateTask) error {
	ctx := v2SchemaContext(InternalNewSchemaSource(p.privateStorage.InternalSession))
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
	return func() *ClientRoutesHandler {
		opaqueStorageValue := new(ClientRoutesHandler)
		opaqueStorageValue.privateStorage.InternalCfg = cfg
		opaqueStorageValue.privateStorage.InternalLog = log
		opaqueStorageValue.privateStorage.InternalPickTLSPorts = pickTLSPorts
		opaqueStorageValue.privateStorage.InternalCloseChan = make(chan struct{})
		opaqueStorageValue.privateStorage.InternalUpdateTasks = make(chan InternalUpdateTask, 1024)
		opaqueStorageValue.privateStorage.InternalResolver = resolver
		opaqueStorageValue.privateStorage.InternalRouteCache = InternalNewClientRouteCache()
		opaqueStorageValue.privateStorage.InternalAddrOverrides = overrides
		return opaqueStorageValue
	}()

}

var _ AddressTranslator = func() *ClientRoutesHandler {
	opaqueStorageValuex := new(ClientRoutesHandler)
	return opaqueStorageValuex
}()

func InternalGetHostPortMappingForConnectionIDs(c InternalControlConnection, table string, ids []string, tls bool) ([]InternalClientRoute, error) {
	return InternalGetV2RoutesConnections(v2SchemaContext(InternalNewSchemaSource(c.InternalGetSession())), c, table, ids, tls)
}
func InternalGetHostPortMappingForPairs(c InternalControlConnection, table string, pairs []InternalPair, tls bool) ([]InternalClientRoute, error) {
	return InternalGetV2RoutesPairs(v2SchemaContext(InternalNewSchemaSource(c.InternalGetSession())), c, table, pairs, tls)
}
