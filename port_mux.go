package gocql

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql/events"
	"github.com/gocql/gocql/internal/debug"
	"github.com/gocql/gocql/internal/eventbus"
)

type PrivateLinkEndpoint struct {
	// Scylla Cloud ConnectionID to read from `system.client_routes`
	connectionID string

	// Ip Address or DNS name of the AWS endpoint
	// Could stay empty, in this case driver will pick it up from system.client_routes table
	connectionAddr string
}

type PrivateLinkEndpointList []PrivateLinkEndpoint

func (l *PrivateLinkEndpointList) GetAllConnectionIDs() []string {
	var ids []string
	for _, endpoint := range *l {
		ids = append(ids, endpoint.connectionID)
	}
	return ids
}

func (l *PrivateLinkEndpointList) GetConnectionAddr(connectionID string) string {
	for _, endpoint := range *l {
		if endpoint.connectionID == connectionID {
			return endpoint.connectionAddr
		}
	}
	return ""
}

type PortMuxConfig struct {
	DNSResolver                  PortMuxResolver
	TableName                    string
	Endpoints                    PrivateLinkEndpointList
	MaxResolverConcurrency       int
	ResolveHealthyEndpointPeriod time.Duration
	Enabled                      bool
	BlockUnknownConnectionIDs    bool
}

func (cfg *PortMuxConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if len(cfg.Endpoints) == 0 {
		return errors.New("no endpoints specified")
	}
	if cfg.ResolveHealthyEndpointPeriod < 0 {
		return errors.New("resolve healthy endpoint period must be >= 0")
	}
	if cfg.DNSResolver == nil {
		return errors.New("DNS resolver cannot be nil")
	}
	if cfg.MaxResolverConcurrency <= 0 {
		return errors.New("max resolver concurrency must be > 0")
	}
	return nil
}

type UnresolvedConnectionMetadata struct {
	ConnectionID  string
	HostID        string
	Address       string
	CQLPort       uint16
	SecureCQLPort uint16
}

// SameHost returns true if both records targets same host and connection id
func (u UnresolvedConnectionMetadata) SameHost(o UnresolvedConnectionMetadata) bool {
	return u.ConnectionID == o.ConnectionID && u.HostID == o.HostID
}

// Equal returns true if both records are exactly the same
func (u UnresolvedConnectionMetadata) Equal(o UnresolvedConnectionMetadata) bool {
	return u == o
}

func (u UnresolvedConnectionMetadata) String() string {
	return fmt.Sprintf(
		"UnresolvedConnectionMetadata{ConnectionID=%s, HostID=%s, Address=%s, CQLPort=%d, SecureCQLPort=%d}",
		u.ConnectionID,
		u.HostID,
		u.Address,
		u.CQLPort,
		u.SecureCQLPort,
	)
}

type UnresolvedConnectionMetadataList []UnresolvedConnectionMetadata

func (l *UnresolvedConnectionMetadataList) Len() int {
	return len(*l)
}

func (l *UnresolvedConnectionMetadataList) MergeWithResolved(resolved ResolvedConnectionMetadataList) ResolvedConnectionMetadataList {
	var ret ResolvedConnectionMetadataList
	for _, metadata := range *l {
		// TODO: Optimize with slices.BinarySearchFunc
		found := false
		for _, res := range resolved {
			if res.SameHost(metadata) {
				found = true
				if res.Equal(metadata) {
					// Records are the same, no information has changed
					ret = append(ret, res)
				} else {
					// Records are not the same, add unresolved record
					// It will be picked up by resolver on very next iteration
					ret = append(ret, ResolvedConnectionMetadata{
						UnresolvedConnectionMetadata: metadata,
						forcedResolve:                true,
					})
				}
				break
			}
		}
		if !found {
			ret = append(ret, ResolvedConnectionMetadata{
				UnresolvedConnectionMetadata: metadata,
				forcedResolve:                true,
			})
		}
	}
	return ret
}

type ResolvedConnectionMetadata struct {
	updateTime time.Time
	UnresolvedConnectionMetadata
	allKnownIPs   []net.IP
	currentIP     net.IP
	forcedResolve bool
}

func (u ResolvedConnectionMetadata) String() string {
	var ip string
	if u.currentIP == nil {
		ip = "<nil>"
	} else {
		ip = u.currentIP.String()
	}

	return fmt.Sprintf(
		"ResolvedConnectionMetadata{ConnectionID=%s, HostID=%s, Address=%s, CQLPort=%d, SecureCQLPort=%d, CurrentIP=%s}",
		u.ConnectionID,
		u.HostID,
		u.Address,
		u.CQLPort,
		u.SecureCQLPort,
		ip,
	)
}

type ResolvedConnectionMetadataList []ResolvedConnectionMetadata

func (l *ResolvedConnectionMetadataList) Len() int {
	return len(*l)
}

func (l *ResolvedConnectionMetadataList) MergeWithUnresolved(unresolved UnresolvedConnectionMetadataList) {
	var updated []int
	for _, res := range *l {
		// TODO: Optimize with slices.BinarySearchFunc
		for id, unres := range unresolved {
			if res.SameHost(unres) {
				updated = append(updated, id)
				if res.Equal(unres) {
					// Records are the same, no information has changed
					break
				} else {
					// Records are not the same, add unresolved record
					// It will be picked up by resolver on very next iteration
					(*l)[id] = ResolvedConnectionMetadata{
						UnresolvedConnectionMetadata: unres,
						forcedResolve:                true,
					}
				}
				break
			}
		}
	}

	for id, unres := range unresolved {
		if !slices.Contains(updated, id) {
			// Add completely new records at the end
			*l = append(*l, ResolvedConnectionMetadata{
				UnresolvedConnectionMetadata: unres,
				forcedResolve:                true,
			})
		}
	}
}

type ResolvedEndpoint struct {
	updateTime    time.Time
	connectionID  string
	dc            string
	rack          string
	address       string
	allKnown      []net.IP
	currentIP     net.IP
	forcedResolve bool
}

type PortMuxResolver interface {
	Resolve(endpoint ResolvedConnectionMetadata) ([]net.IP, net.IP, error)
}

type resolvedCacheRecord struct {
	lastTimeResolved time.Time
	lastResult       []net.IP
}

// simplePortMuxResolver resolves endpoints using the provided lookup function while enforcing
// a minimal period between successive resolutions of the same address.
type simplePortMuxResolver struct {
	resolver         DNSResolver
	cache            map[string]resolvedCacheRecord
	minResolvePeriod time.Duration
	cachingTime      time.Duration
	mu               sync.RWMutex
}

func newSimplePortMuxResolver(minResolvePeriod, cachingTime time.Duration, resolver DNSResolver) *simplePortMuxResolver {
	if resolver == nil {
		resolver = defaultDnsResolver
	}
	return &simplePortMuxResolver{
		resolver:         resolver,
		minResolvePeriod: minResolvePeriod,
		cachingTime:      cachingTime,
		cache:            make(map[string]resolvedCacheRecord),
	}
}

func (r *simplePortMuxResolver) Resolve(endpoint ResolvedConnectionMetadata) (allKnown []net.IP, current net.IP, err error) {
	r.mu.RLock()
	cache, ok := r.cache[endpoint.Address]
	r.mu.RUnlock()
	if ok {
		since := time.Now().UTC().Sub(cache.lastTimeResolved)
		if since < r.cachingTime {
			allKnown = cache.lastResult
		} else if r.minResolvePeriod > 0 && since < r.minResolvePeriod {
			return endpoint.allKnownIPs, endpoint.currentIP, fmt.Errorf("endpoint %s resolved too recently: %s ago < %s", endpoint.Address, since, r.minResolvePeriod)
		}
	}

	if len(allKnown) == 0 {
		allKnown, err = r.resolver.LookupIP(endpoint.Address)
		if err != nil {
			return endpoint.allKnownIPs, endpoint.currentIP, err
		}
		if len(allKnown) == 0 {
			return endpoint.allKnownIPs, endpoint.currentIP, fmt.Errorf("no addresses returned for %s", endpoint.Address)
		}
	}

	for _, addr := range allKnown {
		if endpoint.currentIP != nil && endpoint.currentIP.Equal(addr) {
			current = addr
			break
		}
	}
	if current == nil {
		current = allKnown[0]
	}

	r.mu.Lock()
	r.cache[endpoint.Address] = resolvedCacheRecord{
		lastTimeResolved: time.Now().UTC(),
		lastResult:       allKnown,
	}
	r.mu.Unlock()
	return allKnown, current, nil
}

type PortMuxAddressTranslator struct {
	log               StdLogger
	c                 controlConnection
	sub               *eventbus.Subscriber[events.Event]
	resolvedEndpoints atomic.Pointer[ResolvedConnectionMetadataList]
	updateTasks       chan updateTask
	closeChan         chan struct{}
	cfg               PortMuxConfig
}

// Translate implements old AddressTranslator interface
// should not be uses since driver prefer AddressTranslatorV2 API if it is implemented
func (p *PortMuxAddressTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	panic("should never be called")
}

// TranslateWithHost implements AddressTranslatorV2 interface
func (p *PortMuxAddressTranslator) TranslateWithHost(hostID string, addr net.IP, port int) (net.IP, int) {
	if hostID == "" {
		return addr, port
	}
	hosts := p.getResolveHostPortMapping()
	for _, host := range hosts {
		if host.HostID == hostID {
			if host.SecureCQLPort != 0 {
				return host.currentIP, int(host.SecureCQLPort)
			}
			return host.currentIP, int(host.CQLPort)
		}
	}
	return addr, port
}

func (p *PortMuxAddressTranslator) getResolveHostPortMapping() ResolvedConnectionMetadataList {
	endpoints := p.resolvedEndpoints.Load()
	if endpoints == nil {
		return nil
	}
	return *endpoints
}

var never = time.Unix(1<<63-1, 0)

type updateTask struct {
	result        chan error
	connectionIDs []string
	hostIDs       []string
}

func (p *PortMuxAddressTranslator) Start() {
	p.startUpdateWorker()
}

func (p *PortMuxAddressTranslator) Initialize(s *Session) {
	connectionIDs := make([]string, 0, len(p.cfg.Endpoints))
	for _, ep := range p.cfg.Endpoints {
		if ep.connectionID != "" {
			connectionIDs = append(connectionIDs, ep.connectionID)
		}
	}
	p.c = s.control
	p.sub = s.eventBus.Subscribe("port-mux", 1024, func(event events.Event) bool {
		switch event.Type() {
		case events.SessionEventTypeControlConnectionRecreated, events.ClusterEventTypeConnectionMetadataChanged:
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
}

func (p *PortMuxAddressTranslator) Stop() {
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

// resolveEndpoints updates provided list of resolved endpoint in place
// If it can't resolve it keeps old record as is.
// Logic to pick a single address from all available addresses is delegated to PortMuxResolver at p.endpointResolver
// It does not resolve everything, it picks endpoints that are:
// 1. Marked via forcedResolve=true,
// 2. Have not resolved previously and have no ip address information
// 3. Was resolved more than cfg.ResolveHealthyEndpointPeriod ago.
func (p *PortMuxAddressTranslator) resolveEndpoints(records ResolvedConnectionMetadataList) error {
	if len(records) == 0 {
		return nil
	}

	errs := make([]error, len(records))
	tasks := make(chan int, len(records))

	var cutoffTimeForHealthy time.Time
	if p.cfg.ResolveHealthyEndpointPeriod == 0 {
		cutoffTimeForHealthy = never
	} else {
		cutoffTimeForHealthy = time.Now().UTC().Add(-p.cfg.ResolveHealthyEndpointPeriod)
	}

	scheduled := false
	for id, endpoint := range records {
		if endpoint.currentIP == nil || len(endpoint.allKnownIPs) == 0 || endpoint.forcedResolve {
			scheduled = true
			tasks <- id
		} else if endpoint.updateTime.Before(cutoffTimeForHealthy) {
			scheduled = true
			tasks <- id
		}
	}

	if !scheduled {
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.MaxResolverConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for id := range tasks {
				all, currentIP, err := p.cfg.DNSResolver.Resolve(records[id])
				records[id].updateTime = time.Now().UTC()
				if err != nil {
					errs[id] = fmt.Errorf("resolve %s failed: %w", records[id].currentIP, err)
					continue
				} else if len(all) == 0 {
					errs[id] = fmt.Errorf("resolve %s: no addresses returned", records[id].currentIP)
				} else if currentIP == nil {
					errs[id] = fmt.Errorf("resolve %s: no current addres has been set, should not happen, please report a bug", records[id].currentIP)
				} else {
					// Reset forcedResolve is it was resolved successfully
					records[id].forcedResolve = false
				}
				records[id].allKnownIPs = all
				records[id].currentIP = currentIP
			}
		}()
	}

	close(tasks)
	wg.Wait()

	return errors.Join(errs...)
}

func (p *PortMuxAddressTranslator) updateHostPortMappingAsync(connectionIDs []string, hostIDs []string) {
	p.updateTasks <- updateTask{
		connectionIDs: connectionIDs,
		hostIDs:       hostIDs,
	}
}

func (p *PortMuxAddressTranslator) updateHostPortMappingSync(connectionIDs []string, hostIDs []string) error {
	result := make(chan error, 1)
	p.updateTasks <- updateTask{
		connectionIDs: connectionIDs,
		hostIDs:       hostIDs,
		result:        result,
	}
	return <-result
}

func (p *PortMuxAddressTranslator) startReadingEvents() {
	var connectionIDs []string
	if p.cfg.BlockUnknownConnectionIDs {
		connectionIDs = make([]string, 0, len(p.cfg.Endpoints))
		for _, ep := range p.cfg.Endpoints {
			if ep.connectionID != "" {
				connectionIDs = append(connectionIDs, ep.connectionID)
			}
		}
	}

	go func() {
		for event := range p.sub.Events() {
			switch evt := event.(type) {
			case *events.ConnectionMetadataChangedEvent:
				if debug.Enabled {
					if len(evt.ConnectionIDs) == 0 {
						p.log.Printf("got CONNECTION_METADATA_CHANGE event with no connection IDs")
						continue
					}
					if len(evt.HostIDs) == 0 {
						p.log.Printf("got CONNECTION_METADATA_CHANGE event with no host IDs")
						continue
					}
				}
				var newConnectionIDs []string
				if p.cfg.BlockUnknownConnectionIDs {
					for _, connectionID := range evt.ConnectionIDs {
						if connectionID == "" {
							continue
						}
						if slices.ContainsFunc(p.cfg.Endpoints, func(ep PrivateLinkEndpoint) bool {
							return ep.connectionID == connectionID
						}) {
							newConnectionIDs = append(newConnectionIDs, connectionID)
						}
					}
					if len(newConnectionIDs) != 0 {
						p.updateHostPortMappingAsync(newConnectionIDs, evt.HostIDs)
					}
				} else {
					p.updateHostPortMappingAsync(newConnectionIDs, evt.HostIDs)
				}
			case *events.ControlConnectionRecreatedEvent:
				p.updateHostPortMappingAsync(connectionIDs, nil)
			}
		}
	}()
}

func (p *PortMuxAddressTranslator) startUpdateWorker() {
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

func (p *PortMuxAddressTranslator) updateHostPortMapping(connectionIDs []string, hostIDs []string) error {
	unresolved, err := getHostPortMappingFromCluster(p.c, p.cfg.TableName, connectionIDs, hostIDs)
	if err != nil {
		return err
	}
	current := p.getResolveHostPortMapping()
	cpy := make(ResolvedConnectionMetadataList, len(current))
	if len(current) != 0 {
		copy(cpy, current)
	}

	cpy.MergeWithUnresolved(unresolved)

	err = p.resolveEndpoints(cpy)
	// Despite an error it is better to save results, it should not corrupt existing and resolved records
	p.resolvedEndpoints.Store(&cpy)
	return err
}

func NewPortMuxAddressTranslator(
	cfg PortMuxConfig,
	log StdLogger,
) *PortMuxAddressTranslator {
	return &PortMuxAddressTranslator{
		cfg:         cfg,
		log:         log,
		closeChan:   make(chan struct{}),
		updateTasks: make(chan updateTask, 1024),
	}
}

var _ AddressTranslator = &PortMuxAddressTranslator{}

func getHostPortMappingFromCluster(c controlConnection, table string, connectionIDs []string, hostIDs []string) (UnresolvedConnectionMetadataList, error) {
	var res UnresolvedConnectionMetadataList

	stmt := []string{fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s", table)}
	var bounds []interface{}
	if connectionIDs != nil {
		var inClause []string
		for _, connectionID := range connectionIDs {
			bounds = append(bounds, connectionID)
			inClause = append(inClause, "?")
		}
		if len(stmt) == 1 {
			stmt = append(stmt, "where")
		}
		stmt = append(stmt, fmt.Sprintf("connection_id IN (%s)", strings.Join(inClause, ",")))
	}

	if hostIDs != nil {
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
		stmt = append(stmt, fmt.Sprintf("host_id IN (%s)", strings.Join(inClause, ",")))
	}

	iter := c.query(strings.Join(stmt, " "), bounds...)
	var rec UnresolvedConnectionMetadata
	for iter.Scan(&rec.ConnectionID, &rec.HostID, &rec.Address, &rec.CQLPort, &rec.SecureCQLPort) {
		res = append(res, rec)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("error reading %s table: %v", table, err)
	}
	return res, nil
}
