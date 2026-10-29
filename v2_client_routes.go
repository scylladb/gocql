package gocql

import (
	"context"
	"errors"
	"fmt"
	"github.com/scylladb/gocql/v2/events"
	"maps"
	"net"
	"slices"
	"strings"
	"time"
)

type v2RouteRow struct {
	connection, host, address string
	port, tlsPort             int
}
type v2RouteID struct{}

func (v2RouteID) CheckType(t DataType) error {
	if t.Kind() == TypeUUID {
		return UUIDCodec.CheckType(t)
	}
	return (v2MetadataText{}).CheckType(t)
}
func (v2RouteID) Decode(r ValueReader) (string, error) {
	if r.Type().Kind() == TypeUUID {
		return (v2MetadataUUIDString{}).Decode(r)
	}
	return (v2MetadataText{}).Decode(r)
}
func (v2RouteID) Clone(v string) string { return v }
func v2DecodeRoutes() RowDecoder[v2RouteRow] {
	return v2SchemaRows[v2RouteRow]{fields: []v2SchemaField[v2RouteRow]{
		v2RequiredSchemaColumn("connection_id", v2RouteID{}, func(r *v2RouteRow) *string { return &r.connection }),
		v2RequiredSchemaColumn("host_id", v2RouteID{}, func(r *v2RouteRow) *string { return &r.host }),
		v2RequiredSchemaColumn("address", v2MetadataText{}, func(r *v2RouteRow) *string { return &r.address }),
		v2SchemaColumn("port", v2MetadataInt{}, func(r *v2RouteRow) *int { return &r.port }),
		v2SchemaColumn("tls_port", v2MetadataInt{}, func(r *v2RouteRow) *int { return &r.tlsPort }),
	}}
}

type v2RouteParams struct{ columns Columns }

func (v2RouteParams) Compile(c Columns) (ParamPlan[[][]string], error) { return v2RouteParams{c}, nil }
func (p v2RouteParams) Encode(w *ParamWriter, values [][]string) error {
	if len(values) != p.columns.Len() {
		return typeValidation("route query", "wrong argument count")
	}
	for i, vs := range values {
		c, _ := p.columns.At(i)
		element, _ := c.Type.Element()
		if element.Kind() == TypeUUID {
			ids := make([]UUID, len(vs))
			for j, v := range vs {
				id, err := ParseUUID(v)
				if err != nil {
					return err
				}
				ids[j] = id
			}
			if err := WriteParam(w, ListEncoder(UUIDCodec), ids); err != nil {
				return err
			}
		} else if err := WriteParam(w, ListEncoder(StringEncoder), vs); err != nil {
			return err
		}
	}
	return nil
}
func readV2ClientRoutes(ctx context.Context, c controlConnection, table, stmt string, bounds [][]string, tls bool) ([]clientRoute, error) {
	if c == nil {
		return nil, errNoControl
	}
	backend, ok := c.(v2SystemQueryer)
	if !ok {
		return nil, typeValidation("client routes", "control does not support typed queries")
	}
	columns := make([]ColumnSpec, len(bounds))
	for i, vs := range bounds {
		kind := TypeUUID
		for _, v := range vs {
			if _, err := ParseUUID(v); err != nil {
				kind = TypeVarchar
				break
			}
		}
		typ, _ := ScalarType(kind)
		list, _ := ListType(typ)
		columns[i] = ColumnSpec{Type: list}
	}
	timeout := time.Minute
	if s := c.getSession(); s != nil && s.cfg.MetadataSchemaRequestTimeout > 0 {
		timeout = s.cfg.MetadataSchemaRequestTimeout
	}
	rows, err := readV2SystemRowsVia(ctx, 4, timeout, backend.queryV2System, Statement[[][]string, v2RouteRow]{CQL: stmt, Params: v2RouteParams{}, Rows: v2DecodeRoutes()}, NewColumns(columns...), bounds)
	if err != nil {
		return nil, fmt.Errorf("error reading %s table: %w", table, err)
	}
	result := make([]clientRoute, 0, len(rows))
	seen := make(map[pair]bool, len(rows))
	for _, r := range rows {
		key := pair{r.connection, r.host}
		if r.connection == "" || r.host == "" || seen[key] || r.address == "" || r.port < 0 || r.port > 65535 || r.tlsPort < 0 || r.tlsPort > 65535 {
			return nil, typeValidation("client route", "invalid/duplicate record %s/%s", r.connection, r.host)
		}
		seen[key] = true
		port := r.port
		if tls {
			port = r.tlsPort
		}
		if port == 0 {
			return nil, typeValidation("client route", "record %s/%s missing required port", r.connection, r.host)
		}
		result = append(result, clientRoute{r.connection, r.host, r.address, uint16(port)})
	}
	return result, nil
}
func (p *ClientRoutesHandler) refreshContext(ctx context.Context) error {
	return p.updateHostPortMappingContext(ctx, updateTask{connectionIDs: p.cfg.Endpoints.GetAllConnectionIDs()})
}
func (p *ClientRoutesHandler) orderedRoutes(hostID string) []clientRoute {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	entry, ok := p.routeCache.routes[hostID]
	if !ok {
		return nil
	}
	out := make([]clientRoute, 0, len(entry.allRoutes))
	if entry.current != nil {
		if index := entry.routeIndex(entry.current.connectionID); index >= 0 {
			out = append(out, entry.allRoutes[index])
		}
	}
	ids := p.cfg.Endpoints.GetAllConnectionIDs()
	if len(ids) == 0 {
		for _, r := range entry.allRoutes {
			ids = append(ids, r.connectionID)
		}
	}
	for _, id := range ids {
		if len(out) > 0 && out[0].connectionID == id {
			continue
		}
		if index := entry.routeIndex(id); index >= 0 {
			out = append(out, entry.allRoutes[index])
		}
	}
	return out
}
func (p *ClientRoutesHandler) bindRoute(route clientRoute) bool {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	entry, ok := p.routeCache.routes[route.hostID]
	if !ok {
		return false
	}
	i := entry.routeIndex(route.connectionID)
	if i < 0 || entry.allRoutes[i] != route {
		return false
	}
	entry.BindCurrent(route.connectionID)
	p.routeCache.routes[route.hostID] = entry
	return true
}
func (p *ClientRoutesHandler) resolveRoute(ctx context.Context, route clientRoute) ([]AddressPort, error) {
	if route.port == 0 {
		return nil, typeValidation("client route", "missing required port")
	}
	name := route.address
	if override := p.addrOverrides[route.connectionID]; override != "" {
		name = override
	}
	var ips []net.IP
	var err error
	if resolver, ok := p.resolver.(interface {
		LookupIPContext(context.Context, string) ([]net.IP, error)
	}); ok {
		ips, err = resolver.LookupIPContext(ctx, name)
	} else {
		ips, err = p.resolver.LookupIP(name)
	}
	if err != nil {
		return nil, err
	}
	var out []AddressPort
	for _, ip := range ips {
		if validIpAddr(ip) {
			out = append(out, AddressPort{ip, route.port})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no addresses for route %s/%s (%s)", route.hostID, route.connectionID, name)
	}
	return out, ctx.Err()
}

// Route read publication compares both the connection scope and exact pair
// epochs. Full reads preserve a pair invalidated by an event after read start.
type v2RouteEpochs struct {
	connections map[string]uint64
	pairs       map[pair]uint64
}

func (p *ClientRoutesHandler) captureRouteEpochs() v2RouteEpochs {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	return v2RouteEpochs{maps.Clone(p.connectionEpoch), maps.Clone(p.pairEpoch)}
}
func (p *ClientRoutesHandler) invalidateRouteTask(task updateTask) {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	if p.connectionEpoch == nil {
		p.connectionEpoch = make(map[string]uint64)
	}
	if p.pairEpoch == nil {
		p.pairEpoch = make(map[pair]uint64)
	}
	if task.pairs != nil {
		for _, key := range task.pairs {
			p.pairEpoch[key]++
		}
	} else {
		for _, id := range task.connectionIDs {
			p.connectionEpoch[id]++
		}
	}
	touched := make(map[string]bool)
	if task.pairs != nil {
		for _, key := range task.pairs {
			touched[key.hostID] = true
		}
	} else {
		for host, entry := range p.routeCache.routes {
			for _, route := range entry.allRoutes {
				if slices.Contains(task.connectionIDs, route.connectionID) {
					touched[host] = true
					break
				}
			}
		}
	}
	p.advanceRouteHostEpochsLocked(touched)

}
func (p *ClientRoutesHandler) applyRouteRead(task updateTask, epochs v2RouteEpochs, incoming []clientRoute) bool {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	valid := func(key pair) bool {
		return p.connectionEpoch[key.connectionID] == epochs.connections[key.connectionID] && p.pairEpoch[key] == epochs.pairs[key]
	}
	current := p.routeCache.snapshotCurrentConnectionIDsLocked()
	keys := slices.Clone(task.pairs)
	if task.pairs == nil {
		for host, entry := range p.routeCache.routes {
			for _, r := range entry.allRoutes {
				if slices.Contains(task.connectionIDs, r.connectionID) {
					keys = append(keys, pair{r.connectionID, host})
				}
			}
		}
	}
	for _, key := range keys {
		if valid(key) {
			p.routeCache.deleteByPairsLocked([]pair{key})
		}
	}
	stale := false
	if task.pairs == nil {
		for _, id := range task.connectionIDs {
			if p.connectionEpoch[id] != epochs.connections[id] {
				stale = true
			}
		}
		for key, revision := range p.pairEpoch {
			if slices.Contains(task.connectionIDs, key.connectionID) && revision != epochs.pairs[key] {
				stale = true
			}
		}
	}
	for _, r := range incoming {
		if valid(pair{r.connectionID, r.hostID}) {
			p.routeCache.upsertRecordsLocked([]clientRoute{r})
		} else {
			stale = true
		}
	}
	for _, key := range keys {
		if !valid(key) {
			stale = true
		}
	}
	p.routeCache.rebindCurrentConnectionIDsLocked(current)
	if p.session != nil && p.session.hostSource != nil {
		changed := make(map[string]bool)
		for _, r := range incoming {
			changed[r.hostID] = true
		}
		for _, key := range keys {
			changed[key.hostID] = true
		}
		for id := range changed {
			if host := p.session.hostSource.getHost(id); host != nil {
				host.mu.Lock()
				host.transportGeneration++
				host.mu.Unlock()
			}
		}
	}
	return !stale
}
func (p *ClientRoutesHandler) updateHostPortMappingContext(ctx context.Context, task updateTask) error {
	epochs := p.captureRouteEpochs()
	var rows []clientRoute
	var err error
	switch {
	case task.pairs != nil:
		rows, err = getV2RoutesPairs(ctx, p.c, p.cfg.TableName, task.pairs, p.pickTLSPorts)
	case task.connectionIDs != nil:
		rows, err = getV2RoutesConnections(ctx, p.c, p.cfg.TableName, task.connectionIDs, p.pickTLSPorts)
	default:
		return fmt.Errorf("updateTask has neither pairs nor connectionIDs")
	}
	if err != nil {
		if p.session != nil {
			state := p.session.peripheral()
			state.mu.Lock()
			state.routeDiagnostic = fmt.Errorf("client routes retained stale data: %w", err)
			state.mu.Unlock()
			p.session.publishV2Metadata()
		}
		return &PartialRefreshError{Generation: p.routeGeneration(), ClientRoutes: err}
	}
	if !p.applyRouteRead(task, epochs, rows) {
		return &PartialRefreshError{Generation: p.routeGeneration(), ClientRoutes: fmt.Errorf("routes changed during refresh")}
	}
	if p.session != nil {
		state := p.session.peripheral()
		state.mu.Lock()
		state.routeDiagnostic = nil
		state.mu.Unlock()
		if !p.session.publishV2Metadata() {
			return p.session.v2MetadataPublicationError()
		}
		p.session.publishV2LocalEvent(Event{Kind: ClientRouteEvent, Generation: p.session.Metadata().Generation(), Change: "REFRESHED"})
	}
	return nil
}
func (p *ClientRoutesHandler) routeGeneration() uint64 {
	if p.session != nil {
		return p.session.Metadata().Generation()
	}
	return 0
}
func getV2RoutesConnections(ctx context.Context, c controlConnection, table string, ids []string, tls bool) ([]clientRoute, error) {
	if len(ids) == 0 {
		return nil, typeValidation("routes", "connection IDs cannot be empty")
	}
	if table == "" {
		table = "system.client_routes"
	}
	if !validV2RouteTable(table) {
		return nil, typeValidation("routes table", "invalid qualified identifier")
	}
	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ?", table)
	return readV2ClientRoutes(ctx, c, table, stmt, [][]string{ids}, tls)
}
func getV2RoutesPairs(ctx context.Context, c controlConnection, table string, pairs []pair, tls bool) ([]clientRoute, error) {
	if len(pairs) == 0 {
		return nil, typeValidation("routes", "pairs cannot be empty")
	}
	if table == "" {
		table = "system.client_routes"
	}
	if !validV2RouteTable(table) {
		return nil, typeValidation("routes table", "invalid qualified identifier")
	}
	ids, hosts := make([]string, len(pairs)), make([]string, len(pairs))
	for i, p := range pairs {
		ids[i], hosts[i] = p.connectionID, p.hostID
	}
	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ? and host_id in ?", table)
	rows, err := readV2ClientRoutes(ctx, c, table, stmt, [][]string{ids, hosts}, tls)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(rows, func(r clientRoute) bool { return !slices.Contains(pairs, pair{r.connectionID, r.hostID}) }), nil
}
func validV2RouteTable(table string) bool {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for i, c := range part {
			if !(c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9') {
				return false
			}
		}
	}
	return true
}

// Direct internal notification bypasses the best-effort legacy event bus.
func (p *ClientRoutesHandler) handleRouteEvent(event events.Event) {
	p.handleRouteEventOwned(event, nil)
}
func (p *ClientRoutesHandler) handleRouteEventOwned(event events.Event, release func()) bool {
	switch e := event.(type) {
	case *events.ControlConnectionRecreatedEvent:
		return p.updateHostPortMappingAsync(updateTask{connectionIDs: p.cfg.Endpoints.GetAllConnectionIDs(), release: release})
	case *events.ClientRoutesChangedEvent:
		ids := filterAllowedConnectionIDs(e.ConnectionIDs, p.cfg.Endpoints.GetAllConnectionIDs())
		if len(ids) == 0 {
			return false
		}
		task := updateTask{connectionIDs: ids}
		if pairs := getPairsFromEvent(ids, e.HostIDs); len(pairs) > 0 {
			task = updateTask{pairs: pairs}
		}
		if e.ChangeType == "DELETED" || e.ChangeType == "DROPPED" {
			p.invalidateRouteTask(task)
			p.routeCache.mu.Lock()
			current := p.routeCache.snapshotCurrentConnectionIDsLocked()
			if task.pairs != nil {
				p.routeCache.deleteByPairsLocked(task.pairs)
			} else {
				p.routeCache.deleteByConnectionIDsLocked(task.connectionIDs)
			}
			p.routeCache.rebindCurrentConnectionIDsLocked(current)
			p.routeCache.mu.Unlock()
			if p.session != nil {
				p.session.publishV2Metadata()
				p.session.emitV2Event(e)
			}
		}
		task.release = release
		return p.updateHostPortMappingAsync(task)
	}
	return false

}

func (p *ClientRoutesHandler) routeMatches(route clientRoute, epochs v2RouteEpochs) bool {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	if p.connectionEpoch[route.connectionID] != epochs.connections[route.connectionID] || p.pairEpoch[pair{route.connectionID, route.hostID}] != epochs.pairs[pair{route.connectionID, route.hostID}] {
		return false
	}
	entry, ok := p.routeCache.routes[route.hostID]
	if !ok {
		return false
	}
	i := entry.routeIndex(route.connectionID)
	return i >= 0 && entry.allRoutes[i] == route
}
func (s *Session) dialV2Routes(ctx context.Context, host *HostInfo, cfg *ConnConfig, handler ConnErrorHandler) (*Conn, error) {
	p := s.clientRoutesHandler
	var causes []error
	epochs := p.captureHostRouteEpochs(host.HostID())
	for _, route := range p.orderedRoutes(host.HostID()) {
		endpoints, err := p.resolveRoute(ctx, route)
		if err != nil {
			causes = append(causes, err)
			continue
		}
		for _, endpoint := range endpoints {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !p.routeMatches(route, epochs) {
				return nil, typeValidation("route generation", "route changed while opening")
			}
			host.setTranslatedConnectionInfo(translatedAddresses{CQL: endpoint})
			conn, err := s.dialWithoutObserver(ctx, host, cfg, handler, 0, 0)
			if err != nil {
				causes = append(causes, err)
				continue
			}
			prior := conn.validateTarget
			conn.validateTarget = func(ctx context.Context) error {
				if !p.routeMatches(route, epochs) {
					return typeValidation("route generation", "route changed while opening")
				}
				if prior != nil {
					return prior(ctx)
				}
				return nil
			}
			if !p.routeMatches(route, epochs) || !p.bindRoute(route) {
				conn.Close()
				conn.waitEventDispatch()
				return nil, typeValidation("route generation", "route superseded during opening")
			}
			return conn, nil
		}
	}
	return nil, fmt.Errorf("no usable client route for host %s: %w", host.HostID(), errors.Join(causes...))
}

func (p *ClientRoutesHandler) captureHostRouteEpochs(hostID string) v2RouteEpochs {
	p.routeCache.mu.Lock()
	defer p.routeCache.mu.Unlock()
	out := v2RouteEpochs{connections: make(map[string]uint64), pairs: make(map[pair]uint64)}
	for _, route := range p.routeCache.routes[hostID].allRoutes {
		out.connections[route.connectionID] = p.connectionEpoch[route.connectionID]
		key := pair{route.connectionID, hostID}
		out.pairs[key] = p.pairEpoch[key]
	}
	return out
}

// Called under the route cache publication gate. The transport insertion gate
// checks the same host epoch under host.mu, closing the validation/store gap.
func (p *ClientRoutesHandler) advanceRouteHostEpochsLocked(hosts map[string]bool) {
	if p.session == nil || p.session.hostSource == nil {
		return
	}
	for id := range hosts {
		if host := p.session.hostSource.getHost(id); host != nil {
			host.mu.Lock()
			host.transportGeneration++
			host.mu.Unlock()
		}
	}
}
