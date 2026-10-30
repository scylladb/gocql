// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

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

type InternalV2RouteRow struct {
	connection, host, address string
	port, tlsPort             int
}
type InternalV2RouteID struct{}

func (InternalV2RouteID) CheckType(t DataType) error {
	if t.Kind() == TypeUUID {
		return UUIDCodec.CheckType(t)
	}
	return (v2MetadataText{}).CheckType(t)
}
func (InternalV2RouteID) Decode(r ValueReader) (string, error) {
	if r.Type().Kind() == TypeUUID {
		return (v2MetadataUUIDString{}).Decode(r)
	}
	return (v2MetadataText{}).Decode(r)
}
func (InternalV2RouteID) Clone(v string) string { return v }
func InternalV2DecodeRoutes() RowDecoder[InternalV2RouteRow] {
	return v2SchemaRows[InternalV2RouteRow]{InternalFields: []v2SchemaField[InternalV2RouteRow]{
		v2RequiredSchemaColumn("connection_id", InternalV2RouteID{}, func(r *InternalV2RouteRow) *string { return &r.connection }),
		v2RequiredSchemaColumn("host_id", InternalV2RouteID{}, func(r *InternalV2RouteRow) *string { return &r.host }),
		v2RequiredSchemaColumn("address", v2MetadataText{}, func(r *InternalV2RouteRow) *string { return &r.address }),
		v2SchemaColumn("port", v2MetadataInt{}, func(r *InternalV2RouteRow) *int { return &r.port }),
		v2SchemaColumn("tls_port", v2MetadataInt{}, func(r *InternalV2RouteRow) *int { return &r.tlsPort }),
	}}
}

type InternalV2RouteParams struct{ columns Columns }

func (InternalV2RouteParams) Compile(c Columns) (ParamPlan[[][]string], error) {
	return InternalV2RouteParams{c}, nil
}
func (p InternalV2RouteParams) Encode(w *ParamWriter, values [][]string) error {
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
func InternalReadV2ClientRoutes(ctx context.Context, c InternalControlConnection, table, stmt string, bounds [][]string, tls bool) ([]InternalClientRoute, error) {
	if c == nil {
		return nil, errNoControl
	}
	backend, ok := c.(InternalV2SystemQueryer)
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
	if s := c.InternalGetSession(); s != nil && s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout > 0 {
		timeout = s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout
	}
	rows, err := InternalReadV2SystemRowsVia(ctx, 4, timeout, backend.InternalQueryV2System, Statement[[][]string, InternalV2RouteRow]{CQL: stmt, Params: InternalV2RouteParams{}, Rows: InternalV2DecodeRoutes()}, NewColumns(columns...), bounds)
	if err != nil {
		return nil, fmt.Errorf("error reading %s table: %w", table, err)
	}
	result := make([]InternalClientRoute, 0, len(rows))
	seen := make(map[InternalPair]bool, len(rows))
	for _, r := range rows {
		key := InternalPair{r.connection, r.host}
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
		result = append(result, InternalClientRoute{r.connection, r.host, r.address, uint16(port)})
	}
	return result, nil
}
func (p *ClientRoutesHandler) refreshContext(ctx context.Context) error {
	return p.updateHostPortMappingContext(ctx, InternalUpdateTask{InternalConnectionIDs: p.privateStorage.InternalCfg.Endpoints.GetAllConnectionIDs()})
}
func InternalClientRoutesHandlerOrderedRoutes(p *ClientRoutesHandler, hostID string) []InternalClientRoute {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	entry, ok := p.privateStorage.InternalRouteCache.InternalRoutes[hostID]
	if !ok {
		return nil
	}
	out := make([]InternalClientRoute, 0, len(entry.InternalAllRoutes))
	if entry.current != nil {
		if index := entry.routeIndex(entry.current.InternalConnectionID); index >= 0 {
			out = append(out, entry.InternalAllRoutes[index])
		}
	}
	ids := p.privateStorage.InternalCfg.Endpoints.GetAllConnectionIDs()
	if len(ids) == 0 {
		for _, r := range entry.InternalAllRoutes {
			ids = append(ids, r.InternalConnectionID)
		}
	}
	for _, id := range ids {
		if len(out) > 0 && out[0].InternalConnectionID == id {
			continue
		}
		if index := entry.routeIndex(id); index >= 0 {
			out = append(out, entry.InternalAllRoutes[index])
		}
	}
	return out
}
func InternalClientRoutesHandlerBindRoute(p *ClientRoutesHandler, route InternalClientRoute) bool {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	entry, ok := p.privateStorage.InternalRouteCache.InternalRoutes[route.InternalHostID]
	if !ok {
		return false
	}
	i := entry.routeIndex(route.InternalConnectionID)
	if i < 0 || entry.InternalAllRoutes[i] != route {
		return false
	}
	entry.BindCurrent(route.InternalConnectionID)
	p.privateStorage.InternalRouteCache.InternalRoutes[route.InternalHostID] = entry
	return true
}
func (p *ClientRoutesHandler) resolveRoute(ctx context.Context, route InternalClientRoute) ([]AddressPort, error) {
	if route.InternalPort == 0 {
		return nil, typeValidation("client route", "missing required port")
	}
	name := route.InternalAddress
	if override := p.privateStorage.InternalAddrOverrides[route.InternalConnectionID]; override != "" {
		name = override
	}
	var ips []net.IP
	var err error
	if resolver, ok := p.privateStorage.InternalResolver.(interface {
		LookupIPContext(context.Context, string) ([]net.IP, error)
	}); ok {
		ips, err = resolver.LookupIPContext(ctx, name)
	} else {
		ips, err = p.privateStorage.InternalResolver.LookupIP(name)
	}
	if err != nil {
		return nil, err
	}
	var out []AddressPort
	for _, ip := range ips {
		if validIpAddr(ip) {
			out = append(out, AddressPort{Address: ip, Port: route.InternalPort})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no addresses for route %s/%s (%s)", route.InternalHostID, route.InternalConnectionID, name)
	}
	return out, ctx.Err()
}

// Route read publication compares both the connection scope and exact pair
// epochs. Full reads preserve a pair invalidated by an event after read start.
type InternalV2RouteEpochs struct {
	connections map[string]uint64
	pairs       map[InternalPair]uint64
}

func InternalClientRoutesHandlerCaptureRouteEpochs(p *ClientRoutesHandler) InternalV2RouteEpochs {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	return InternalV2RouteEpochs{maps.Clone(p.privateStorage.connectionEpoch), maps.Clone(p.privateStorage.pairEpoch)}
}
func InternalClientRoutesHandlerInvalidateRouteTask(p *ClientRoutesHandler, task InternalUpdateTask) {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	if p.privateStorage.connectionEpoch == nil {
		p.privateStorage.connectionEpoch = make(map[string]uint64)
	}
	if p.privateStorage.pairEpoch == nil {
		p.privateStorage.pairEpoch = make(map[InternalPair]uint64)
	}
	if task.InternalPairs != nil {
		for _, key := range task.InternalPairs {
			p.privateStorage.pairEpoch[key]++
		}
	} else {
		for _, id := range task.InternalConnectionIDs {
			p.privateStorage.connectionEpoch[id]++
		}
	}
	touched := make(map[string]bool)
	if task.InternalPairs != nil {
		for _, key := range task.InternalPairs {
			touched[key.InternalHostID] = true
		}
	} else {
		for host, entry := range p.privateStorage.InternalRouteCache.InternalRoutes {
			for _, route := range entry.InternalAllRoutes {
				if slices.Contains(task.InternalConnectionIDs, route.InternalConnectionID) {
					touched[host] = true
					break
				}
			}
		}
	}
	p.advanceRouteHostEpochsLocked(touched)

}
func InternalClientRoutesHandlerApplyRouteRead(p *ClientRoutesHandler, task InternalUpdateTask, epochs InternalV2RouteEpochs, incoming []InternalClientRoute) bool {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	valid := func(key InternalPair) bool {
		return p.privateStorage.connectionEpoch[key.InternalConnectionID] == epochs.connections[key.InternalConnectionID] && p.privateStorage.pairEpoch[key] == epochs.pairs[key]
	}
	current := p.privateStorage.InternalRouteCache.snapshotCurrentConnectionIDsLocked()
	keys := slices.Clone(task.InternalPairs)
	if task.InternalPairs == nil {
		for host, entry := range p.privateStorage.InternalRouteCache.InternalRoutes {
			for _, r := range entry.InternalAllRoutes {
				if slices.Contains(task.InternalConnectionIDs, r.InternalConnectionID) {
					keys = append(keys, InternalPair{r.InternalConnectionID, host})
				}
			}
		}
	}
	for _, key := range keys {
		if valid(key) {
			p.privateStorage.InternalRouteCache.deleteByPairsLocked([]InternalPair{key})
		}
	}
	stale := false
	if task.InternalPairs == nil {
		for _, id := range task.InternalConnectionIDs {
			if p.privateStorage.connectionEpoch[id] != epochs.connections[id] {
				stale = true
			}
		}
		for key, revision := range p.privateStorage.pairEpoch {
			if slices.Contains(task.InternalConnectionIDs, key.InternalConnectionID) && revision != epochs.pairs[key] {
				stale = true
			}
		}
	}
	for _, r := range incoming {
		if valid(InternalPair{r.InternalConnectionID, r.InternalHostID}) {
			p.privateStorage.InternalRouteCache.upsertRecordsLocked([]InternalClientRoute{r})
		} else {
			stale = true
		}
	}
	for _, key := range keys {
		if !valid(key) {
			stale = true
		}
	}
	p.privateStorage.InternalRouteCache.rebindCurrentConnectionIDsLocked(current)
	if p.privateStorage.InternalSession != nil && p.privateStorage.InternalSession.privateStorage.InternalHostSource != nil {
		changed := make(map[string]bool)
		for _, r := range incoming {
			changed[r.InternalHostID] = true
		}
		for _, key := range keys {
			changed[key.InternalHostID] = true
		}
		for id := range changed {
			if host := p.privateStorage.InternalSession.privateStorage.InternalHostSource.InternalGetHost(id); host != nil {
				implEndpoint.HostInfoData(host).InternalMu.Lock()
				implEndpoint.HostInfoData(host).InternalTransportGeneration++
				implEndpoint.HostInfoData(host).InternalMu.Unlock()
			}
		}
	}
	return !stale
}
func (p *ClientRoutesHandler) updateHostPortMappingContext(ctx context.Context, task InternalUpdateTask) error {
	epochs := InternalClientRoutesHandlerCaptureRouteEpochs(p)
	var rows []InternalClientRoute
	var err error
	switch {
	case task.InternalPairs != nil:
		rows, err = InternalGetV2RoutesPairs(ctx, p.privateStorage.InternalC, p.privateStorage.InternalCfg.TableName, task.InternalPairs, p.privateStorage.InternalPickTLSPorts)
	case task.InternalConnectionIDs != nil:
		rows, err = InternalGetV2RoutesConnections(ctx, p.privateStorage.InternalC, p.privateStorage.InternalCfg.TableName, task.InternalConnectionIDs, p.privateStorage.InternalPickTLSPorts)
	default:
		return fmt.Errorf("updateTask has neither pairs nor connectionIDs")
	}
	if err != nil {
		if p.privateStorage.InternalSession != nil {
			state := InternalSessionPeripheral(p.privateStorage.InternalSession)
			state.InternalMu.Lock()
			state.routeDiagnostic = fmt.Errorf("client routes retained stale data: %w", err)
			state.InternalMu.Unlock()
			InternalSessionPublishV2Metadata(p.privateStorage.InternalSession)
		}
		return &PartialRefreshError{Generation: p.routeGeneration(), ClientRoutes: err}
	}
	if !InternalClientRoutesHandlerApplyRouteRead(p, task, epochs, rows) {
		return &PartialRefreshError{Generation: p.routeGeneration(), ClientRoutes: fmt.Errorf("routes changed during refresh")}
	}
	if p.privateStorage.InternalSession != nil {
		state := InternalSessionPeripheral(p.privateStorage.InternalSession)
		state.InternalMu.Lock()
		state.routeDiagnostic = nil
		state.InternalMu.Unlock()
		if !InternalSessionPublishV2Metadata(p.privateStorage.InternalSession) {
			return p.privateStorage.InternalSession.v2MetadataPublicationError()
		}
		InternalSessionPublishV2LocalEvent(p.privateStorage.InternalSession, Event{Kind: ClientRouteEvent, Generation: p.privateStorage.InternalSession.Metadata().Generation(), Change: "REFRESHED"})
	}
	return nil
}
func (p *ClientRoutesHandler) routeGeneration() uint64 {
	if p.privateStorage.InternalSession != nil {
		return p.privateStorage.InternalSession.Metadata().Generation()
	}
	return 0
}
func InternalGetV2RoutesConnections(ctx context.Context, c InternalControlConnection, table string, ids []string, tls bool) ([]InternalClientRoute, error) {
	if len(ids) == 0 {
		return nil, typeValidation("routes", "connection IDs cannot be empty")
	}
	if table == "" {
		table = "system.client_routes"
	}
	if !InternalValidV2RouteTable(table) {
		return nil, typeValidation("routes table", "invalid qualified identifier")
	}
	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ?", table)
	return InternalReadV2ClientRoutes(ctx, c, table, stmt, [][]string{ids}, tls)
}
func InternalGetV2RoutesPairs(ctx context.Context, c InternalControlConnection, table string, pairs []InternalPair, tls bool) ([]InternalClientRoute, error) {
	if len(pairs) == 0 {
		return nil, typeValidation("routes", "pairs cannot be empty")
	}
	if table == "" {
		table = "system.client_routes"
	}
	if !InternalValidV2RouteTable(table) {
		return nil, typeValidation("routes table", "invalid qualified identifier")
	}
	ids, hosts := make([]string, len(pairs)), make([]string, len(pairs))
	for i, p := range pairs {
		ids[i], hosts[i] = p.InternalConnectionID, p.InternalHostID
	}
	stmt := fmt.Sprintf("select connection_id, host_id, address, port, tls_port from %s where connection_id in ? and host_id in ?", table)
	rows, err := InternalReadV2ClientRoutes(ctx, c, table, stmt, [][]string{ids, hosts}, tls)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(rows, func(r InternalClientRoute) bool {
		return !slices.Contains(pairs, InternalPair{r.InternalConnectionID, r.InternalHostID})
	}), nil
}
func InternalValidV2RouteTable(table string) bool {
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
	InternalClientRoutesHandlerHandleRouteEventOwned(p, event, nil)
}
func InternalClientRoutesHandlerHandleRouteEventOwned(p *ClientRoutesHandler, event events.Event, release func()) bool {
	switch e := event.(type) {
	case *events.ControlConnectionRecreatedEvent:
		return InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, InternalUpdateTask{InternalConnectionIDs: p.privateStorage.InternalCfg.Endpoints.GetAllConnectionIDs(), InternalRelease: release})
	case *events.ClientRoutesChangedEvent:
		ids := InternalFilterAllowedConnectionIDs(e.ConnectionIDs, p.privateStorage.InternalCfg.Endpoints.GetAllConnectionIDs())
		if len(ids) == 0 {
			return false
		}
		task := InternalUpdateTask{InternalConnectionIDs: ids}
		if pairs := InternalGetPairsFromEvent(ids, e.HostIDs); len(pairs) > 0 {
			task = InternalUpdateTask{InternalPairs: pairs}
		}
		if e.ChangeType == "DELETED" || e.ChangeType == "DROPPED" {
			InternalClientRoutesHandlerInvalidateRouteTask(p, task)
			p.privateStorage.InternalRouteCache.mu.Lock()
			current := p.privateStorage.InternalRouteCache.snapshotCurrentConnectionIDsLocked()
			if task.InternalPairs != nil {
				p.privateStorage.InternalRouteCache.deleteByPairsLocked(task.InternalPairs)
			} else {
				p.privateStorage.InternalRouteCache.deleteByConnectionIDsLocked(task.InternalConnectionIDs)
			}
			p.privateStorage.InternalRouteCache.rebindCurrentConnectionIDsLocked(current)
			p.privateStorage.InternalRouteCache.mu.Unlock()
			if p.privateStorage.InternalSession != nil {
				InternalSessionPublishV2Metadata(p.privateStorage.InternalSession)
				p.privateStorage.InternalSession.emitV2Event(e)
			}
		}
		task.InternalRelease = release
		return InternalClientRoutesHandlerUpdateHostPortMappingAsync(p, task)
	}
	return false

}

func (p *ClientRoutesHandler) routeMatches(route InternalClientRoute, epochs InternalV2RouteEpochs) bool {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	if p.privateStorage.connectionEpoch[route.InternalConnectionID] != epochs.connections[route.InternalConnectionID] || p.privateStorage.pairEpoch[InternalPair{route.InternalConnectionID, route.InternalHostID}] != epochs.pairs[InternalPair{route.InternalConnectionID, route.InternalHostID}] {
		return false
	}
	entry, ok := p.privateStorage.InternalRouteCache.InternalRoutes[route.InternalHostID]
	if !ok {
		return false
	}
	i := entry.routeIndex(route.InternalConnectionID)
	return i >= 0 && entry.InternalAllRoutes[i] == route
}
func (s *Session) dialV2Routes(ctx context.Context, host *HostInfo, cfg *ConnConfig, handler ConnErrorHandler) (*Conn, error) {
	p := s.privateStorage.InternalClientRoutesHandler
	var causes []error
	epochs := p.captureHostRouteEpochs(host.HostID())
	for _, route := range InternalClientRoutesHandlerOrderedRoutes(p, host.HostID()) {
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
			implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, translatedAddresses{CQL: endpoint})
			conn, err := InternalSessionDialWithoutObserver(s, ctx, host, cfg, handler, 0, 0)
			if err != nil {
				causes = append(causes, err)
				continue
			}
			prior := conn.InternalValidateTarget
			conn.InternalValidateTarget = func(ctx context.Context) error {
				if !p.routeMatches(route, epochs) {
					return typeValidation("route generation", "route changed while opening")
				}
				if prior != nil {
					return prior(ctx)
				}
				return nil
			}
			if !p.routeMatches(route, epochs) || !InternalClientRoutesHandlerBindRoute(p, route) {
				conn.Close()
				conn.InternalWaitEventDispatch()
				return nil, typeValidation("route generation", "route superseded during opening")
			}
			return conn, nil
		}
	}
	return nil, fmt.Errorf("no usable client route for host %s: %w", host.HostID(), errors.Join(causes...))
}

func (p *ClientRoutesHandler) captureHostRouteEpochs(hostID string) InternalV2RouteEpochs {
	p.privateStorage.InternalRouteCache.mu.Lock()
	defer p.privateStorage.InternalRouteCache.mu.Unlock()
	out := InternalV2RouteEpochs{connections: make(map[string]uint64), pairs: make(map[InternalPair]uint64)}
	for _, route := range p.privateStorage.InternalRouteCache.InternalRoutes[hostID].InternalAllRoutes {
		out.connections[route.InternalConnectionID] = p.privateStorage.connectionEpoch[route.InternalConnectionID]
		key := InternalPair{route.InternalConnectionID, hostID}
		out.pairs[key] = p.privateStorage.pairEpoch[key]
	}
	return out
}

// Called under the route cache publication gate. The transport insertion gate
// checks the same host epoch under host.mu, closing the validation/store gap.
func (p *ClientRoutesHandler) advanceRouteHostEpochsLocked(hosts map[string]bool) {
	if p.privateStorage.InternalSession == nil || p.privateStorage.InternalSession.privateStorage.InternalHostSource == nil {
		return
	}
	for id := range hosts {
		if host := p.privateStorage.InternalSession.privateStorage.InternalHostSource.InternalGetHost(id); host != nil {
			implEndpoint.HostInfoData(host).InternalMu.Lock()
			implEndpoint.HostInfoData(host).InternalTransportGeneration++
			implEndpoint.HostInfoData(host).InternalMu.Unlock()
		}
	}
}
