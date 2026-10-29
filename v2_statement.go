package gocql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type Statement[P, R any] struct {
	CQL    string
	Params ParamEncoder[P]
	Rows   RowDecoder[R]
}
type PrepareOptions struct {
	connection *Conn
	Keyspace   string
	HostID     Option[UUID]
	Trace      bool
}
type PreparedInfo struct {
	Keyspace, Table     string
	PartitionKeyIndexes []int
	Conditional         bool
	Generation          uint64
}
type hostPrepared struct {
	epoch           preparedSchemaStamp
	result          *typedResultMetadata
	revision        uint64
	metadataChanged bool
	id, metadataID  []byte
	columns         Columns
	generation      uint64
}
type preparedState struct {
	conflictDirty       atomic.Bool
	conflictMu          sync.Mutex
	conflictEpoch       atomic.Uint64
	conflictRefresh     atomic.Bool
	resultHost          UUID
	result              *typedResultMetadata
	mu                  sync.RWMutex
	session             *Session
	protocol            uint8
	statement, keyspace string
	parameters, columns Columns
	info                PreparedInfo
	hosts               map[UUID]*hostPrepared
	preparing           map[UUID]*hostPrepareFlight
	dependencies        *preparedDependencies
	partitioner         string
	initialEpoch        preparedSchemaStamp
}
type hostPrepareFlight struct {
	started  uint64
	waiters  atomic.Int32
	done     chan struct{}
	previous *hostPrepared
	epoch    preparedSchemaStamp
	result   *hostPrepared
	err      error
}
type Prepared[P, R any] struct {
	state  *preparedState
	params ParamPlan[P]
	rows   RowDecoder[R]
	plans  *resultPlanCache[R]
}
type Bound[R any] struct {
	request executionRequest
	rows    RowDecoder[R]
	plans   *resultPlanCache[R]
}

func Prepare[P, R any](ctx context.Context, s *Session, statement Statement[P, R], options PrepareOptions) (*Prepared[P, R], error) {
	if ctx == nil || s == nil {
		return nil, typeValidation("prepare", "nil context or session")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStatement(statement); err != nil {
		return nil, err
	}
	keyspace := options.Keyspace
	if keyspace == "" {
		keyspace = s.cfg.Keyspace
	}
	state, err := s.prepareTyped(ctx, statement.CQL, keyspace, options)
	if err != nil {
		return nil, err
	}
	state.mu.RLock()
	parameters, columns := state.parameters, state.columns
	state.mu.RUnlock()
	plan, err := compileParamPlan(statement.Params, parameters)
	if err != nil {
		return nil, err
	}
	rows, err := compileRowPlan(statement.Rows, columns)
	if err != nil {
		return nil, err
	}
	return &Prepared[P, R]{state: state, params: plan, rows: statement.Rows, plans: &resultPlanCache[R]{columns: columns, plan: rows}}, nil
}
func validateStatement[P, R any](s Statement[P, R]) error {
	if strings.TrimSpace(s.CQL) == "" {
		return typeValidation("statement", "empty CQL")
	}
	if firstCQLWord(s.CQL) == "USE" {
		return ErrUseStmt
	}
	if s.Params == nil || s.Rows == nil {
		return typeValidation("statement", "parameter and row codecs are required")
	}
	return nil
}
func (p *Prepared[P, R]) Parameters() Columns {
	if p == nil || p.state == nil {
		return Columns{}
	}
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()
	return p.state.parameters
}
func (p *Prepared[P, R]) Columns() Columns {
	if p == nil || p.state == nil {
		return Columns{}
	}
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()
	return p.state.columns
}
func (p *Prepared[P, R]) Info() PreparedInfo {
	if p == nil || p.state == nil {
		return PreparedInfo{}
	}
	p.state.mu.RLock()
	defer p.state.mu.RUnlock()
	v := p.state.info
	v.PartitionKeyIndexes = append([]int(nil), v.PartitionKeyIndexes...)
	return v
}
func BindBorrowed[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return bindPrepared(p, value, false)
}
func BindOwned[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return bindPrepared(p, value, true)
}
func bindPrepared[P, R any](p *Prepared[P, R], value P, owned bool) (*Bound[R], error) {
	if p == nil || p.state == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	p.state.mu.RLock()
	columns := p.state.parameters
	info := p.state.info
	statement, keyspace := p.state.statement, p.state.keyspace
	p.state.mu.RUnlock()
	allowUnset := p.state.protocol >= 4
	values, err := encodeParameters(p.params, columns, value, owned, allowUnset)
	if err != nil {
		return nil, err
	}
	token, err := computeRoutingToken(values.cells, info.PartitionKeyIndexes, p.state.session.routingPartitioner(info.Keyspace, info.Table))
	if err != nil {
		return nil, err
	}
	return &Bound[R]{request: executionRequest{statement: statement, keyspace: keyspace, prepared: p.state, values: values, owner: newInputOwner(), routing: RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Token: token, Conditional: info.Conditional}}, rows: p.rows, plans: p.plans}, nil
}
func BindUnpreparedBorrowed[P, R any](s Statement[P, R], columns Columns, value P) (*Bound[R], error) {
	return bindUnprepared(s, columns, value, false)
}
func BindUnpreparedOwned[P, R any](s Statement[P, R], columns Columns, value P) (*Bound[R], error) {
	return bindUnprepared(s, columns, value, true)
}
func bindUnprepared[P, R any](s Statement[P, R], columns Columns, value P, owned bool) (*Bound[R], error) {
	if err := validateStatement(s); err != nil {
		return nil, err
	}
	plan, err := compileParamPlan(s.Params, columns)
	if err != nil {
		return nil, err
	}
	values, err := encodeParameters(plan, columns, value, owned, true)
	if err != nil {
		return nil, err
	}
	return &Bound[R]{request: executionRequest{statement: s.CQL, values: values, owner: newInputOwner(), routing: RoutingInfo{Conditional: conditionalCQL(s.CQL)}}, rows: s.Rows, plans: &resultPlanCache[R]{}}, nil
}
func encodeParameters[P any](plan ParamPlan[P], columns Columns, value P, owned, allowUnset bool) (encodedValues, error) {
	if columns.Len() > 65535 {
		return encodedValues{}, typeValidation("parameters", "maximum count is 65535")
	}
	b := wire.NewBuilder(4096)
	writer := newParamWriter(b, columns, allowUnset)
	writer.ownBorrowed = owned
	if err := plan.Encode(writer, value); err != nil {
		return encodedValues{}, err
	}
	if err := finishParamWriter(writer); err != nil {
		return encodedValues{}, err
	}
	framed, err := b.Freeze()
	if err != nil {
		return encodedValues{}, err
	}
	cursor := wire.NewCursor(framed)
	cells := make([]encodedCell, columns.Len())
	for i := range cells {
		column, _ := columns.At(i)
		v, err := readValue(cursor, column.Type, nil, allowUnset)
		if err != nil {
			return encodedValues{}, err
		}
		cells[i] = encodedCell{typ: column.Type, state: v.State(), body: v.Bytes().chain}
	}
	if err := cursor.RequireEnd(); err != nil {
		return encodedValues{}, fmt.Errorf("gocql: parameter framing: %w", err)
	}
	return encodedValues{columns: columns, cells: cells, framed: framed}, nil
}
func (b *Bound[R]) Close() {
	if b != nil && b.request.owner != nil {
		b.request.owner.close()
	}
}
func (b *Bound[R]) Wait(ctx context.Context) error {
	if b == nil || b.request.owner == nil {
		return typeValidation("binding", "nil binding")
	}
	if ctx == nil {
		return typeValidation("context", "nil context")
	}
	return b.request.owner.wait(ctx)
}
func (b *Bound[R]) Routing() RoutingInfo {
	if b == nil {
		return RoutingInfo{}
	}
	return b.request.routing
}

func firstCQLWord(cql string) string {
	word := ""
	visitCQLWords(cql, func(next string) bool { word = next; return false })
	return word
}
func conditionalCQL(cql string) bool {
	conditional := false
	visitCQLWords(cql, func(word string) bool {
		if word == "IF" {
			conditional = true
			return false
		}
		return true
	})
	return conditional
}
func visitCQLWords(cql string, visit func(string) bool) {
	for i := 0; i < len(cql); {
		if cql[i] == '\'' || cql[i] == '"' {
			quote := cql[i]
			i++
			for i < len(cql) {
				if cql[i] == quote {
					i++
					if i < len(cql) && cql[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}
		if i+1 < len(cql) && cql[i] == '-' && cql[i+1] == '-' {
			i += 2
			for i < len(cql) && cql[i] != '\n' {
				i++
			}
			continue
		}
		if i+1 < len(cql) && cql[i] == '/' && cql[i+1] == '*' {
			i += 2
			for i+1 < len(cql) && (cql[i] != '*' || cql[i+1] != '/') {
				i++
			}
			i += 2
			continue
		}
		if cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] == '_' {
			start := i
			i++
			for i < len(cql) && (cql[i] >= 'a' && cql[i] <= 'z' || cql[i] >= 'A' && cql[i] <= 'Z' || cql[i] >= '0' && cql[i] <= '9' || cql[i] == '_') {
				i++
			}
			if !visit(strings.ToUpper(cql[start:i])) {
				return
			}
			continue
		}
		i++
	}
}
