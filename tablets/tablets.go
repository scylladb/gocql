package tablets

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type ReplicaInfo struct {
	// hostId for sake of better performance, it has to be same type as HostInfo.hostId
	hostId  string
	shardId int
}

func (r ReplicaInfo) HostID() string {
	return r.hostId
}

func (r ReplicaInfo) ShardID() int {
	return r.shardId
}

func (r ReplicaInfo) String() string {
	return fmt.Sprintf("ReplicaInfo{hostId:%s, shardId:%d}", r.hostId, r.shardId)
}

type TabletInfoBuilder struct {
	KeyspaceName string
	TableName    string
	Replicas     [][]interface{}
	FirstToken   int64
	LastToken    int64
}

func NewTabletInfoBuilder() TabletInfoBuilder {
	return TabletInfoBuilder{}
}

type toString interface {
	String() string
}

func (b TabletInfoBuilder) Build() (*TabletInfo, error) {
	tabletReplicas := make([]ReplicaInfo, 0, len(b.Replicas))
	for _, replica := range b.Replicas {
		if len(replica) != 2 {
			return nil, fmt.Errorf("replica info should have exactly two elements, but it has %d: %v", len(replica), replica)
		}
		if hostId, ok := replica[0].(toString); ok {
			if shardId, ok := replica[1].(int); ok {
				repInfo := ReplicaInfo{hostId.String(), shardId}
				tabletReplicas = append(tabletReplicas, repInfo)
			} else {
				return nil, fmt.Errorf("second element (shard) of replica is not int: %v", replica)
			}
		} else {
			return nil, fmt.Errorf("first element (hostID) of replica is not UUID: %v", replica)
		}
	}

	return &TabletInfo{
		keyspaceName: b.KeyspaceName,
		tableName:    b.TableName,
		firstToken:   b.FirstToken,
		lastToken:    b.LastToken,
		replicas:     tabletReplicas,
	}, nil
}

type TabletInfo struct {
	keyspaceName string
	tableName    string
	replicas     []ReplicaInfo
	firstToken   int64
	lastToken    int64
}

func (t *TabletInfo) KeyspaceName() string {
	return t.keyspaceName
}

func (t *TabletInfo) FirstToken() int64 {
	return t.firstToken
}

func (t *TabletInfo) LastToken() int64 {
	return t.lastToken
}

func (t *TabletInfo) TableName() string {
	return t.tableName
}

func (t *TabletInfo) Replicas() []ReplicaInfo {
	return t.replicas
}

type TabletInfoList []*TabletInfo

// FindTablets returns the range [l, r] of indices within the TabletInfoList
// that correspond to consecutive tablets matching the given keyspace and table.
//
// If no matching tablets are found, both l and r are set to -1.
// The search stops at the first non-matching tablet after finding the first match.
//
// Parameters:
//
//	keyspace - the name of the keyspace to match.
//	table    - the name of the table to match.
//
// Returns:
//
//	l - the index of the first matching tablet.
//	r - the index of the last matching tablet in the contiguous block.
func (t TabletInfoList) FindTablets(keyspace string, table string) (int, int) {
	l := -1
	r := -1
	for i, tablet := range t {
		if tablet.KeyspaceName() == keyspace && tablet.TableName() == table {
			if l == -1 {
				l = i
			}
			r = i
		} else if l != -1 {
			break
		}
	}

	return l, r
}

// AddTabletToTabletsList inserts a new tablet into the TabletInfoList while preserving sorted order
// and removing any existing overlapping tablets for the same keyspace and table.
//
// It first locates the range of tablets corresponding to the same keyspace and table,
// then determines the overlapping region (if any) based on token ranges.
// Any overlapping tablets in that range are removed, and the new tablet is inserted
// at the appropriate position.
//
// Parameters:
//
//	tablet - pointer to the TabletInfo to be added.
//
// Returns:
//
//	A new TabletInfoList with the given tablet inserted and any overlapping tablets removed.
func (t TabletInfoList) AddTabletToTabletsList(tablet *TabletInfo) TabletInfoList {
	l, r := t.FindTablets(tablet.keyspaceName, tablet.tableName)
	if l == -1 && r == -1 {
		l = 0
		r = 0
	} else {
		r = r + 1
	}

	l1, r1 := l, r
	l2, r2 := l1, r1

	// find first overlaping range
	for l1 < r1 {
		mid := (l1 + r1) / 2
		if t[mid].FirstToken() < tablet.FirstToken() {
			l1 = mid + 1
		} else {
			r1 = mid
		}
	}
	start := l1

	if start > l && t[start-1].LastToken() > tablet.FirstToken() {
		start = start - 1
	}

	// find last overlaping range
	for l2 < r2 {
		mid := (l2 + r2) / 2
		if t[mid].LastToken() < tablet.LastToken() {
			l2 = mid + 1
		} else {
			r2 = mid
		}
	}
	end := l2
	if end < r && t[end].FirstToken() >= tablet.LastToken() {
		end = end - 1
	}
	if end == len(t) {
		end = end - 1
	}

	updated_tablets := t
	if start <= end {
		// Delete elements from index start to end
		updated_tablets = append(t[:start], t[end+1:]...)
	}
	// Insert tablet element at index start
	t = append(updated_tablets[:start], append([]*TabletInfo{tablet}, updated_tablets[start:]...)...)
	return t
}

// BulkAddTabletsToTabletsList inserts a sorted list of tablets into the TabletInfoList,
// replacing any overlapping tablets for the same keyspace and table.
//
// The method assumes the input tablets are sorted by token range. It locates the existing
// tablet range matching the keyspace and table, finds and removes any tablets whose token
// ranges overlap with the new ones, and inserts the new tablets at the appropriate position.
//
// Parameters:
//
//	tablets - a slice of *TabletInfo to insert.
//
// Returns:
//
//	A new TabletInfoList with the given tablets inserted and any overlapping tablets removed.
func (t TabletInfoList) BulkAddTabletsToTabletsList(tablets []*TabletInfo) TabletInfoList {
	firstToken := tablets[0].FirstToken()
	lastToken := tablets[len(tablets)-1].LastToken()
	l, r := t.FindTablets(tablets[0].keyspaceName, tablets[0].tableName)
	if l == -1 && r == -1 {
		l = 0
		r = 0
	} else {
		r = r + 1
	}

	l1, r1 := l, r
	l2, r2 := l1, r1

	// find first overlaping range
	for l1 < r1 {
		mid := (l1 + r1) / 2
		if t[mid].FirstToken() < firstToken {
			l1 = mid + 1
		} else {
			r1 = mid
		}
	}
	start := l1

	if start > l && t[start-1].LastToken() > firstToken {
		start = start - 1
	}

	// find last overlaping range
	for l2 < r2 {
		mid := (l2 + r2) / 2
		if t[mid].LastToken() < lastToken {
			l2 = mid + 1
		} else {
			r2 = mid
		}
	}
	end := l2
	if end < r && t[end].FirstToken() >= lastToken {
		end = end - 1
	}
	if end == len(t) {
		end = end - 1
	}

	updated_tablets := t
	if start <= end {
		// Delete elements from index start to end
		updated_tablets = append(t[:start], t[end+1:]...)
	}
	// Insert tablet element at index start
	t = append(updated_tablets[:start], append(append([]*TabletInfo(nil), tablets...), updated_tablets[start:]...)...)
	return t
}

// RemoveTabletsWithHost returns a new TabletInfoList excluding any tablets
// that have a replica hosted on the specified host ID.
//
// It iterates through the list and filters out tablets where any replica's hostId
// matches the provided value.
//
// Parameters:
//
//	hostID - the ID of the host to filter out.
//
// Returns:
//
//	A new TabletInfoList excluding tablets with replicas on the specified host.
func (t TabletInfoList) RemoveTabletsWithHost(hostID string) TabletInfoList {
	filteredTablets := make([]*TabletInfo, 0, len(t)) // Preallocate for efficiency

	for _, tablet := range t {
		// Check if any replica matches the given host ID
		shouldExclude := false
		for _, replica := range tablet.replicas {
			if replica.hostId == hostID {
				shouldExclude = true
				break
			}
		}
		if !shouldExclude {
			filteredTablets = append(filteredTablets, tablet)
		}
	}

	t = filteredTablets
	return t
}

// RemoveTabletsWithKeyspace returns a new TabletInfoList excluding all tablets
// that belong to the specified keyspace.
//
// It filters out any tablet whose keyspace name matches the given keyspace.
//
// Parameters:
//
//	keyspace - the name of the keyspace to remove.
//
// Returns:
//
//	A new TabletInfoList without tablets from the specified keyspace.
func (t TabletInfoList) RemoveTabletsWithKeyspace(keyspace string) TabletInfoList {
	filteredTablets := make([]*TabletInfo, 0, len(t))

	for _, tablet := range t {
		if tablet.keyspaceName != keyspace {
			filteredTablets = append(filteredTablets, tablet)
		}
	}

	t = filteredTablets
	return t
}

// RemoveTabletsWithTableFromTabletsList returns a new TabletInfoList excluding all tablets
// that belong to the specified keyspace and table.
//
// It filters out any tablet whose keyspace and table name both match the provided values.
//
// Parameters:
//
//	keyspace - the name of the keyspace to remove.
//	table    - the name of the table to remove.
//
// Returns:
//
//	A new TabletInfoList without tablets from the specified keyspace and table.
func (t TabletInfoList) RemoveTabletsWithTableFromTabletsList(keyspace string, table string) TabletInfoList {
	filteredTablets := make([]*TabletInfo, 0, len(t))

	for _, tablet := range t {
		if !(tablet.keyspaceName == keyspace && tablet.tableName == table) {
			filteredTablets = append(filteredTablets, tablet)
		}
	}

	t = filteredTablets
	return t
}

// FindTabletForToken performs a binary search within the specified range [l, r)
// of the TabletInfoList to find the tablet that owns the given token.
//
// It assumes the tablets are sorted by token range and returns the first tablet
// whose LastToken is greater than or equal to the given token.
//
// Parameters:
//
//	token - the token to search for.
//	l     - the start index of the search range (inclusive).
//	r     - the end index of the search range (exclusive).
//
// Returns:
//
//	A pointer to the TabletInfo that owns the token.
func (t TabletInfoList) FindTabletForToken(token int64, l int, r int) *TabletInfo {
	for l < r {
		var m int
		if r*l > 0 {
			m = l + (r-l)/2
		} else {
			m = (r + l) / 2
		}
		if t[m].LastToken() < token {
			l = m + 1
		} else {
			r = m
		}
	}

	return t[l]
}

// addTabletToPerTableList inserts a tablet into a per-table TabletInfoList.
// Unlike AddTabletToTabletsList, it assumes all entries belong to the same table,
// so it skips the O(n) FindTablets scan.
// It builds a new slice without mutating the input, safe for concurrent readers.
func (t TabletInfoList) addTabletToPerTableList(tablet *TabletInfo) TabletInfoList {
	l := 0
	r := len(t)

	l1, r1 := l, r
	l2, r2 := l1, r1

	// find first overlapping range
	for l1 < r1 {
		mid := (l1 + r1) / 2
		if t[mid].FirstToken() < tablet.FirstToken() {
			l1 = mid + 1
		} else {
			r1 = mid
		}
	}
	start := l1

	if start > l && t[start-1].LastToken() > tablet.FirstToken() {
		start = start - 1
	}

	// find last overlapping range
	for l2 < r2 {
		mid := (l2 + r2) / 2
		if t[mid].LastToken() < tablet.LastToken() {
			l2 = mid + 1
		} else {
			r2 = mid
		}
	}
	end := l2
	if end < r && t[end].FirstToken() >= tablet.LastToken() {
		end = end - 1
	}
	if end == len(t) {
		end = end - 1
	}

	// Build new slice without mutating input (concurrent readers may reference it).
	var tailStart int
	if start <= end {
		tailStart = end + 1
	} else {
		tailStart = start
	}
	result := make(TabletInfoList, 0, start+1+(len(t)-tailStart))
	result = append(result, t[:start]...)
	result = append(result, tablet)
	result = append(result, t[tailStart:]...)
	return result
}

// bulkAddToPerTableList inserts a sorted batch of tablets into a per-table TabletInfoList.
// Unlike BulkAddTabletsToTabletsList, it assumes all entries belong to the same table,
// so it skips the O(n) FindTablets scan.
// It builds a new slice without mutating the input, safe for concurrent readers.
func (t TabletInfoList) bulkAddToPerTableList(newTablets []*TabletInfo) TabletInfoList {
	firstToken := newTablets[0].FirstToken()
	lastToken := newTablets[len(newTablets)-1].LastToken()

	l := 0
	r := len(t)

	l1, r1 := l, r
	l2, r2 := l1, r1

	// find first overlapping range
	for l1 < r1 {
		mid := (l1 + r1) / 2
		if t[mid].FirstToken() < firstToken {
			l1 = mid + 1
		} else {
			r1 = mid
		}
	}
	start := l1

	if start > l && t[start-1].LastToken() > firstToken {
		start = start - 1
	}

	// find last overlapping range
	for l2 < r2 {
		mid := (l2 + r2) / 2
		if t[mid].LastToken() < lastToken {
			l2 = mid + 1
		} else {
			r2 = mid
		}
	}
	end := l2
	if end < r && t[end].FirstToken() >= lastToken {
		end = end - 1
	}
	if end == len(t) {
		end = end - 1
	}

	// Build new slice without mutating input (concurrent readers may reference it).
	var tailStart int
	if start <= end {
		tailStart = end + 1
	} else {
		tailStart = start
	}
	result := make(TabletInfoList, 0, start+len(newTablets)+(len(t)-tailStart))
	result = append(result, t[:start]...)
	result = append(result, newTablets...)
	result = append(result, t[tailStart:]...)
	return result
}

// tableKey identifies a specific table within a keyspace.
type tableKey struct {
	keyspace string
	table    string
}

// tableTablets holds a per-table sorted tablet list with copy-on-write semantics.
// Reads are lock-free via atomic.Value; writes happen only on the writer goroutine.
type tableTablets struct {
	list atomic.Value // stores TabletInfoList for this table
}

func newTableTablets() *tableTablets {
	tt := &tableTablets{}
	tt.list.Store(make(TabletInfoList, 0))
	return tt
}

func (tt *tableTablets) get() TabletInfoList {
	return tt.list.Load().(TabletInfoList)
}

// tabletOpKind identifies the type of write operation.
type tabletOpKind int

const (
	opAddTablet tabletOpKind = iota
	opBulkAddTablets
	opRemoveHost
	opRemoveKeyspace
	opRemoveTable
	opFlush
)

// tabletOp represents a write operation to be processed by the writer goroutine.
type tabletOp struct {
	tablet   *TabletInfo
	done     chan struct{}
	hostID   string
	keyspace string
	table    string
	tablets  []*TabletInfo
	kind     tabletOpKind
}

// CowTabletList stores tablets partitioned by keyspace/table for O(1) table lookup.
// All writes are serialized through a single writer goroutine.
// Reads are lock-free on per-table lists (atomic.Value) and use RLock for map access.
type CowTabletList struct {
	tables  map[tableKey]*tableTablets
	ops     chan tabletOp
	quit    chan struct{}
	stopped chan struct{}
	mu      sync.RWMutex
}

// NewCowTabletList creates a new CowTabletList and starts its writer goroutine.
// The caller must call Close() when done to stop the writer goroutine.
func NewCowTabletList() *CowTabletList {
	c := &CowTabletList{
		tables:  make(map[tableKey]*tableTablets),
		ops:     make(chan tabletOp, 4096),
		quit:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go c.writeLoop()
	return c
}

// Close stops the writer goroutine after draining all pending operations.
func (c *CowTabletList) Close() {
	close(c.quit)
	<-c.stopped
}

// Flush blocks until all previously submitted write operations have been processed.
func (c *CowTabletList) Flush() {
	done := make(chan struct{})
	select {
	case c.ops <- tabletOp{kind: opFlush, done: done}:
		<-done
	case <-c.quit:
	}
}

// writeLoop is the single writer goroutine that processes all write operations.
func (c *CowTabletList) writeLoop() {
	defer close(c.stopped)
	for {
		select {
		case op := <-c.ops:
			c.processOp(op)
		case <-c.quit:
			// Drain remaining ops before exiting.
			for {
				select {
				case op := <-c.ops:
					c.processOp(op)
				default:
					return
				}
			}
		}
	}
}

// processOp executes a single write operation. Only called from writeLoop.
func (c *CowTabletList) processOp(op tabletOp) {
	switch op.kind {
	case opAddTablet:
		c.doAddTablet(op.tablet)
	case opBulkAddTablets:
		c.doBulkAddTablets(op.tablets)
	case opRemoveHost:
		c.doRemoveTabletsWithHost(op.hostID)
	case opRemoveKeyspace:
		c.doRemoveTabletsWithKeyspace(op.keyspace)
	case opRemoveTable:
		c.doRemoveTabletsWithTable(op.keyspace, op.table)
	case opFlush:
		close(op.done)
	}
}

// sendOp sends an operation to the writer goroutine. No-op if already shut down.
func (c *CowTabletList) sendOp(op tabletOp) {
	select {
	case c.ops <- op:
	case <-c.quit:
	}
}

// getOrCreateTable returns the tableTablets for the given key, creating it if needed.
// Only called from the writer goroutine.
func (c *CowTabletList) getOrCreateTable(key tableKey) *tableTablets {
	tt := c.tables[key]
	if tt != nil {
		return tt
	}
	tt = newTableTablets()
	c.mu.Lock()
	c.tables[key] = tt
	c.mu.Unlock()
	return tt
}

func (c *CowTabletList) doAddTablet(tablet *TabletInfo) {
	key := tableKey{tablet.keyspaceName, tablet.tableName}
	tt := c.getOrCreateTable(key)
	tt.list.Store(tt.get().addTabletToPerTableList(tablet))
}

func (c *CowTabletList) doBulkAddTablets(tablets []*TabletInfo) {
	groups := make(map[tableKey][]*TabletInfo)
	for _, t := range tablets {
		key := tableKey{t.keyspaceName, t.tableName}
		groups[key] = append(groups[key], t)
	}
	for key, group := range groups {
		tt := c.getOrCreateTable(key)
		tt.list.Store(tt.get().bulkAddToPerTableList(group))
	}
}

func (c *CowTabletList) doRemoveTabletsWithHost(hostID string) {
	c.mu.RLock()
	keys := make([]tableKey, 0, len(c.tables))
	for k := range c.tables {
		keys = append(keys, k)
	}
	c.mu.RUnlock()

	for _, key := range keys {
		tt := c.tables[key]
		if tt == nil {
			continue
		}
		tt.list.Store(tt.get().RemoveTabletsWithHost(hostID))
	}
}

func (c *CowTabletList) doRemoveTabletsWithKeyspace(keyspace string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.tables {
		if key.keyspace == keyspace {
			delete(c.tables, key)
		}
	}
}

func (c *CowTabletList) doRemoveTabletsWithTable(keyspace, table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tables, tableKey{keyspace, table})
}

// --- Public read methods (lock-free on per-table data) ---

// getTable returns the tableTablets for the given key, or nil if not found.
func (c *CowTabletList) getTable(key tableKey) *tableTablets {
	c.mu.RLock()
	tt := c.tables[key]
	c.mu.RUnlock()
	return tt
}

// Get returns a flat TabletInfoList containing all tablets across all tables.
// It is safe for concurrent use.
func (c *CowTabletList) Get() TabletInfoList {
	c.mu.RLock()
	total := 0
	for _, tt := range c.tables {
		total += len(tt.get())
	}
	result := make(TabletInfoList, 0, total)
	for _, tt := range c.tables {
		result = append(result, tt.get()...)
	}
	c.mu.RUnlock()
	return result
}

// FindReplicasForToken returns the replica set responsible for the given token,
// within the specified keyspace and table.
func (c *CowTabletList) FindReplicasForToken(keyspace, table string, token int64) []ReplicaInfo {
	tl := c.FindTabletForToken(keyspace, table, token)
	if tl == nil {
		return nil
	}
	return tl.Replicas()
}

// FindTabletForToken locates the tablet that covers the given token
// for the specified keyspace and table. Returns nil if not found.
func (c *CowTabletList) FindTabletForToken(keyspace, table string, token int64) *TabletInfo {
	tt := c.getTable(tableKey{keyspace, table})
	if tt == nil {
		return nil
	}
	tablets := tt.get()
	if len(tablets) == 0 {
		return nil
	}
	return tablets.FindTabletForToken(token, 0, len(tablets)-1)
}

// --- Public write methods (send to writer goroutine) ---

// AddTablet queues a single tablet addition. The write is processed asynchronously
// by the writer goroutine. Use Flush() to wait for completion.
func (c *CowTabletList) AddTablet(tablet *TabletInfo) {
	c.sendOp(tabletOp{kind: opAddTablet, tablet: tablet})
}

// BulkAddTablets queues a batch tablet addition. The write is processed asynchronously.
func (c *CowTabletList) BulkAddTablets(tablets []*TabletInfo) {
	c.sendOp(tabletOp{kind: opBulkAddTablets, tablets: tablets})
}

// RemoveTabletsWithHost queues removal of all tablets with replicas on the specified host.
func (c *CowTabletList) RemoveTabletsWithHost(hostID string) {
	c.sendOp(tabletOp{kind: opRemoveHost, hostID: hostID})
}

// RemoveTabletsWithKeyspace queues removal of all tablets for the given keyspace.
func (c *CowTabletList) RemoveTabletsWithKeyspace(keyspace string) {
	c.sendOp(tabletOp{kind: opRemoveKeyspace, keyspace: keyspace})
}

// RemoveTabletsWithTableFromTabletsList queues removal of all tablets for the specified table.
func (c *CowTabletList) RemoveTabletsWithTableFromTabletsList(keyspace string, table string) {
	c.sendOp(tabletOp{kind: opRemoveTable, keyspace: keyspace, table: table})
}
