package tablets

import (
	"fmt"
	"slices"
	"sort"
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
	if b.FirstToken > b.LastToken {
		return nil, fmt.Errorf("invalid token range: firstToken (%d) > lastToken (%d)",
			b.FirstToken, b.LastToken)
	}

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

// TabletInfo represents a tablet with its token range and replica set.
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
	result := make([]ReplicaInfo, len(t.replicas))
	copy(result, t.replicas)
	return result
}

// ReplicasUnsafe returns the raw replica slice without copying.
func (t *TabletInfo) ReplicasUnsafe() []ReplicaInfo {
	return t.replicas
}

type TabletInfoList []*TabletInfo

// TabletEntry is the per-table representation of a tablet, without keyspace/table names.
type TabletEntry struct {
	replicas   []ReplicaInfo
	firstToken int64
	lastToken  int64
}

type TabletEntryList []*TabletEntry

// Replicas returns a copy of the replica list for this entry.
func (e *TabletEntry) Replicas() []ReplicaInfo {
	result := make([]ReplicaInfo, len(e.replicas))
	copy(result, e.replicas)
	return result
}

// ReplicasUnsafe returns the raw replica slice without copying.
func (e *TabletEntry) ReplicasUnsafe() []ReplicaInfo {
	return e.replicas
}

func (e *TabletEntry) FirstToken() int64 {
	return e.firstToken
}

func (e *TabletEntry) LastToken() int64 {
	return e.lastToken
}

// findOverlapRange returns the start and tailStart indices for entries
// overlapping with the token range [firstToken, lastToken].
// start is the first overlapping entry; tailStart is the first entry
// after the overlap region.
func (t TabletEntryList) findOverlapRange(firstToken, lastToken int64) (start, tailStart int) {
	if len(t) == 0 {
		return 0, 0
	}

	l := 0
	r := len(t)

	l1, r1 := l, r
	l2, r2 := l1, r1

	for l1 < r1 {
		mid := l1 + (r1-l1)/2
		if t[mid].firstToken < firstToken {
			l1 = mid + 1
		} else {
			r1 = mid
		}
	}
	start = l1

	// Adjust start backward if the previous entry overlaps.
	if start > l && t[start-1].lastToken > firstToken {
		start = start - 1
	}

	for l2 < r2 {
		mid := l2 + (r2-l2)/2
		if t[mid].lastToken < lastToken {
			l2 = mid + 1
		} else {
			r2 = mid
		}
	}
	end := l2
	if end < len(t) && t[end].firstToken > lastToken {
		end = end - 1
	}
	if end >= len(t) {
		end = len(t) - 1
	}

	if start <= end && end >= 0 {
		tailStart = end + 1
	} else {
		tailStart = start
	}
	return start, tailStart
}

// addEntry inserts a single entry into the sorted list, replacing any overlapping ranges.
// Returns a new slice without mutating the original.
func (t TabletEntryList) addEntry(e *TabletEntry) TabletEntryList {
	start, tailStart := t.findOverlapRange(e.firstToken, e.lastToken)
	result := make(TabletEntryList, 0, start+1+(len(t)-tailStart))
	result = append(result, t[:start]...)
	result = append(result, e)
	result = append(result, t[tailStart:]...)
	return result
}

// bulkAddEntries inserts a sorted batch of entries, replacing any overlapping ranges.
// Returns a new slice without mutating the original.
// The entries must be sorted by firstToken, then lastToken. Entries may have
// gaps between them or overlap each other within the batch; existing entries
// that fall in gaps between batch entries are preserved. Intra-batch overlaps
// are resolved by letting later entries replace earlier ones.
func (t TabletEntryList) bulkAddEntries(entries []*TabletEntry) TabletEntryList {
	if len(entries) == 0 {
		return t
	}

	// Resolve intra-batch overlaps: later entries replace earlier ones.
	deduped := make([]*TabletEntry, 0, len(entries))
	for _, e := range entries {
		// Drop any previously added entries that the current one overlaps.
		for len(deduped) > 0 && deduped[len(deduped)-1].firstToken >= e.firstToken {
			deduped = deduped[:len(deduped)-1]
		}
		// If the last kept entry partially overlaps, drop it too.
		if len(deduped) > 0 && deduped[len(deduped)-1].lastToken > e.firstToken {
			deduped = deduped[:len(deduped)-1]
		}
		deduped = append(deduped, e)
	}
	entries = deduped

	// Merge the existing list (t) with the batch (entries).
	// Both are sorted by firstToken. For each batch entry we remove any
	// overlapping existing entries; existing entries that sit in gaps
	// between batch entries are preserved.
	result := make(TabletEntryList, 0, len(t)+len(entries))
	ti := 0 // index into t (existing list)

	for _, e := range entries {
		// Copy existing entries that come entirely before this batch entry.
		for ti < len(t) && t[ti].lastToken <= e.firstToken && t[ti].firstToken < e.firstToken {
			result = append(result, t[ti])
			ti++
		}
		// Skip existing entries that overlap with this batch entry.
		for ti < len(t) && t[ti].firstToken < e.lastToken && t[ti].lastToken > e.firstToken {
			ti++
		}
		result = append(result, e)
	}
	// Append remaining existing entries that come after all batch entries.
	result = append(result, t[ti:]...)
	return result
}

// findEntryForToken performs a binary search within [l, r) to find the entry
// covering the given token. Returns nil if no such entry exists.
func (t TabletEntryList) findEntryForToken(token int64, l int, r int) *TabletEntry {
	if l < 0 || r > len(t) || l > r {
		return nil
	}
	if l == r {
		return nil
	}

	for l < r {
		m := l + (r-l)/2
		if t[m].lastToken < token {
			l = m + 1
		} else {
			r = m
		}
	}
	if l >= len(t) {
		return nil
	}
	if t[l].firstToken > token {
		return nil
	}
	return t[l]
}

// removeEntriesWithHost returns a new list excluding entries with a replica on the given host.
func (t TabletEntryList) removeEntriesWithHost(hostID string) TabletEntryList {
	hasMatch := false
	for _, e := range t {
		for _, r := range e.replicas {
			if r.hostId == hostID {
				hasMatch = true
				break
			}
		}
		if hasMatch {
			break
		}
	}
	if !hasMatch {
		return t
	}

	result := make(TabletEntryList, 0, len(t))
	for _, e := range t {
		exclude := false
		for _, r := range e.replicas {
			if r.hostId == hostID {
				exclude = true
				break
			}
		}
		if !exclude {
			result = append(result, e)
		}
	}
	return result
}

// toEntry converts a TabletInfo to a TabletEntry.
func (t *TabletInfo) toEntry() *TabletEntry {
	return &TabletEntry{
		replicas:   t.replicas,
		firstToken: t.firstToken,
		lastToken:  t.lastToken,
	}
}

// toTabletInfo converts a TabletEntry back to a TabletInfo.
func (e *TabletEntry) toTabletInfo(keyspace, table string) *TabletInfo {
	return &TabletInfo{
		keyspaceName: keyspace,
		tableName:    table,
		replicas:     slices.Clone(e.replicas),
		firstToken:   e.firstToken,
		lastToken:    e.lastToken,
	}
}

// tableKey identifies a specific table within a keyspace.
type tableKey struct {
	keyspace string
	table    string
}

// tableTablets holds a per-table sorted tablet list with copy-on-write semantics.
type tableTablets struct {
	list atomic.Pointer[TabletEntryList] // stores TabletEntryList for this table
}

func newTableTablets() *tableTablets {
	tt := &tableTablets{}
	empty := make(TabletEntryList, 0)
	tt.list.Store(&empty)
	return tt
}

func (tt *tableTablets) store(list TabletEntryList) {
	tt.list.Store(&list)
}

// tabletOp is an operation processed by the writer goroutine.
type tabletOp interface {
	execute(c *CowTabletList)
}

type opAddTablet struct {
	tablet *TabletInfo
}

func (op opAddTablet) execute(c *CowTabletList) { c.doAddTablet(op.tablet) }

type opBulkAddTablets struct {
	tablets []*TabletInfo
}

func (op opBulkAddTablets) execute(c *CowTabletList) { c.doBulkAddTablets(op.tablets) }

type opRemoveHost struct {
	hostID string
}

func (op opRemoveHost) execute(c *CowTabletList) { c.doRemoveTabletsWithHost(op.hostID) }

type opRemoveKeyspace struct {
	keyspace string
}

func (op opRemoveKeyspace) execute(c *CowTabletList) { c.doRemoveTabletsWithKeyspace(op.keyspace) }

type opRemoveTable struct {
	keyspace string
	table    string
}

func (op opRemoveTable) execute(c *CowTabletList) { c.doRemoveTabletsWithTable(op.keyspace, op.table) }

type opFlush struct {
	done chan struct{}
}

func (op opFlush) execute(_ *CowTabletList) { close(op.done) }

// opQueueBufferSize is the capacity of the writer goroutine's operation queue.
const opQueueBufferSize = 4096

// opQueue manages a single writer goroutine with safe send/close/flush coordination.
type opQueue struct {
	cachedItem tabletOp
	ops        chan tabletOp
	quit       chan struct{}
	stopped    chan struct{}
	waiters    *sync.Cond
	senders    int
	closeOnce  sync.Once
	lifecycle  sync.Mutex
	closed     bool
}

func newOpQueue() *opQueue {
	q := &opQueue{
		ops:     make(chan tabletOp, opQueueBufferSize),
		quit:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	q.waiters = sync.NewCond(&q.lifecycle)
	return q
}

func (q *opQueue) next() tabletOp {
	if q.cachedItem != nil {
		item := q.cachedItem
		q.cachedItem = nil
		return item
	}
	var op tabletOp
	select {
	case <-q.quit:
		{
			return nil
		}
	case op = <-q.ops:
		opAdd, ok := op.(opAddTablet)
		if !ok {
			return op
		}
		bulkOp := opBulkAddTablets{
			tablets: []*TabletInfo{
				opAdd.tablet,
			},
		}
		for {
			select {
			case op = <-q.ops:
				opAdd, ok = op.(opAddTablet)
				if !ok {
					q.cachedItem = op
					return bulkOp
				}
				bulkOp.tablets = append(bulkOp.tablets, opAdd.tablet)
			default:
				return bulkOp
			}
		}
	}
}

// run is the writer goroutine loop.
func (q *opQueue) run(process func(tabletOp)) {
	defer close(q.stopped)
	for {
		op := q.next()
		if op == nil {
			return
		}
		process(op)
	}
}

// close stops the writer goroutine after draining in-flight senders.
func (q *opQueue) close() {
	q.closeOnce.Do(func() {
		q.lifecycle.Lock()
		q.closed = true
		for q.senders > 0 {
			q.waiters.Wait()
		}
		close(q.quit)
		q.lifecycle.Unlock()
	})
	<-q.stopped
}

// flush blocks until all previously submitted operations have been processed.
func (q *opQueue) flush() {
	done := make(chan struct{})
	if !q.beginSend() {
		return
	}
	defer q.endSend()
	sent := false
	select {
	case q.ops <- opFlush{done: done}:
		sent = true
	case <-q.quit:
	}
	if !sent {
		return
	}

	select {
	case <-done:
	case <-q.stopped:
	}
}

// send enqueues an operation.
func (q *opQueue) send(op tabletOp) {
	if !q.beginSend() {
		return
	}
	defer q.endSend()
	select {
	case q.ops <- op:
	case <-q.quit:
	}
}

func (q *opQueue) beginSend() bool {
	q.lifecycle.Lock()
	defer q.lifecycle.Unlock()
	if q.closed {
		return false
	}
	q.senders++
	return true
}

func (q *opQueue) endSend() {
	q.lifecycle.Lock()
	q.senders--
	if q.senders == 0 {
		q.waiters.Broadcast()
	}
	q.lifecycle.Unlock()
}

// tableMap is the type stored inside the atomic pointer.
type tableMap = map[tableKey]*tableTablets

// CowTabletList stores tablets partitioned by keyspace/table.
// All writes are serialized through a single writer goroutine; reads are lock-free.
// Write operations are asynchronous; use Flush() for read-your-writes consistency.
type CowTabletList struct {
	tables atomic.Pointer[tableMap]
	queue  *opQueue
}

// NewCowTabletList creates a new CowTabletList and starts its writer goroutine.
// The caller must call Close() when done to stop the writer goroutine.
func NewCowTabletList() *CowTabletList {
	c := &CowTabletList{
		queue: newOpQueue(),
	}
	empty := make(tableMap)
	c.tables.Store(&empty)
	go c.queue.run(func(op tabletOp) { op.execute(c) })
	return c
}

// Close stops the writer goroutine after draining all pending operations.
func (c *CowTabletList) Close() {
	if c == nil {
		return
	}
	c.queue.close()
}

// Flush blocks until all previously submitted write operations have been processed.
func (c *CowTabletList) Flush() {
	if c == nil {
		return
	}
	c.queue.flush()
}

// cloneTableMap returns a shallow copy of the current table map.
func (c *CowTabletList) cloneTableMap() tableMap {
	old := *c.tables.Load()
	m := make(tableMap, len(old)+1)
	for k, v := range old {
		m[k] = v
	}
	return m
}

// getOrCreateTable returns the tableTablets for the given key, creating it if needed.
func (c *CowTabletList) getOrCreateTable(key tableKey) *tableTablets {
	current := *c.tables.Load()
	tt := current[key]
	if tt != nil {
		return tt
	}
	tt = newTableTablets()
	m := c.cloneTableMap()
	m[key] = tt
	c.tables.Store(&m)
	return tt
}

func (c *CowTabletList) doAddTablet(tablet *TabletInfo) {
	if tablet == nil || tablet.keyspaceName == "" || tablet.tableName == "" {
		return
	}
	key := tableKey{tablet.keyspaceName, tablet.tableName}
	tt := c.getOrCreateTable(key)
	tt.store((*tt.list.Load()).addEntry(tablet.toEntry()))
}

func (c *CowTabletList) doBulkAddTablets(tablets []*TabletInfo) {
	if len(tablets) == 0 {
		return
	}

	groups := make(map[tableKey][]*TabletInfo)
	for _, t := range tablets {
		if t == nil {
			continue
		}
		if t.keyspaceName == "" || t.tableName == "" {
			continue
		}
		key := tableKey{t.keyspaceName, t.tableName}
		groups[key] = append(groups[key], t)
	}
	for key, group := range groups {
		sort.Slice(group, func(i, j int) bool {
			if group[i].FirstToken() != group[j].FirstToken() {
				return group[i].FirstToken() < group[j].FirstToken()
			}
			return group[i].LastToken() < group[j].LastToken()
		})
		entries := make([]*TabletEntry, len(group))
		for i, t := range group {
			entries[i] = t.toEntry()
		}
		tt := c.getOrCreateTable(key)
		tt.store((*tt.list.Load()).bulkAddEntries(entries))
	}
}

func (c *CowTabletList) doRemoveTabletsWithHost(hostID string) {
	current := *c.tables.Load()
	needsMapUpdate := false
	for _, tt := range current {
		old := tt.list.Load()
		newList := (*old).removeEntriesWithHost(hostID)
		if len(newList) != len(*old) {
			tt.store(newList)
			if len(newList) == 0 {
				needsMapUpdate = true
			}
		}
	}
	if needsMapUpdate {
		newMap := make(tableMap, len(current))
		for k, v := range current {
			if len(*v.list.Load()) > 0 {
				newMap[k] = v
			}
		}
		c.tables.Store(&newMap)
	}
}

func (c *CowTabletList) doRemoveTabletsWithKeyspace(keyspace string) {
	current := *c.tables.Load()
	hasKey := false
	for k := range current {
		if k.keyspace == keyspace {
			hasKey = true
			break
		}
	}
	if !hasKey {
		return
	}
	newMap := make(tableMap, len(current))
	for k, v := range current {
		if k.keyspace != keyspace {
			newMap[k] = v
		}
	}
	c.tables.Store(&newMap)
}

func (c *CowTabletList) doRemoveTabletsWithTable(keyspace, table string) {
	current := *c.tables.Load()
	key := tableKey{keyspace, table}
	if _, exists := current[key]; !exists {
		return
	}
	newMap := make(tableMap, len(current))
	for k, v := range current {
		if k != key {
			newMap[k] = v
		}
	}
	c.tables.Store(&newMap)
}

// --- Public read methods ---

// getTable returns the tableTablets for the given key, or nil if not found.
func (c *CowTabletList) getTable(key tableKey) *tableTablets {
	current := *c.tables.Load()
	return current[key]
}

// Get returns a flat TabletInfoList containing all tablets across all tables.
//
// Deprecated: Use [CowTabletList.GetTableTablets] for per-table lookups or
// [CowTabletList.ForEach] to iterate without aggregating into a flat list.
func (c *CowTabletList) Get() TabletInfoList {
	if c == nil {
		return nil
	}
	current := *c.tables.Load()
	type snap struct {
		key  tableKey
		list TabletEntryList
	}
	snaps := make([]snap, 0, len(current))
	total := 0
	for key, tt := range current {
		l := *tt.list.Load()
		snaps = append(snaps, snap{key, l})
		total += len(l)
	}
	result := make(TabletInfoList, 0, total)
	for _, s := range snaps {
		for _, e := range s.list {
			result = append(result, e.toTabletInfo(s.key.keyspace, s.key.table))
		}
	}
	return result
}

// GetTableTablets returns a copy of the tablet list for the specified keyspace and table.
// Returns nil if no tablets exist for the given combination.
func (c *CowTabletList) GetTableTablets(keyspace, table string) TabletEntryList {
	if c == nil {
		return nil
	}
	tt := c.getTable(tableKey{keyspace, table})
	if tt == nil {
		return nil
	}
	snapshot := *tt.list.Load()
	if len(snapshot) == 0 {
		return nil
	}
	result := make(TabletEntryList, len(snapshot))
	copy(result, snapshot)
	return result
}

// ForEach iterates over all keyspace/table pairs and their tablet entry lists,
// calling fn for each one. Iteration stops early if fn returns false.
// The returned TabletEntryList is a shallow copy; do not mutate individual entries.
func (c *CowTabletList) ForEach(fn func(keyspace, table string, entries TabletEntryList) bool) {
	if c == nil || fn == nil {
		return
	}
	current := *c.tables.Load()
	for key, tt := range current {
		snapshot := *tt.list.Load()
		if len(snapshot) == 0 {
			continue
		}
		entries := make(TabletEntryList, len(snapshot))
		copy(entries, snapshot)
		if !fn(key.keyspace, key.table, entries) {
			return
		}
	}
}

// FindReplicasForToken returns a copy of the replica set for the given token.
func (c *CowTabletList) FindReplicasForToken(keyspace, table string, token int64) []ReplicaInfo {
	tl := c.FindTabletForToken(keyspace, table, token)
	if tl == nil {
		return nil
	}
	return tl.Replicas()
}

// FindReplicasUnsafeForToken returns the replica set for the given token without copying.
func (c *CowTabletList) FindReplicasUnsafeForToken(keyspace, table string, token int64) []ReplicaInfo {
	tl := c.FindTabletForToken(keyspace, table, token)
	if tl == nil {
		return nil
	}
	return tl.ReplicasUnsafe()
}

// FindTabletForToken locates the tablet covering the given token. Returns nil if not found.
func (c *CowTabletList) FindTabletForToken(keyspace, table string, token int64) *TabletEntry {
	if c == nil {
		return nil
	}
	tt := c.getTable(tableKey{keyspace, table})
	if tt == nil {
		return nil
	}
	entries := *tt.list.Load()
	if len(entries) == 0 {
		return nil
	}
	return entries.findEntryForToken(token, 0, len(entries))
}

// --- Public write methods ---

// sendOp sends an operation to the writer goroutine.
func (c *CowTabletList) sendOp(op tabletOp) {
	if c == nil {
		return
	}
	c.queue.send(op)
}

// AddTablet queues a single tablet addition.
func (c *CowTabletList) AddTablet(tablet *TabletInfo) {
	c.sendOp(opAddTablet{tablet: tablet})
}

// BulkAddTablets queues a batch tablet addition.
func (c *CowTabletList) BulkAddTablets(tablets []*TabletInfo) {
	c.sendOp(opBulkAddTablets{tablets: tablets})
}

// RemoveTabletsWithHost queues removal of all tablets with replicas on the specified host.
func (c *CowTabletList) RemoveTabletsWithHost(hostID string) {
	c.sendOp(opRemoveHost{hostID: hostID})
}

// RemoveTabletsWithKeyspace queues removal of all tablets for the given keyspace.
func (c *CowTabletList) RemoveTabletsWithKeyspace(keyspace string) {
	c.sendOp(opRemoveKeyspace{keyspace: keyspace})
}

// RemoveTabletsWithTable queues removal of all tablets for the specified table.
func (c *CowTabletList) RemoveTabletsWithTable(keyspace string, table string) {
	c.sendOp(opRemoveTable{keyspace: keyspace, table: table})
}
