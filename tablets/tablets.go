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

type TabletInfoBuilder struct {
	KeyspaceName string
	TableName    string
	FirstToken   int64
	LastToken    int64
	Replicas     [][]interface{}
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
	firstToken   int64
	lastToken    int64
	replicas     []ReplicaInfo
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

// Search for place in tablets table with specific Keyspace and Table name
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

// Remove all tablets that have given host as a replica
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

// CowTabletList implements a copy on write tablet list, its equivalent type is TabletInfoList
type CowTabletList struct {
	list atomic.Value
	// every tablet update reads tablets states, update it and then writes it.
	// parallel writing will cause one of write being lost
	writeLock sync.Mutex
}

func NewCowTabletList() CowTabletList {
	list := atomic.Value{}
	list.Store(make(TabletInfoList, 0))
	return CowTabletList{
		list: list,
	}
}

func (c *CowTabletList) Get() TabletInfoList {
	return c.list.Load().(TabletInfoList)
}

func (c *CowTabletList) set(tablets TabletInfoList) {
	c.list.Store(tablets)
}

func (c *CowTabletList) AddTablet(tablet *TabletInfo) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	c.set(c.Get().AddTabletToTabletsList(tablet))
}

func (c *CowTabletList) RemoveTabletsWithHost(hostID string) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	c.set(c.Get().RemoveTabletsWithHost(hostID))
}

func (c *CowTabletList) RemoveTabletsWithKeyspace(keyspace string) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	c.set(c.Get().RemoveTabletsWithKeyspace(keyspace))
}

func (c *CowTabletList) RemoveTabletsWithTableFromTabletsList(keyspace string, table string) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	c.set(c.Get().RemoveTabletsWithTableFromTabletsList(keyspace, table))
}

func (c *CowTabletList) FindReplicasForToken(keyspace, table string, token int64) []ReplicaInfo {
	tablets := c.Get()
	l, r := tablets.FindTablets(keyspace, table)
	if l == -1 {
		return nil
	}
	return tablets.FindTabletForToken(token, l, r).Replicas()
}
