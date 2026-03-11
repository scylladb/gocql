//go:build unit
// +build unit

package tablets

import (
	"sort"
	"sync"
	"testing"

	"github.com/gocql/gocql/internal/tests"
)

// --- Per-table list methods ---

func TestAddTabletToPerTableListEmpty(t *testing.T) {
	t.Parallel()

	tl := TabletInfoList{}
	tl = tl.addTabletToPerTableList(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -100, lastToken: 100,
	})

	tests.AssertEqual(t, "length", 1, len(tl))
	tests.AssertEqual(t, "firstToken", int64(-100), tl[0].FirstToken())
	tests.AssertEqual(t, "lastToken", int64(100), tl[0].LastToken())
}

func TestAddTabletToPerTableListBeginning(t *testing.T) {
	t.Parallel()

	tl := TabletInfoList{{
		keyspaceName: "ks", tableName: "tb",
		firstToken: 100, lastToken: 200,
	}}
	tl = tl.addTabletToPerTableList(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -200, lastToken: -100,
	})

	tests.AssertEqual(t, "length", 2, len(tl))
	tests.AssertTrue(t, "sorted", CompareRanges(tl, [][]int64{{-200, -100}, {100, 200}}))
}

func TestAddTabletToPerTableListEnd(t *testing.T) {
	t.Parallel()

	tl := TabletInfoList{{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -200, lastToken: -100,
	}}
	tl = tl.addTabletToPerTableList(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: 100, lastToken: 200,
	})

	tests.AssertEqual(t, "length", 2, len(tl))
	tests.AssertTrue(t, "sorted", CompareRanges(tl, [][]int64{{-200, -100}, {100, 200}}))
}

func TestAddTabletToPerTableListOverlap(t *testing.T) {
	t.Parallel()

	tl := TabletInfoList{
		{keyspaceName: "ks", tableName: "tb", firstToken: -300, lastToken: -200},
		{keyspaceName: "ks", tableName: "tb", firstToken: -200, lastToken: -100},
		{keyspaceName: "ks", tableName: "tb", firstToken: -100, lastToken: 0},
		{keyspaceName: "ks", tableName: "tb", firstToken: 0, lastToken: 100},
	}
	// New tablet {-150, 50} overlaps indices 1,2,3 (LastToken -100 > FirstToken -150, etc.)
	tl = tl.addTabletToPerTableList(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -150, lastToken: 50,
	})

	tests.AssertTrue(t, "overlap resolved",
		CompareRanges(tl, [][]int64{{-300, -200}, {-150, 50}}))
}

func TestBulkAddToPerTableListEmpty(t *testing.T) {
	t.Parallel()

	tl := TabletInfoList{}
	batch := TabletInfoList{
		{keyspaceName: "ks", tableName: "tb", firstToken: -200, lastToken: -100},
		{keyspaceName: "ks", tableName: "tb", firstToken: -100, lastToken: 0},
		{keyspaceName: "ks", tableName: "tb", firstToken: 0, lastToken: 100},
	}
	tl = tl.bulkAddToPerTableList(batch)

	tests.AssertEqual(t, "length", 3, len(tl))
	tests.AssertTrue(t, "ranges", CompareRanges(tl, [][]int64{{-200, -100}, {-100, 0}, {0, 100}}))
}

func TestBulkAddToPerTableListOverlap(t *testing.T) {
	t.Parallel()

	tl := TabletInfoList{
		{keyspaceName: "ks", tableName: "tb", firstToken: -400, lastToken: -300},
		{keyspaceName: "ks", tableName: "tb", firstToken: -300, lastToken: -200},
		{keyspaceName: "ks", tableName: "tb", firstToken: -200, lastToken: -100},
		{keyspaceName: "ks", tableName: "tb", firstToken: 100, lastToken: 200},
	}
	// Batch {-350,-150} overlaps indices 0,1,2 (LastToken -300 > FirstToken -350, etc.)
	batch := TabletInfoList{
		{keyspaceName: "ks", tableName: "tb", firstToken: -350, lastToken: -250},
		{keyspaceName: "ks", tableName: "tb", firstToken: -250, lastToken: -150},
	}
	tl = tl.bulkAddToPerTableList(batch)

	tests.AssertTrue(t, "overlap resolved",
		CompareRanges(tl, [][]int64{{-350, -250}, {-250, -150}, {100, 200}}))
}

// --- CowTabletList methods ---

func TestCowTabletListAddAndFind(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()

	host1 := tests.RandomUUID()
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks1", tableName: "tb1",
		firstToken: -100, lastToken: 0,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks1", tableName: "tb1",
		firstToken: 0, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 1}},
	})
	cl.Flush()

	// Find in first range
	ti := cl.FindTabletForToken("ks1", "tb1", -50)
	if ti == nil {
		t.Fatal("expected tablet for token -50")
	}
	tests.AssertEqual(t, "lastToken", int64(0), ti.LastToken())

	// Find in second range
	ti = cl.FindTabletForToken("ks1", "tb1", 50)
	if ti == nil {
		t.Fatal("expected tablet for token 50")
	}
	tests.AssertEqual(t, "lastToken", int64(100), ti.LastToken())

	// Miss on unknown table
	ti = cl.FindTabletForToken("ks1", "unknown", 0)
	if ti != nil {
		t.Fatal("expected nil for unknown table")
	}

	// Miss on unknown keyspace
	ti = cl.FindTabletForToken("unknown", "tb1", 0)
	if ti != nil {
		t.Fatal("expected nil for unknown keyspace")
	}
}

func TestCowTabletListFindReplicasForToken(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()
	host2 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks1", tableName: "tb1",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 0}, {host2, 1}},
	})
	cl.Flush()

	replicas := cl.FindReplicasForToken("ks1", "tb1", 0)
	tests.AssertEqual(t, "replica count", 2, len(replicas))
	tests.AssertEqual(t, "replica0 host", host1, replicas[0].HostID())
	tests.AssertEqual(t, "replica1 host", host2, replicas[1].HostID())

	// Miss returns nil
	replicas = cl.FindReplicasForToken("ks1", "missing", 0)
	if replicas != nil {
		t.Fatal("expected nil replicas for missing table")
	}
}

func TestCowTabletListMultiTable(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb1",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb2",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 5}},
	})
	cl.Flush()

	r1 := cl.FindReplicasForToken("ks", "tb1", 0)
	tests.AssertEqual(t, "tb1 shard", 0, r1[0].ShardID())

	r2 := cl.FindReplicasForToken("ks", "tb2", 0)
	tests.AssertEqual(t, "tb2 shard", 5, r2[0].ShardID())
}

func TestCowTabletListMultiKeyspace(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks1", tableName: "tb",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 1}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks2", tableName: "tb",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 2}},
	})
	cl.Flush()

	r1 := cl.FindReplicasForToken("ks1", "tb", 0)
	tests.AssertEqual(t, "ks1 shard", 1, r1[0].ShardID())

	r2 := cl.FindReplicasForToken("ks2", "tb", 0)
	tests.AssertEqual(t, "ks2 shard", 2, r2[0].ShardID())
}

func TestCowTabletListBulkAdd(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	batch := []*TabletInfo{
		{keyspaceName: "ks", tableName: "tb", firstToken: -300, lastToken: -200, replicas: []ReplicaInfo{{host1, 0}}},
		{keyspaceName: "ks", tableName: "tb", firstToken: -200, lastToken: -100, replicas: []ReplicaInfo{{host1, 1}}},
		{keyspaceName: "ks", tableName: "tb", firstToken: -100, lastToken: 0, replicas: []ReplicaInfo{{host1, 2}}},
	}
	cl.BulkAddTablets(batch)
	cl.Flush()

	ti := cl.FindTabletForToken("ks", "tb", -250)
	if ti == nil {
		t.Fatal("expected tablet")
	}
	tests.AssertEqual(t, "shard", 0, ti.Replicas()[0].ShardID())

	ti = cl.FindTabletForToken("ks", "tb", -150)
	if ti == nil {
		t.Fatal("expected tablet")
	}
	tests.AssertEqual(t, "shard", 1, ti.Replicas()[0].ShardID())
}

func TestCowTabletListBulkAddMultiTable(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	batch := []*TabletInfo{
		{keyspaceName: "ks", tableName: "tb1", firstToken: -100, lastToken: 0, replicas: []ReplicaInfo{{host1, 0}}},
		{keyspaceName: "ks", tableName: "tb1", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 1}}},
		{keyspaceName: "ks", tableName: "tb2", firstToken: -50, lastToken: 50, replicas: []ReplicaInfo{{host1, 2}}},
	}
	cl.BulkAddTablets(batch)
	cl.Flush()

	ti := cl.FindTabletForToken("ks", "tb1", -50)
	tests.AssertEqual(t, "tb1 shard", 0, ti.Replicas()[0].ShardID())

	ti = cl.FindTabletForToken("ks", "tb2", 0)
	tests.AssertEqual(t, "tb2 shard", 2, ti.Replicas()[0].ShardID())
}

func TestCowTabletListGet(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks1", tableName: "tb1",
		firstToken: -100, lastToken: 0,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks1", tableName: "tb2",
		firstToken: 0, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 1}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks2", tableName: "tb1",
		firstToken: 100, lastToken: 200,
		replicas: []ReplicaInfo{{host1, 2}},
	})
	cl.Flush()

	flat := cl.Get()
	tests.AssertEqual(t, "total tablets", 3, len(flat))

	sort.Slice(flat, func(i, j int) bool {
		return flat[i].FirstToken() < flat[j].FirstToken()
	})
	tests.AssertEqual(t, "first", int64(-100), flat[0].FirstToken())
	tests.AssertEqual(t, "second", int64(0), flat[1].FirstToken())
	tests.AssertEqual(t, "third", int64(100), flat[2].FirstToken())
}

func TestCowTabletListGetEmpty(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	flat := cl.Get()
	tests.AssertEqual(t, "empty list length", 0, len(flat))
}

func TestCowTabletListRemoveTabletsWithHost(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	removedHost := tests.RandomUUID()
	keptHost := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb1",
		firstToken: -100, lastToken: 0,
		replicas: []ReplicaInfo{{removedHost, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb1",
		firstToken: 0, lastToken: 100,
		replicas: []ReplicaInfo{{keptHost, 1}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb2",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{removedHost, 2}},
	})
	cl.RemoveTabletsWithHost(removedHost)
	cl.Flush()

	ti := cl.FindTabletForToken("ks", "tb1", 50)
	if ti == nil {
		t.Fatal("expected kept tablet in tb1")
	}
	tests.AssertEqual(t, "kept shard", 1, ti.Replicas()[0].ShardID())

	flat := cl.Get()
	for _, tab := range flat {
		for _, r := range tab.Replicas() {
			if r.HostID() == removedHost {
				t.Fatalf("found removed host in tablet %v", tab)
			}
		}
	}
}

func TestCowTabletListRemoveTabletsWithKeyspace(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "removed_ks", tableName: "tb1",
		firstToken: -100, lastToken: 0,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "removed_ks", tableName: "tb2",
		firstToken: 0, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 1}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "kept_ks", tableName: "tb1",
		firstToken: 100, lastToken: 200,
		replicas: []ReplicaInfo{{host1, 2}},
	})
	cl.RemoveTabletsWithKeyspace("removed_ks")
	cl.Flush()

	ti := cl.FindTabletForToken("removed_ks", "tb1", -50)
	if ti != nil {
		t.Fatal("expected nil for removed keyspace table tb1")
	}
	ti = cl.FindTabletForToken("removed_ks", "tb2", 50)
	if ti != nil {
		t.Fatal("expected nil for removed keyspace table tb2")
	}

	ti = cl.FindTabletForToken("kept_ks", "tb1", 150)
	if ti == nil {
		t.Fatal("expected tablet for kept keyspace")
	}
	tests.AssertEqual(t, "kept shard", 2, ti.Replicas()[0].ShardID())

	tests.AssertEqual(t, "total tablets", 1, len(cl.Get()))
}

func TestCowTabletListRemoveTabletsWithTable(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "removed_tb",
		firstToken: -100, lastToken: 0,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "kept_tb",
		firstToken: 0, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 1}},
	})
	cl.RemoveTabletsWithTableFromTabletsList("ks", "removed_tb")
	cl.Flush()

	ti := cl.FindTabletForToken("ks", "removed_tb", -50)
	if ti != nil {
		t.Fatal("expected nil for removed table")
	}

	ti = cl.FindTabletForToken("ks", "kept_tb", 50)
	if ti == nil {
		t.Fatal("expected tablet for kept table")
	}
	tests.AssertEqual(t, "kept shard", 1, ti.Replicas()[0].ShardID())
}

func TestCowTabletListRemoveNonexistent(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.RemoveTabletsWithKeyspace("nonexistent")
	cl.RemoveTabletsWithTableFromTabletsList("ks", "nonexistent")
	cl.RemoveTabletsWithHost("nonexistent-host-id")
	cl.Flush()

	tests.AssertEqual(t, "still has tablet", 1, len(cl.Get()))
}

func TestCowTabletListAddOverwritesExisting(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	host1 := tests.RandomUUID()
	host2 := tests.RandomUUID()

	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host1, 0}},
	})
	cl.AddTablet(&TabletInfo{
		keyspaceName: "ks", tableName: "tb",
		firstToken: -100, lastToken: 100,
		replicas: []ReplicaInfo{{host2, 5}},
	})
	cl.Flush()

	ti := cl.FindTabletForToken("ks", "tb", 0)
	if ti == nil {
		t.Fatal("expected tablet")
	}
	tests.AssertEqual(t, "updated host", host2, ti.Replicas()[0].HostID())
	tests.AssertEqual(t, "updated shard", 5, ti.Replicas()[0].ShardID())
}

// --- Concurrent access tests ---

func TestCowTabletListConcurrentReads(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	hosts := tests.GenerateHostNames(3)
	cl.BulkAddTablets(createTablets("ks", "tb", hosts, 2, 100, 100))
	cl.Flush()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := getThreadSafeRnd()
			for j := 0; j < 1000; j++ {
				token := rnd.Int63()
				cl.FindTabletForToken("ks", "tb", token)
				cl.FindReplicasForToken("ks", "tb", token)
			}
		}()
	}
	wg.Wait()
}

func TestCowTabletListConcurrentReadWrite(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	hosts := tests.GenerateHostNames(3)
	cl.BulkAddTablets(createTablets("ks", "tb", hosts, 2, 100, 100))
	cl.Flush()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := getThreadSafeRnd()
			for j := 0; j < 1000; j++ {
				cl.FindTabletForToken("ks", "tb", rnd.Int63())
				cl.Get()
			}
		}()
	}
	repGen := NewReplicaSetGenerator(hosts, 2)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := getThreadSafeRnd()
			for j := 0; j < 100; j++ {
				token := rnd.Int63()
				cl.AddTablet(&TabletInfo{
					keyspaceName: "ks", tableName: "tb",
					firstToken: token - 100, lastToken: token,
					replicas: repGen.Next(),
				})
			}
		}()
	}
	wg.Wait()
}

func TestCowTabletListConcurrentMultiTableReadWrite(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	hosts := tests.GenerateHostNames(6)
	tables := []string{"tb1", "tb2", "tb3", "tb4", "tb5"}

	for _, tb := range tables {
		cl.BulkAddTablets(createTablets("ks", tb, hosts, 3, 50, 50))
	}
	cl.Flush()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rnd := getThreadSafeRnd()
			tb := tables[idx%len(tables)]
			for j := 0; j < 500; j++ {
				cl.FindTabletForToken("ks", tb, rnd.Int63())
			}
		}(i)
	}

	repGen := NewReplicaSetGenerator(hosts, 3)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rnd := getThreadSafeRnd()
			tb := tables[idx%len(tables)]
			for j := 0; j < 50; j++ {
				token := rnd.Int63()
				cl.AddTablet(&TabletInfo{
					keyspaceName: "ks", tableName: tb,
					firstToken: token - 100, lastToken: token,
					replicas: repGen.Next(),
				})
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			cl.RemoveTabletsWithHost(hosts[i])
		}
	}()

	wg.Wait()
}

func TestCowTabletListConcurrentRemoveKeyspace(t *testing.T) {
	t.Parallel()

	cl := NewCowTabletList()
	defer cl.Close()
	hosts := tests.GenerateHostNames(3)

	cl.BulkAddTablets(createTablets("ks1", "tb", hosts, 2, 50, 50))
	cl.BulkAddTablets(createTablets("ks2", "tb", hosts, 2, 50, 50))
	cl.Flush()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rnd := getThreadSafeRnd()
			for j := 0; j < 500; j++ {
				cl.FindTabletForToken("ks1", "tb", rnd.Int63())
				cl.FindTabletForToken("ks2", "tb", rnd.Int63())
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		cl.RemoveTabletsWithKeyspace("ks2")
	}()

	wg.Wait()
	cl.Flush()

	ti := cl.FindTabletForToken("ks2", "tb", 0)
	if ti != nil {
		t.Fatal("expected nil for removed keyspace")
	}

	ti = cl.FindTabletForToken("ks1", "tb", 0)
	if ti == nil {
		t.Fatal("expected tablet for ks1")
	}
}
