//go:build unit
// +build unit

package tablets

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/internal/tests"
)

func testHostUUID(s string) HostUUID {
	var u HostUUID
	copy(u[:], s)
	return u
}

func compareEntryRanges(entries TabletEntryList, ranges [][]int64) bool {
	if len(entries) != len(ranges) {
		return false
	}
	for i, e := range entries {
		if e.firstToken != ranges[i][0] || e.lastToken != ranges[i][1] {
			return false
		}
	}
	return true
}

func TestAddTabletToPerTableList(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		tl := TabletEntryList{}
		tl = tl.addEntry(&TabletEntry{
			firstToken: -100, lastToken: 100,
		})

		tests.AssertEqual(t, "length", 1, len(tl))
		tests.AssertEqual(t, "firstToken", int64(-100), tl[0].firstToken)
		tests.AssertEqual(t, "lastToken", int64(100), tl[0].lastToken)
	})

	t.Run("Beginning", func(t *testing.T) {
		tl := TabletEntryList{{
			firstToken: 100, lastToken: 200,
		}}
		tl = tl.addEntry(&TabletEntry{
			firstToken: -200, lastToken: -100,
		})

		tests.AssertEqual(t, "length", 2, len(tl))
		tests.AssertTrue(t, "sorted", compareEntryRanges(tl, [][]int64{{-200, -100}, {100, 200}}))
	})

	t.Run("End", func(t *testing.T) {
		tl := TabletEntryList{{
			firstToken: -200, lastToken: -100,
		}}
		tl = tl.addEntry(&TabletEntry{
			firstToken: 100, lastToken: 200,
		})

		tests.AssertEqual(t, "length", 2, len(tl))
		tests.AssertTrue(t, "sorted", compareEntryRanges(tl, [][]int64{{-200, -100}, {100, 200}}))
	})

	t.Run("Overlap", func(t *testing.T) {
		tl := TabletEntryList{
			{firstToken: -300, lastToken: -200},
			{firstToken: -200, lastToken: -100},
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		tl = tl.addEntry(&TabletEntry{
			firstToken: -150, lastToken: 50,
		})

		tests.AssertTrue(t, "overlap resolved",
			compareEntryRanges(tl, [][]int64{{-300, -200}, {-150, 50}}))
	})

	t.Run("NewTabletContainedWithinExisting", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -300, lastToken: 300},
		}
		result := entries.addEntry(&TabletEntry{firstToken: -100, lastToken: 100})
		if len(result) != 1 {
			t.Errorf("expected 1 tablet after replacement, got %d", len(result))
		}
		if len(result) == 1 {
			tests.AssertEqual(t, "tablet firstToken", int64(-100), result[0].firstToken)
			tests.AssertEqual(t, "tablet lastToken", int64(100), result[0].lastToken)
		}
	})

	t.Run("NewTabletContainsMultiple", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -200, lastToken: -100},
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		result := entries.addEntry(&TabletEntry{firstToken: -300, lastToken: 200})
		if len(result) != 1 {
			t.Errorf("expected consolidation to 1 tablet, got %d", len(result))
		}
		if len(result) == 1 {
			tests.AssertEqual(t, "tablet firstToken", int64(-300), result[0].firstToken)
			tests.AssertEqual(t, "tablet lastToken", int64(200), result[0].lastToken)
		}
	})

	t.Run("MultiplePartialOverlaps", func(t *testing.T) {
		entries := TabletEntryList{
			{firstToken: -300, lastToken: -200},
			{firstToken: -100, lastToken: 0},
			{firstToken: 100, lastToken: 200},
		}
		result := entries.addEntry(&TabletEntry{firstToken: -150, lastToken: 150})
		if len(result) != 2 {
			t.Errorf("expected 2 tablets after partial overlap, got %d", len(result))
		}
		if len(result) == 2 {
			tests.AssertEqual(t, "first tablet firstToken", int64(-300), result[0].firstToken)
			tests.AssertEqual(t, "first tablet lastToken", int64(-200), result[0].lastToken)
			tests.AssertEqual(t, "second tablet firstToken", int64(-150), result[1].firstToken)
			tests.AssertEqual(t, "second tablet lastToken", int64(150), result[1].lastToken)
		}
	})
}

func TestBulkAddToPerTableList(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		tl := TabletEntryList{}
		batch := []*TabletEntry{
			{firstToken: -200, lastToken: -100},
			{firstToken: -100, lastToken: 0},
			{firstToken: 0, lastToken: 100},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertEqual(t, "length", 3, len(tl))
		tests.AssertTrue(t, "ranges", compareEntryRanges(tl, [][]int64{{-200, -100}, {-100, 0}, {0, 100}}))
	})

	t.Run("Overlap", func(t *testing.T) {
		tl := TabletEntryList{
			{firstToken: -400, lastToken: -300},
			{firstToken: -300, lastToken: -200},
			{firstToken: -200, lastToken: -100},
			{firstToken: 100, lastToken: 200},
		}
		batch := []*TabletEntry{
			{firstToken: -350, lastToken: -250},
			{firstToken: -250, lastToken: -150},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertTrue(t, "overlap resolved",
			compareEntryRanges(tl, [][]int64{{-350, -250}, {-250, -150}, {100, 200}}))
	})

	t.Run("IntraBatchOverlappingPair", func(t *testing.T) {
		tl := TabletEntryList{}
		batch := []*TabletEntry{
			{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
			{firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertEqual(t, "length", 1, len(tl))
		tests.AssertEqual(t, "firstToken", int64(50), tl[0].firstToken)
		tests.AssertEqual(t, "lastToken", int64(150), tl[0].lastToken)
		tests.AssertEqual(t, "host", testHostUUID("h2"), tl[0].replicas[0].hostId)
	})

	t.Run("IntraBatchOverlappingTriple", func(t *testing.T) {
		tl := TabletEntryList{}
		batch := []*TabletEntry{
			{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
			{firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
			{firstToken: 100, lastToken: 200, replicas: []ReplicaInfo{{testHostUUID("h3"), 0}}},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertEqual(t, "length", 1, len(tl))
		tests.AssertEqual(t, "firstToken", int64(100), tl[0].firstToken)
		tests.AssertEqual(t, "lastToken", int64(200), tl[0].lastToken)
	})

	t.Run("IntraBatchOverlappingWithExistingList", func(t *testing.T) {
		tl := TabletEntryList{
			{firstToken: -500, lastToken: -400},
			{firstToken: 500, lastToken: 600},
		}
		batch := []*TabletEntry{
			{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
			{firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertEqual(t, "length", 3, len(tl))
		tests.AssertEqual(t, "existing-pre firstToken", int64(-500), tl[0].firstToken)
		tests.AssertEqual(t, "resolved firstToken", int64(50), tl[1].firstToken)
		tests.AssertEqual(t, "resolved lastToken", int64(150), tl[1].lastToken)
		tests.AssertEqual(t, "existing-post firstToken", int64(500), tl[2].firstToken)
	})

	t.Run("NonOverlappingBatchStillWorks", func(t *testing.T) {
		tl := TabletEntryList{}
		batch := []*TabletEntry{
			{firstToken: 0, lastToken: 100},
			{firstToken: 100, lastToken: 200},
			{firstToken: 200, lastToken: 300},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertEqual(t, "length", 3, len(tl))
		tests.AssertTrue(t, "ranges", compareEntryRanges(tl, [][]int64{{0, 100}, {100, 200}, {200, 300}}))
	})

	t.Run("BatchWithGapsPreservesExisting", func(t *testing.T) {
		tl := TabletEntryList{
			{firstToken: 200, lastToken: 300, replicas: []ReplicaInfo{{testHostUUID("existing"), 0}}},
		}
		batch := []*TabletEntry{
			{firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
			{firstToken: 500, lastToken: 600, replicas: []ReplicaInfo{{testHostUUID("h2"), 0}}},
		}
		tl = tl.bulkAddEntries(batch)

		tests.AssertEqual(t, "length", 3, len(tl))
		tests.AssertTrue(t, "ranges", compareEntryRanges(tl, [][]int64{{0, 100}, {200, 300}, {500, 600}}))
		tests.AssertEqual(t, "existing host preserved", testHostUUID("existing"), tl[1].replicas[0].hostId)
	})
}

func TestCowTabletListAddAndFind(t *testing.T) {
	t.Parallel()

	t.Run("BasicAddAndFind", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		host1 := GenerateHostUUIDs(1)[0]
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

		ti := cl.FindTabletForToken("ks1", "tb1", -50)
		if ti == nil {
			t.Fatal("expected tablet for token -50")
		}
		tests.AssertEqual(t, "lastToken", int64(0), ti.LastToken())

		ti = cl.FindTabletForToken("ks1", "tb1", 50)
		if ti == nil {
			t.Fatal("expected tablet for token 50")
		}
		tests.AssertEqual(t, "lastToken", int64(100), ti.LastToken())

		ti = cl.FindTabletForToken("ks1", "unknown", 0)
		if ti != nil {
			t.Fatal("expected nil for unknown table")
		}

		ti = cl.FindTabletForToken("unknown", "tb1", 0)
		if ti != nil {
			t.Fatal("expected nil for unknown keyspace")
		}
	})

	t.Run("FindReplicasUnsafeForToken", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(2)
		host1 := hosts[0]
		host2 := hosts[1]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks1", tableName: "tb1",
			firstToken: -100, lastToken: 100,
			replicas: []ReplicaInfo{{host1, 0}, {host2, 1}},
		})
		cl.Flush()

		replicas := cl.FindReplicasUnsafeForToken("ks1", "tb1", 0)
		tests.AssertEqual(t, "replica count", 2, len(replicas))
		tests.AssertEqual(t, "replica0 host", host1.String(), replicas[0].HostID())
		tests.AssertEqual(t, "replica1 host", host2.String(), replicas[1].HostID())

		replicas = cl.FindReplicasUnsafeForToken("ks1", "missing", 0)
		if replicas != nil {
			t.Fatal("expected nil replicas for missing table")
		}
	})

	t.Run("MultiTable", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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

		r1 := cl.FindReplicasUnsafeForToken("ks", "tb1", 0)
		tests.AssertEqual(t, "tb1 shard", 0, r1[0].ShardID())

		r2 := cl.FindReplicasUnsafeForToken("ks", "tb2", 0)
		tests.AssertEqual(t, "tb2 shard", 5, r2[0].ShardID())
	})

	t.Run("MultiKeyspace", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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

		r1 := cl.FindReplicasUnsafeForToken("ks1", "tb", 0)
		tests.AssertEqual(t, "ks1 shard", 1, r1[0].ShardID())

		r2 := cl.FindReplicasUnsafeForToken("ks2", "tb", 0)
		tests.AssertEqual(t, "ks2 shard", 2, r2[0].ShardID())
	})

	t.Run("OverwritesExisting", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(2)
		host1 := hosts[0]
		host2 := hosts[1]

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
		tests.AssertEqual(t, "updated host", host2.String(), ti.Replicas()[0].HostID())
		tests.AssertEqual(t, "updated shard", 5, ti.Replicas()[0].ShardID())
	})

	t.Run("SameFirstTokenDifferentLastToken", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(2)
		host1 := hosts[0]
		host2 := hosts[1]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: 0, lastToken: 100,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: 0, lastToken: 200,
			replicas: []ReplicaInfo{{host2, 1}},
		})
		cl.Flush()

		ti := cl.FindTabletForToken("ks", "tb", 50)
		if ti == nil {
			t.Fatal("expected tablet for token 50")
		}
		tests.AssertEqual(t, "replaced host", host2.String(), ti.Replicas()[0].HostID())
		tests.AssertEqual(t, "replaced lastToken", int64(200), ti.LastToken())

		ti = cl.FindTabletForToken("ks", "tb", 150)
		if ti == nil {
			t.Fatal("expected tablet for token 150")
		}
		tests.AssertEqual(t, "host at 150", host2.String(), ti.Replicas()[0].HostID())
	})
}

func TestCowTabletListBulkAdd(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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
	})

	t.Run("MultiTable", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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
	})

	t.Run("SortsPerTableGroups", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		batch := []*TabletInfo{
			{keyspaceName: "ks", tableName: "tb1", firstToken: 100, lastToken: 200, replicas: []ReplicaInfo{{host1, 2}}},
			{keyspaceName: "ks", tableName: "tb2", firstToken: -100, lastToken: 100, replicas: []ReplicaInfo{{host1, 7}}},
			{keyspaceName: "ks", tableName: "tb1", firstToken: -100, lastToken: 0, replicas: []ReplicaInfo{{host1, 0}}},
			{keyspaceName: "ks", tableName: "tb1", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 1}}},
		}

		cl.BulkAddTablets(batch)
		cl.Flush()

		ti := cl.FindTabletForToken("ks", "tb1", -50)
		if ti == nil {
			t.Fatal("expected tablet for tb1 token -50")
		}
		tests.AssertEqual(t, "tb1 shard for -50", 0, ti.Replicas()[0].ShardID())

		ti = cl.FindTabletForToken("ks", "tb1", 50)
		if ti == nil {
			t.Fatal("expected tablet for tb1 token 50")
		}
		tests.AssertEqual(t, "tb1 shard for 50", 1, ti.Replicas()[0].ShardID())

		ti = cl.FindTabletForToken("ks", "tb1", 150)
		if ti == nil {
			t.Fatal("expected tablet for tb1 token 150")
		}
		tests.AssertEqual(t, "tb1 shard for 150", 2, ti.Replicas()[0].ShardID())

		ti = cl.FindTabletForToken("ks", "tb2", 0)
		if ti == nil {
			t.Fatal("expected tablet for tb2 token 0")
		}
		tests.AssertEqual(t, "tb2 shard", 7, ti.Replicas()[0].ShardID())
	})

	t.Run("NilEntries", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.BulkAddTablets([]*TabletInfo{
			nil,
			{keyspaceName: "ks", tableName: "tb", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 0}}},
			nil,
		})
		cl.Flush()

		result := cl.GetTableTablets("ks", "tb")
		tests.AssertEqual(t, "tablet count", 1, len(result))
		tests.AssertEqual(t, "firstToken", int64(0), result[0].FirstToken())
		tests.AssertEqual(t, "lastToken", int64(100), result[0].LastToken())
	})

	t.Run("EmptyIdentifiers", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.BulkAddTablets([]*TabletInfo{
			{keyspaceName: "", tableName: "tb", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 0}}},
			{keyspaceName: "ks", tableName: "", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 1}}},
			{keyspaceName: "", tableName: "", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 2}}},
			{keyspaceName: "ks", tableName: "tb", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{host1, 3}}},
		})
		cl.Flush()

		result := cl.GetTableTablets("ks", "tb")
		tests.AssertEqual(t, "valid tablet count", 1, len(result))
		tests.AssertEqual(t, "shard", 3, result[0].Replicas()[0].ShardID())

		tests.AssertEqual(t, "empty-ks phantom", 0, len(cl.GetTableTablets("", "tb")))
		tests.AssertEqual(t, "empty-table phantom", 0, len(cl.GetTableTablets("ks", "")))
		tests.AssertEqual(t, "both-empty phantom", 0, len(cl.GetTableTablets("", "")))
	})

	t.Run("IntraBatchOverlap", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		batch := []*TabletInfo{
			{keyspaceName: "ks", tableName: "tb", firstToken: 0, lastToken: 100, replicas: []ReplicaInfo{{testHostUUID("h1"), 0}}},
			{keyspaceName: "ks", tableName: "tb", firstToken: 50, lastToken: 150, replicas: []ReplicaInfo{{testHostUUID("h2"), 1}}},
		}
		cl.BulkAddTablets(batch)
		cl.Flush()

		result := cl.GetTableTablets("ks", "tb")
		tests.AssertEqual(t, "tablet count", 1, len(result))
		tests.AssertEqual(t, "firstToken", int64(50), result[0].FirstToken())
		tests.AssertEqual(t, "lastToken", int64(150), result[0].LastToken())
		tests.AssertEqual(t, "shard", 1, result[0].Replicas()[0].ShardID())
	})
}

func TestCowTabletListGet(t *testing.T) {
	t.Parallel()

	t.Run("AllTablets", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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
	})

	t.Run("Empty", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		flat := cl.Get()
		tests.AssertEqual(t, "empty list length", 0, len(flat))
	})
}

func TestCowTabletListGetTableTablets(t *testing.T) {
	t.Parallel()

	t.Run("MultipleTablesAndKeyspaces", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks1", tableName: "tb2",
			firstToken: 100, lastToken: 200,
			replicas: []ReplicaInfo{{host1, 2}},
		})
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks2", tableName: "tb1",
			firstToken: 200, lastToken: 300,
			replicas: []ReplicaInfo{{host1, 3}},
		})
		cl.Flush()

		result := cl.GetTableTablets("ks1", "tb1")
		tests.AssertEqual(t, "ks1.tb1 count", 2, len(result))
		tests.AssertEqual(t, "ks1.tb1 first token", int64(-100), result[0].FirstToken())
		tests.AssertEqual(t, "ks1.tb1 second token", int64(0), result[1].FirstToken())

		result = cl.GetTableTablets("ks1", "tb2")
		tests.AssertEqual(t, "ks1.tb2 count", 1, len(result))
		tests.AssertEqual(t, "ks1.tb2 first token", int64(100), result[0].FirstToken())

		result = cl.GetTableTablets("ks2", "tb1")
		tests.AssertEqual(t, "ks2.tb1 count", 1, len(result))
		tests.AssertEqual(t, "ks2.tb1 first token", int64(200), result[0].FirstToken())
	})

	t.Run("NonExistent", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks1", tableName: "tb1",
			firstToken: -100, lastToken: 0,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.Flush()

		result := cl.GetTableTablets("no_such_ks", "tb1")
		if result != nil {
			t.Fatalf("expected nil for non-existent keyspace, got %d tablets", len(result))
		}

		result = cl.GetTableTablets("ks1", "no_such_tb")
		if result != nil {
			t.Fatalf("expected nil for non-existent table, got %d tablets", len(result))
		}
	})

	t.Run("ReturnsCopy", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 0,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: 0, lastToken: 100,
			replicas: []ReplicaInfo{{host1, 1}},
		})
		cl.Flush()

		result1 := cl.GetTableTablets("ks", "tb")
		result2 := cl.GetTableTablets("ks", "tb")

		result1[0] = nil
		if result2[0] == nil {
			t.Fatal("GetTableTablets should return independent copies")
		}
	})

	t.Run("Empty", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		result := cl.GetTableTablets("ks", "tb")
		if result != nil {
			t.Fatalf("expected nil for empty list, got %d tablets", len(result))
		}
	})
}

func TestCowTabletListRemove(t *testing.T) {
	t.Parallel()

	t.Run("WithHost", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(2)
		removedHost := hosts[0]
		keptHost := hosts[1]

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
				if r.HostUUIDValue() == removedHost {
					t.Fatalf("found removed host in tablet %v", tab)
				}
			}
		}
	})

	t.Run("WithKeyspace", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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
	})

	t.Run("WithTable", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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
		cl.RemoveTabletsWithTable("ks", "removed_tb")
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
	})

	t.Run("Nonexistent", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 100,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.RemoveTabletsWithKeyspace("nonexistent")
		cl.RemoveTabletsWithTable("ks", "nonexistent")
		cl.RemoveTabletsWithHost(testHostUUID("nonexistent-host"))
		cl.Flush()

		tests.AssertEqual(t, "still has tablet", 1, len(cl.Get()))
	})
}

func TestCowTabletListForEach(t *testing.T) {
	t.Parallel()

	t.Run("VisitsAllTables", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

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

		visited := make(map[string]int) // "keyspace.table" -> entry count
		cl.ForEach(func(keyspace, table string, entries TabletEntryList) bool {
			visited[keyspace+"."+table] = len(entries)
			return true
		})

		tests.AssertEqual(t, "visited count", 3, len(visited))
		tests.AssertEqual(t, "ks1.tb1 entries", 1, visited["ks1.tb1"])
		tests.AssertEqual(t, "ks1.tb2 entries", 1, visited["ks1.tb2"])
		tests.AssertEqual(t, "ks2.tb1 entries", 1, visited["ks2.tb1"])
	})

	t.Run("StopsEarly", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		for i := 0; i < 10; i++ {
			cl.AddTablet(&TabletInfo{
				keyspaceName: "ks", tableName: fmt.Sprintf("tb%d", i),
				firstToken: int64(i * 100), lastToken: int64(i*100 + 99),
				replicas: []ReplicaInfo{{host1, i}},
			})
		}
		cl.Flush()

		count := 0
		cl.ForEach(func(keyspace, table string, entries TabletEntryList) bool {
			count++
			return count < 3 // stop after visiting 3 tables
		})

		tests.AssertEqual(t, "stopped after 3", 3, count)
	})

	t.Run("Empty", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		count := 0
		cl.ForEach(func(keyspace, table string, entries TabletEntryList) bool {
			count++
			return true
		})

		tests.AssertEqual(t, "empty iteration", 0, count)
	})

	t.Run("NilReceiver", func(t *testing.T) {
		var cl *CowTabletList

		cl.ForEach(func(keyspace, table string, entries TabletEntryList) bool {
			t.Fatal("callback should not be called on nil receiver")
			return true
		})
	})

	t.Run("MutationDoesNotCorruptState", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 0,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: 0, lastToken: 100,
			replicas: []ReplicaInfo{{host1, 1}},
		})
		cl.Flush()

		cl.ForEach(func(keyspace, table string, entries TabletEntryList) bool {
			for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
				entries[i], entries[j] = entries[j], entries[i]
			}
			entries[0] = nil
			return true
		})

		entry := cl.FindTabletForToken("ks", "tb", 50)
		if entry == nil {
			t.Fatal("expected to find tablet for token 50 after ForEach mutation")
		}
		tests.AssertEqual(t, "firstToken", int64(0), entry.FirstToken())
		tests.AssertEqual(t, "lastToken", int64(100), entry.LastToken())

		entry = cl.FindTabletForToken("ks", "tb", -50)
		if entry == nil {
			t.Fatal("expected to find tablet for token -50 after ForEach mutation")
		}
		tests.AssertEqual(t, "firstToken", int64(-100), entry.FirstToken())
		tests.AssertEqual(t, "lastToken", int64(0), entry.LastToken())
	})

	t.Run("EntriesAreReadable", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 0,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: 0, lastToken: 100,
			replicas: []ReplicaInfo{{host1, 1}},
		})
		cl.Flush()

		cl.ForEach(func(keyspace, table string, entries TabletEntryList) bool {
			tests.AssertEqual(t, "keyspace", "ks", keyspace)
			tests.AssertEqual(t, "table", "tb", table)
			tests.AssertEqual(t, "entry count", 2, len(entries))
			tests.AssertEqual(t, "first entry firstToken", int64(-100), entries[0].FirstToken())
			tests.AssertEqual(t, "first entry lastToken", int64(0), entries[0].LastToken())
			tests.AssertEqual(t, "second entry firstToken", int64(0), entries[1].FirstToken())
			tests.AssertEqual(t, "second entry lastToken", int64(100), entries[1].LastToken())
			return true
		})
	})

	t.Run("NilCallback", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 100,
			replicas: []ReplicaInfo{{testHostUUID("host1"), 0}},
		})
		cl.Flush()

		cl.ForEach(nil)
	})
}

func TestCowTabletListLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("CloseIdempotent", func(t *testing.T) {
		cl := NewCowTabletList()
		cl.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			cl.Close()
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("second Close call did not return")
		}
	})

	t.Run("FlushAfterClose", func(t *testing.T) {
		cl := NewCowTabletList()
		cl.Close()

		done := make(chan struct{})
		go func() {
			defer close(done)
			cl.Flush()
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Flush after Close did not return")
		}
	})

	t.Run("AddAfterCloseNoop", func(t *testing.T) {
		cl := NewCowTabletList()
		cl.Close()

		host1 := GenerateHostUUIDs(1)[0]
		done := make(chan struct{})
		go func() {
			defer close(done)
			cl.AddTablet(&TabletInfo{
				keyspaceName: "ks", tableName: "tb",
				firstToken: -100, lastToken: 100,
				replicas: []ReplicaInfo{{host1, 0}},
			})
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("AddTablet after Close did not return")
		}

		tests.AssertEqual(t, "tablet count", 0, len(cl.Get()))
	})

	t.Run("NilReceiver", func(t *testing.T) {
		var cl *CowTabletList

		cl.Close()
		cl.Flush()
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 100,
			replicas: []ReplicaInfo{{testHostUUID("host"), 0}},
		})
		cl.BulkAddTablets([]*TabletInfo{{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -100, lastToken: 100,
			replicas: []ReplicaInfo{{testHostUUID("host"), 0}},
		}})
		cl.RemoveTabletsWithHost(testHostUUID("host"))
		cl.RemoveTabletsWithKeyspace("ks")
		cl.RemoveTabletsWithTable("ks", "tb")
	})
}

func TestOpQueueRun(t *testing.T) {
	t.Parallel()

	newTablet := func(first, last int64) *TabletInfo {
		return &TabletInfo{
			keyspaceName: "ks",
			tableName:    "tb",
			firstToken:   first,
			lastToken:    last,
		}
	}

	runQueue := func(send func(q *opQueue), wantTypes []string, validate func(t *testing.T, processed []tabletOp)) {
		t.Helper()

		q := newOpQueue()
		var mu sync.Mutex
		processed := make([]tabletOp, 0, len(wantTypes))
		go q.run(func(op tabletOp) {
			mu.Lock()
			processed = append(processed, op)
			mu.Unlock()
			if flush, ok := op.(opFlush); ok {
				close(flush.done)
			}
		})

		send(q)
		q.close()

		mu.Lock()
		defer mu.Unlock()
		if len(processed) != len(wantTypes) {
			t.Fatalf("processed %d ops, want %d", len(processed), len(wantTypes))
		}

		for i, op := range processed {
			switch wantTypes[i] {
			case "bulk":
				if _, ok := op.(opBulkAddTablets); !ok {
					t.Fatalf("processed[%d] = %T, want opBulkAddTablets", i, op)
				}
			case "flush":
				if _, ok := op.(opFlush); !ok {
					t.Fatalf("processed[%d] = %T, want opFlush", i, op)
				}
			case "removeHost":
				if _, ok := op.(opRemoveHost); !ok {
					t.Fatalf("processed[%d] = %T, want opRemoveHost", i, op)
				}
			default:
				t.Fatalf("unsupported expected type %q", wantTypes[i])
			}
		}

		if validate != nil {
			validate(t, processed)
		}
	}

	t.Run("CoalescesBufferedAddOps", func(t *testing.T) {
		runQueue(func(q *opQueue) {
			q.send(opAddTablet{tablet: newTablet(0, 99)})
			q.send(opAddTablet{tablet: newTablet(100, 199)})
			q.send(opAddTablet{tablet: newTablet(200, 299)})
			q.flush()
		}, []string{"bulk", "flush"}, func(t *testing.T, processed []tabletOp) {
			bulk := processed[0].(opBulkAddTablets)
			if len(bulk.tablets) != 3 {
				t.Fatalf("coalesced %d tablets, want 3", len(bulk.tablets))
			}
		})
	})

	t.Run("DoesNotCrossFlushOrNonAddBoundaries", func(t *testing.T) {
		runQueue(func(q *opQueue) {
			flushDone := make(chan struct{})
			q.send(opAddTablet{tablet: newTablet(0, 99)})
			q.send(opFlush{done: flushDone})
			q.send(opRemoveHost{hostID: testHostUUID("host-1")})
			q.send(opAddTablet{tablet: newTablet(100, 199)})
			<-flushDone
		}, []string{"bulk", "flush", "removeHost", "bulk"}, func(t *testing.T, processed []tabletOp) {
			first := processed[0].(opBulkAddTablets)
			second := processed[3].(opBulkAddTablets)
			if len(first.tablets) != 1 || len(second.tablets) != 1 {
				t.Fatalf("unexpected coalesced batch sizes: %d and %d", len(first.tablets), len(second.tablets))
			}
		})
	})
}

func TestCowTabletListConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("ConcurrentReads", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(3)
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
					cl.FindReplicasUnsafeForToken("ks", "tb", token)
				}
			}()
		}
		wg.Wait()
	})

	t.Run("ConcurrentReadWrite", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(3)
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
	})

	t.Run("ConcurrentMultiTableReadWrite", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(6)
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
	})

	t.Run("ConcurrentRemoveKeyspace", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		hosts := GenerateHostUUIDs(3)

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
	})

	t.Run("ConcurrentRemovalOperations", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		hostUUIDs := GenerateHostUUIDs(9) // 3 keyspaces * 3 hosts
		hostIdx := 0
		for ks := 0; ks < 3; ks++ {
			for host := 0; host < 3; host++ {
				hostID := hostUUIDs[hostIdx]
				hostIdx++
				for i := 0; i < 10; i++ {
					cl.AddTablet(&TabletInfo{
						keyspaceName: fmt.Sprintf("ks%d", ks),
						tableName:    "tb",
						firstToken:   int64(ks*10000 + host*1000 + i*100),
						lastToken:    int64(ks*10000 + host*1000 + i*100 + 99),
						replicas:     []ReplicaInfo{{hostID, 0}},
					})
				}
			}
		}
		cl.Flush()

		allTablets := cl.Get()
		var host0, host1 HostUUID
		var host0Set, host1Set bool
		hostCount := make(map[HostUUID]int)
		for _, tablet := range allTablets {
			for _, replica := range tablet.Replicas() {
				hostID := replica.HostUUIDValue()
				hostCount[hostID]++
				if !host0Set {
					host0 = hostID
					host0Set = true
				} else if !host1Set && hostID != host0 {
					host1 = hostID
					host1Set = true
				}
			}
		}
		_ = host1 // host1 is used below implicitly via hostCount

		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			defer wg.Done()
			cl.RemoveTabletsWithHost(host0)
		}()

		go func() {
			defer wg.Done()
			cl.RemoveTabletsWithKeyspace("ks1")
		}()

		go func() {
			defer wg.Done()
			cl.RemoveTabletsWithTable("ks2", "tb")
		}()

		wg.Wait()
		cl.Flush()

		remaining := cl.Get()
		t.Logf("Remaining tablets after concurrent removals: %d", len(remaining))

		for _, tablet := range remaining {
			for _, replica := range tablet.Replicas() {
				if replica.HostUUIDValue() == host0 {
					t.Errorf("found tablet with removed host %s", host0.String())
				}
			}
		}

		for _, tablet := range remaining {
			if tablet.KeyspaceName() == "ks1" || tablet.KeyspaceName() == "ks2" {
				t.Errorf("found tablet in removed keyspace: %s", tablet.KeyspaceName())
			}
		}
	})

	t.Run("CloseRace", func(t *testing.T) {
		list := NewCowTabletList()

		tablet := &TabletInfo{
			keyspaceName: "ks",
			tableName:    "tbl",
			firstToken:   -100,
			lastToken:    100,
			replicas:     []ReplicaInfo{{testHostUUID("host1"), 0}},
		}
		list.BulkAddTablets([]*TabletInfo{tablet})
		list.Flush()

		ready := make(chan struct{})
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				defer func() { done <- true }()
				ready <- struct{}{}
				for j := 0; j < 1000; j++ {
					_ = list.FindTabletForToken("ks", "tbl", 50)
				}
			}()
		}

		for i := 0; i < 10; i++ {
			<-ready
		}

		list.Close()

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("FlushCloseRace", func(t *testing.T) {
		cl := NewCowTabletList()

		uuids := GenerateHostUUIDs(100)
		for i := 0; i < 100; i++ {
			cl.AddTablet(&TabletInfo{
				keyspaceName: "ks", tableName: "tb",
				firstToken: int64(i * 100), lastToken: int64(i*100 + 99),
				replicas: []ReplicaInfo{{uuids[i], 0}},
			})
		}

		var wg sync.WaitGroup
		const flushers = 10

		for i := 0; i < flushers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cl.Flush()
			}()
		}

		cl.Close()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Flush + Close caused deadlock")
		}
	})
}

func TestCowTabletListEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("ExtremeTokenValues", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()
		host1 := GenerateHostUUIDs(1)[0]

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: math.MinInt64, lastToken: 0,
			replicas: []ReplicaInfo{{host1, 0}},
		})
		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: 0, lastToken: math.MaxInt64,
			replicas: []ReplicaInfo{{host1, 1}},
		})
		cl.Flush()

		ti := cl.FindTabletForToken("ks", "tb", math.MinInt64)
		if ti == nil {
			t.Fatal("expected tablet for math.MinInt64")
		}
		tests.AssertEqual(t, "MinInt64 shard", 0, ti.Replicas()[0].ShardID())

		ti = cl.FindTabletForToken("ks", "tb", math.MaxInt64)
		if ti == nil {
			t.Fatal("expected tablet for math.MaxInt64")
		}
		tests.AssertEqual(t, "MaxInt64 shard", 1, ti.Replicas()[0].ShardID())

		ti = cl.FindTabletForToken("ks", "tb", 0)
		if ti == nil {
			t.Fatal("expected tablet for token 0")
		}
	})

	t.Run("EmptyReplicaSet", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks",
			tableName:    "tb",
			firstToken:   -100,
			lastToken:    100,
			replicas:     []ReplicaInfo{},
		})
		cl.Flush()

		replicas := cl.FindReplicasForToken("ks", "tb", 0)
		if replicas == nil {
			t.Log("FindReplicasForToken returned nil for tablet with no replicas")
		} else if len(replicas) != 0 {
			t.Errorf("expected empty replica set, got %d replicas", len(replicas))
		}

		tablet := cl.FindTabletForToken("ks", "tb", 0)
		if tablet == nil {
			t.Fatal("expected tablet to exist")
		}
		if len(tablet.ReplicasUnsafe()) != 0 {
			t.Errorf("expected empty replica list, got %d replicas", len(tablet.ReplicasUnsafe()))
		}
	})

	t.Run("TabletInfoBuilderInvalidRange", func(t *testing.T) {
		hostID := GenerateHostUUIDs(1)[0]
		builder := TabletInfoBuilder{
			KeyspaceName: "ks",
			TableName:    "tb",
			FirstToken:   100,
			LastToken:    -100,
			Replicas:     [][]any{{hostID.String(), 0}},
		}
		_, err := builder.Build()
		if err == nil {
			t.Fatal("expected error for inverted token range")
		}
		t.Logf("Got expected error: %v", err)
	})

	t.Run("QueueSaturation", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		const operations = 10000
		done := make(chan bool)

		uuids := GenerateHostUUIDs(operations)
		go func() {
			for i := 0; i < operations; i++ {
				tablet := &TabletInfo{
					keyspaceName: "ks",
					tableName:    "tb",
					firstToken:   int64(i * 100),
					lastToken:    int64(i*100 + 99),
					replicas:     []ReplicaInfo{{uuids[i], 0}},
				}
				cl.AddTablet(tablet)
			}
			done <- true
		}()

		select {
		case <-done:
			cl.Flush()
			tablets := cl.GetTableTablets("ks", "tb")
			tests.AssertEqual(t, "tablet count", operations, len(tablets))
		case <-time.After(10 * time.Second):
			t.Fatal("queue saturation caused deadlock")
		}
	})

	t.Run("QueueSaturationReadsConsistent", func(t *testing.T) {
		cl := NewCowTabletList()
		defer cl.Close()

		cl.AddTablet(&TabletInfo{
			keyspaceName: "ks", tableName: "tb",
			firstToken: -1000, lastToken: -900,
			replicas: []ReplicaInfo{{GenerateHostUUIDs(1)[0], 0}},
		})
		cl.Flush()

		const writers = 10000
		var wg sync.WaitGroup

		writerUUIDs := GenerateHostUUIDs(writers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writers; i++ {
				cl.AddTablet(&TabletInfo{
					keyspaceName: "ks", tableName: "tb",
					firstToken: int64(i * 100), lastToken: int64(i*100 + 99),
					replicas: []ReplicaInfo{{writerUUIDs[i], 0}},
				})
			}
		}()

		const readers = 5
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					entry := cl.FindTabletForToken("ks", "tb", -950)
					if entry != nil {
						if entry.FirstToken() > entry.LastToken() {
							t.Errorf("invalid token range: first=%d > last=%d", entry.FirstToken(), entry.LastToken())
						}
						replicas := entry.Replicas()
						if len(replicas) == 0 {
							t.Error("expected at least one replica")
						}
					}

					entries := cl.GetTableTablets("ks", "tb")
					for _, e := range entries {
						if e == nil {
							t.Error("nil entry in GetTableTablets result")
							continue
						}
						if e.FirstToken() > e.LastToken() {
							t.Errorf("invalid token range in list: first=%d > last=%d", e.FirstToken(), e.LastToken())
						}
					}
				}
			}()
		}

		wg.Wait()
	})

}
