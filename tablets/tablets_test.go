//go:build unit
// +build unit

package tablets

import (
	"testing"

	"github.com/gocql/gocql/internal/tests"
)

var tablets = TabletInfoList{
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}},
		firstToken:   -7917529027641081857,
		lastToken:    -6917529027641081857,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 8}},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}},
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 8}},
		firstToken:   -2305843009213693953,
		lastToken:    -1,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 3}},
		firstToken:   -1,
		lastToken:    2305843009213693951,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 3}},
		firstToken:   2305843009213693951,
		lastToken:    4611686018427387903,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 7}},
		firstToken:   4611686018427387903,
		lastToken:    6917529027641081855,
	},
	{
		keyspaceName: "test1",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 7}},
		firstToken:   6917529027641081855,
		lastToken:    9223372036854775807,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}},
		firstToken:   -7917529027641081857,
		lastToken:    -6917529027641081857,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 8}},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}},
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 8}},
		firstToken:   -2305843009213693953,
		lastToken:    -1,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 3}},
		firstToken:   -1,
		lastToken:    2305843009213693951,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 3}},
		firstToken:   2305843009213693951,
		lastToken:    4611686018427387903,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 7}},
		firstToken:   4611686018427387903,
		lastToken:    6917529027641081855,
	},
	{
		keyspaceName: "test2",
		tableName:    "table1",
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 7}},
		firstToken:   6917529027641081855,
		lastToken:    9223372036854775807,
	},
}

func TestFindTablets(t *testing.T) {
	t.Parallel()

	id, id2 := tablets.FindTablets("test1", "table1")
	tests.AssertEqual(t, "id", 0, id)
	tests.AssertEqual(t, "id2", 7, id2)

	id, id2 = tablets.FindTablets("test2", "table1")
	tests.AssertEqual(t, "id", 8, id)
	tests.AssertEqual(t, "id2", 15, id2)

	id, id2 = tablets.FindTablets("test3", "table1")
	tests.AssertEqual(t, "id", -1, id)
	tests.AssertEqual(t, "id2", -1, id2)
}

func TestFindTabletForToken(t *testing.T) {
	t.Parallel()

	tablet := tablets.FindTabletForToken(0, 0, 7)
	tests.AssertTrue(t, "tablet.lastToken == 2305843009213693951", tablet.lastToken == 2305843009213693951)

	tablet = tablets.FindTabletForToken(9223372036854775807, 0, 7)
	tests.AssertTrue(t, "tablet.lastToken == 9223372036854775807", tablet.lastToken == 9223372036854775807)

	tablet = tablets.FindTabletForToken(-4611686018427387904, 0, 7)
	tests.AssertTrue(t, "tablet.lastToken == -2305843009213693953", tablet.lastToken == -2305843009213693953)
}

func CompareRanges(tablets TabletInfoList, ranges [][]int64) bool {
	if len(tablets) != len(ranges) {
		return false
	}

	for idx, tablet := range tablets {
		if tablet.FirstToken() != ranges[idx][0] || tablet.LastToken() != ranges[idx][1] {
			return false
		}
	}
	return true
}
func TestAddTabletToEmptyTablets(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheBeggining(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -8611686018427387905,
		lastToken:    -7917529027641081857,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857}, {-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheEnd(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -1,
		lastToken:    2305843009213693951,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-1, 2305843009213693951}}))
}

func TestAddTabletInTheMiddle(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -1,
		lastToken:    2305843009213693951,
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-4611686018427387905, -2305843009213693953},
		{-1, 2305843009213693951}}))
}

func TestAddTabletIntersecting(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -2305843009213693953,
		lastToken:    -1,
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -1,
		lastToken:    2305843009213693951,
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -3611686018427387905,
		lastToken:    -6,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
			{-3611686018427387905, -6},
			{-1, 2305843009213693951}}))
}

func TestAddTabletIntersectingWithFirst(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -8611686018427387905,
		lastToken:    -7917529027641081857,
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -8011686018427387905,
		lastToken:    -7987529027641081857,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8011686018427387905, -7987529027641081857},
		{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletIntersectingWithLast(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -8611686018427387905,
		lastToken:    -7917529027641081857,
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		replicas:     []ReplicaInfo{},
		firstToken:   -5011686018427387905,
		lastToken:    -2987529027641081857,
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857},
		{-5011686018427387905, -2987529027641081857}}))
}

func TestRemoveTabletsWithHost(t *testing.T) {
	t.Parallel()

	removed_host_id := tests.RandomUUID()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		firstToken:   -8611686018427387905,
		lastToken:    -7917529027641081857,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
		replicas:     []ReplicaInfo{{removed_host_id, 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {removed_host_id, 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithHost(removed_host_id)

	tests.AssertEqual(t, "TabletsList length", 1, len(tablets))
}

func TestRemoveTabletsWithKeyspace(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "removed_ks",
		tableName:    "test_tb",
		firstToken:   -8611686018427387905,
		lastToken:    -7917529027641081857,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		keyspaceName: "removed_ks",
		tableName:    "test_tb",
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithKeyspace("removed_ks")

	tests.AssertEqual(t, "TabletsList length", 1, len(tablets))
}

func TestRemoveTabletsWithTable(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		firstToken:   -8611686018427387905,
		lastToken:    -7917529027641081857,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		keyspaceName: "test_ks",
		tableName:    "test_tb",
		firstToken:   -6917529027641081857,
		lastToken:    -4611686018427387905,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		keyspaceName: "test_ks",
		tableName:    "removed_tb",
		firstToken:   -4611686018427387905,
		lastToken:    -2305843009213693953,
		replicas:     []ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithTableFromTabletsList("test_ks", "removed_tb")

	tests.AssertEqual(t, "TabletsList length", 2, len(tablets))
}
