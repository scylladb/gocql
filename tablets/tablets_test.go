//go:build unit
// +build unit

package tablets

import (
	"testing"

	"github.com/gocql/gocql/internal/tests"
)

var tablets = TabletInfoList{
	{
		"test1",
		"table1",
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		"test1",
		"table1",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		"test1",
		"table1",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		"test1",
		"table1",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		"test1",
		"table1",
		-1,
		2305843009213693951,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		"test1",
		"table1",
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		"test1",
		"table1",
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
	{
		"test1",
		"table1",
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
	{
		"test2",
		"table1",
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		"test2",
		"table1",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		"test2",
		"table1",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		"test2",
		"table1",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		"test2",
		"table1",
		-1,
		2305843009213693951,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		"test2",
		"table1",
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		"test2",
		"table1",
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
	{
		"test2",
		"table1",
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
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
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheBeggining(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857}, {-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheEnd(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-1, 2305843009213693951}}))
}

func TestAddTabletInTheMiddle(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
		{-4611686018427387905, -2305843009213693953},
		{-1, 2305843009213693951}}))
}

func TestAddTabletIntersecting(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-2305843009213693953,
		-1,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-3611686018427387905,
		-6,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct",
		CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905},
			{-3611686018427387905, -6},
			{-1, 2305843009213693951}}))
}

func TestAddTabletIntersectingWithFirst(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-8011686018427387905,
		-7987529027641081857,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8011686018427387905, -7987529027641081857},
		{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletIntersectingWithLast(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
		"test_ks",
		"test_tb",
		-5011686018427387905,
		-2987529027641081857,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-8611686018427387905, -7917529027641081857},
		{-5011686018427387905, -2987529027641081857}}))
}

func TestRemoveTabletsWithHost(t *testing.T) {
	t.Parallel()

	removed_host_id := tests.RandomUUID()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{removed_host_id, 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {removed_host_id, 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithHost(removed_host_id)

	tests.AssertEqual(t, "TabletsList length", 1, len(tablets))
}

func TestRemoveTabletsWithKeyspace(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"removed_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		"removed_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithKeyspace("removed_ks")

	tests.AssertEqual(t, "TabletsList length", 1, len(tablets))
}

func TestRemoveTabletsWithTable(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		"test_ks",
		"test_tb",
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		"test_ks",
		"test_tb",
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		"test_ks",
		"removed_tb",
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithTableFromTabletsList("test_ks", "removed_tb")

	tests.AssertEqual(t, "TabletsList length", 2, len(tablets))
}
