//go:build unit
// +build unit

package tablets

import (
	"testing"

	"github.com/gocql/gocql/internal/tests"
)

var tablets = TabletInfoList{
	{
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		-1,
		2305843009213693951,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
	{
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
	{
		-7917529027641081857,
		-6917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}},
	},
	{
		-2305843009213693953,
		-1,
		[]ReplicaInfo{{tests.RandomUUID(), 8}},
	},
	{
		-1,
		2305843009213693951,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		2305843009213693951,
		4611686018427387903,
		[]ReplicaInfo{{tests.RandomUUID(), 3}},
	},
	{
		4611686018427387903,
		6917529027641081855,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
	{
		6917529027641081855,
		9223372036854775807,
		[]ReplicaInfo{{tests.RandomUUID(), 7}},
	},
}

func TestFindTabletForToken(t *testing.T) {
	t.Parallel()

	tablet := tablets.FindTabletForToken(7)
	tests.AssertTrue(t, "tablet.lastToken == 2305843009213693951", tablet.lastToken == 2305843009213693951)
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
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	})

	tests.AssertTrue(t, "Token range in tablets table not correct", CompareRanges(tablets, [][]int64{{-6917529027641081857, -4611686018427387905}}))
}

func TestAddTabletAtTheBeggining(t *testing.T) {
	t.Parallel()

	tablets := TabletInfoList{{
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
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
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
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
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
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
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}, {
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{},
	}, {
		-2305843009213693953,
		-1,
		[]ReplicaInfo{},
	}, {
		-1,
		2305843009213693951,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
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
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
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
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{},
	}, {
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{},
	}}

	tablets = tablets.AddTabletToTabletsList(&TabletInfo{
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
		-8611686018427387905,
		-7917529027641081857,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		-6917529027641081857,
		-4611686018427387905,
		[]ReplicaInfo{{removed_host_id, 9}, {tests.RandomUUID(), 8}, {tests.RandomUUID(), 3}},
	}, {
		-4611686018427387905,
		-2305843009213693953,
		[]ReplicaInfo{{tests.RandomUUID(), 9}, {removed_host_id, 8}, {tests.RandomUUID(), 3}},
	}}

	tablets = tablets.RemoveTabletsWithHost(removed_host_id)

	tests.AssertEqual(t, "TabletsList length", 1, len(tablets))
}
