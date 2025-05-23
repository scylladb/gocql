package tablets

import (
	"fmt"
	"github.com/gocql/gocql/internal/tests"
	"math"
	"runtime"
	"sync/atomic"
	"testing"
)

const tabletsCountMedium = 1500

func BenchmarkTabletInfoList(b *testing.B) {
	hosts := tests.GenerateHostNames(3)
	tlist := createTablets(hosts, 2, tabletsCountMedium, tabletsCountMedium)
	tlist2 := createTablets(hosts, 2, tabletsCountMedium, tabletsCountMedium)
	tlist3 := createTablets(hosts, 2, tabletsCountMedium, tabletsCountMedium)
	tlist = append(tlist, tlist2...)
	tlist = append(tlist, tlist3...)

	b.ResetTimer()

	b.Run("FindTabletForToken", func(b *testing.B) {
		tokens := tests.RandomTokens(getThreadSafeRnd(), b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tlist.FindTabletForToken(tokens[i])
		}
	})

	b.Run("AddTabletToTabletsList", func(b *testing.B) {
		b.Run("FromEmpty", func(b *testing.B) {
			runtime.GC()
			var tlist TabletInfoList
			indexes := tests.ShuffledIndexes(getRnd(), b.N)
			multiplier := int64(math.MaxUint64 / uint64(b.N))
			replicas := []ReplicaInfo{{hostId: "h1"}, {hostId: "h2"}, {hostId: "h3"}}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				token := int64(indexes[i]) * multiplier
				tlist = tlist.AddTabletToTabletsList(&TabletInfo{
					firstToken: token - multiplier,
					lastToken:  token,
					replicas:   replicas,
				})
			}
		})

		b.Run("NewTable", func(b *testing.B) {
			runtime.GC()
			tl := createTablets(tests.GenerateHostNames(3), 2, tabletsCountMedium, tabletsCountMedium)
			indexes := tests.ShuffledIndexes(getRnd(), b.N)
			multiplier := int64(math.MaxUint64 / uint64(b.N))
			replicas := []ReplicaInfo{{hostId: "h1"}, {hostId: "h2"}, {hostId: "h3"}}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				token := int64(indexes[i]) * multiplier
				tl = tl.AddTabletToTabletsList(&TabletInfo{
					firstToken: token - multiplier,
					lastToken:  token,
					replicas:   replicas,
				})
			}
		})
	})
}

type opConfig struct {
	opRemoveKeyspace int64
	opRemoveTable    int64
	opRemoveHost     int64
}

func BenchmarkCowTabletList(b *testing.B) {
	const (
		rf = 3
	)
	b.Run("Parallel-10", func(b *testing.B) {
		runCowTabletListTestSuit(b, "ManyTables", 6, 10, rf, 1500, 5)
		runCowTabletListTestSuit(b, "SingleTable", 6, 10, rf, 1500, 0)
	})

	b.Run("SingleThread", func(b *testing.B) {
		runCowTabletListTestSuit(b, "ManyTables", 6, 1, rf, 1500, 5)
		runCowTabletListTestSuit(b, "SingleTable", 6, 1, rf, 1500, 0)
	})
}

func runCowTabletListTestSuit(b *testing.B, name string, hostsCount, parallelism, rf, totalTablets, extraTables int) {
	b.Run(name, func(b *testing.B) {

		b.Run("New", func(b *testing.B) {
			runSingleCowTabletListTest(b, hostsCount, parallelism, rf, totalTablets, extraTables, false, opConfig{
				opRemoveKeyspace: -1,
				opRemoveHost:     -1,
				opRemoveTable:    -1,
			})
		})

		b.Run("Prepopulated", func(b *testing.B) {
			runSingleCowTabletListTest(b, hostsCount, parallelism, rf, totalTablets, extraTables, true, opConfig{
				opRemoveKeyspace: -1,
				opRemoveHost:     -1,
				opRemoveTable:    -1,
			})
		})

		b.Run("RemoveHost", func(b *testing.B) {
			runSingleCowTabletListTest(b, hostsCount, parallelism, rf, totalTablets, extraTables, true, opConfig{
				opRemoveKeyspace: -1,
				opRemoveTable:    -1,
				opRemoveHost:     1000, // Every 1000 query is remove host, to measure congestion
			})
		})

		b.Run("RemoveTable", func(b *testing.B) {
			runSingleCowTabletListTest(b, hostsCount, parallelism, rf, totalTablets, extraTables, true, opConfig{
				opRemoveKeyspace: -1,
				opRemoveHost:     -1,
				opRemoveTable:    1000, // Every 1000 query is remove table, to measure congestion
			})
		})

		b.Run("RemoveKeyspace", func(b *testing.B) {
			runSingleCowTabletListTest(b, hostsCount, parallelism, rf, totalTablets, extraTables, true, opConfig{
				opRemoveHost:     -1,
				opRemoveTable:    -1,
				opRemoveKeyspace: 1000, // Every 1000 query is remove keyspace, to measure congestion
			})
		})
	})
}

func runSingleCowTabletListTest(b *testing.B, hostsCount, parallelism, rf, totalTablets, extraTables int, prepopulate bool, ratios opConfig) {
	tokenRangeCount64 := int64(totalTablets)
	hosts := tests.GenerateHostNames(hostsCount)
	targetKS := "kstarget"
	targetTable := "ttarget"
	removeKs := "ksremove"
	removeTable := "tremove"
	repGen := NewReplicaSetGenerator(hosts, rf)
	readyTablets := createTablets(hosts, rf, totalTablets, tokenRangeCount64)
	b.SetParallelism(parallelism)
	tl := NewCowTabletList()
	rnd := getThreadSafeRnd()
	opID := atomic.Int64{}

	if prepopulate {
		tl.BulkAddTablets(targetKS, targetTable, createTablets(hosts, rf, totalTablets, tokenRangeCount64))
	}

	for i := 0; i < extraTables; i++ {
		tl.BulkAddTablets(targetKS, fmt.Sprintf("table-%d", i), createTablets(hosts, rf, totalTablets, tokenRangeCount64))
	}

	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := opID.Add(1)
			token := rnd.Int63()
			tablet := tl.FindTabletForToken(targetKS, targetTable, token)
			if tablet == nil || tablet.lastToken > token || tablet.firstToken < token {
				// If there is no tablet for token, emulate update, same way it is usually happening
				firstToken := (token / tokenRangeCount64) * tokenRangeCount64
				lastToken := firstToken + tokenRangeCount64
				tl.AddTablet(targetKS, targetTable, &TabletInfo{
					firstToken: firstToken,
					lastToken:  lastToken,
					replicas:   repGen.Next(),
				})
			}
			if ratios.opRemoveTable == 0 || ((ratios.opRemoveTable != -1) && id%ratios.opRemoveTable == 0) {
				tl.BulkAddTablets(removeKs, removeTable, readyTablets)
				tl.RemoveTabletsWithTableFromTabletsList(targetKS, removeTable)
			}
			if ratios.opRemoveKeyspace == 0 || ((ratios.opRemoveKeyspace != -1) && id%ratios.opRemoveKeyspace == 0) {
				tl.BulkAddTablets(removeKs, removeTable, readyTablets)
				tl.RemoveTabletsWithKeyspace(removeKs)
			}
			if ratios.opRemoveHost == 0 || ((ratios.opRemoveHost != -1) && id%ratios.opRemoveHost == 0) {
				tl.RemoveTabletsWithHost(hosts[rnd.Intn(len(hosts))])
			}
		}
	})
}
