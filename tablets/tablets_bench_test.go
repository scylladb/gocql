//go:build unit
// +build unit

package tablets

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
)

const tabletsCountMedium = 1500

// BenchmarkFindReplicasUnsafeForToken measures the pure lookup+replica-return
// path for a prepopulated CowTabletList.
func BenchmarkFindReplicasUnsafeForToken(b *testing.B) {
	for _, numTablets := range []int{1500, 10000} {
		b.Run(fmt.Sprintf("Tablets%d", numTablets), func(b *testing.B) {
			const rf = 3
			const hostsCount = 6
			hosts := GenerateHostUUIDs(hostsCount)
			tl := NewCowTabletList()
			defer tl.Close()

			tl.BulkAddTablets(createTablets("ks", "tbl", hosts, rf, numTablets, int64(numTablets)))
			tl.Flush()
			runtime.GC()
			b.ResetTimer()
			b.ReportAllocs()

			rnd := getThreadSafeRnd()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					token := rnd.Int63()
					replicas := tl.FindReplicasUnsafeForToken("ks", "tbl", token)
					if len(replicas) != rf {
						// Token may fall in a gap; that's fine for benchmarking.
						_ = replicas
					}
				}
			})
		})
	}
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

func BenchmarkCowTabletListGCShape(b *testing.B) {
	const (
		rf                = 3
		hostsCount        = 16
		tabletsPerTable   = 100000
		tokenRangeCount   = int64(tabletsPerTable)
		lookupBatchSize   = 2048
		gcEveryIterations = 8
	)

	oldGCPercent := debug.SetGCPercent(10)
	defer debug.SetGCPercent(oldGCPercent)

	hosts := GenerateHostUUIDs(hostsCount)
	cl := NewCowTabletList()
	defer cl.Close()

	cl.BulkAddTablets(createTablets("ks", "tbl", hosts, rf, tabletsPerTable, tokenRangeCount))
	cl.Flush()

	runtime.GC()
	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < lookupBatchSize; j++ {
			token := int64((i*lookupBatchSize + j) % tabletsPerTable)
			_, _ = cl.FindTabletForToken("ks", "tbl", token)
		}
		if i%gcEveryIterations == 0 {
			runtime.GC()
		}
	}
	b.StopTimer()

	runtime.GC()
	runtime.GC()
	runtime.KeepAlive(cl)
	runtime.ReadMemStats(&after)

	b.ReportMetric(float64(after.HeapObjects), "heap_objects")
	b.ReportMetric(float64(after.HeapAlloc), "heap_bytes")
	b.ReportMetric(float64(after.NumGC-before.NumGC), "gc_cycles")
	b.ReportMetric(float64(after.PauseTotalNs-before.PauseTotalNs), "gc_pause_ns")
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
	hosts := GenerateHostUUIDs(hostsCount)
	targetKS := "kstarget"
	targetTable := "ttarget"
	removeKs := "ksremove"
	removeTable := "tremove"
	repGen := NewReplicaSetGenerator(hosts, rf)
	readyTablets := createTablets(removeKs, removeTable, hosts, rf, totalTablets, tokenRangeCount64)
	b.SetParallelism(parallelism)
	tl := NewCowTabletList()
	defer tl.Close()
	rnd := getThreadSafeRnd()
	opID := atomic.Int64{}

	if prepopulate {
		tl.BulkAddTablets(createTablets(targetKS, targetTable, hosts, rf, totalTablets, tokenRangeCount64))
	}

	for i := 0; i < extraTables; i++ {
		tl.BulkAddTablets(createTablets(targetKS, fmt.Sprintf("table-%d", i), hosts, rf, totalTablets, tokenRangeCount64))
	}

	tl.Flush()
	runtime.GC()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := opID.Add(1)
			token := rnd.Int63()
			tablet, ok := tl.FindTabletForToken(targetKS, targetTable, token)
			if !ok || tablet.lastToken < token || tablet.firstToken > token {
				// If there is no tablet for token, emulate update, same way it is usually happening
				firstToken := (token / tokenRangeCount64) * tokenRangeCount64
				lastToken := firstToken + tokenRangeCount64
				tl.AddTablet(TabletInfo{
					keyspaceName: targetKS,
					tableName:    targetTable,
					firstToken:   firstToken,
					lastToken:    lastToken,
					replicas:     repGen.Next(),
				})
			}
			if ratios.opRemoveTable == 0 || ((ratios.opRemoveTable != -1) && id%ratios.opRemoveTable == 0) {
				tl.BulkAddTablets(readyTablets)
				tl.RemoveTabletsWithTable(targetKS, removeTable)
			}
			if ratios.opRemoveKeyspace == 0 || ((ratios.opRemoveKeyspace != -1) && id%ratios.opRemoveKeyspace == 0) {
				tl.BulkAddTablets(readyTablets)
				tl.RemoveTabletsWithKeyspace(removeKs)
			}
			if ratios.opRemoveHost == 0 || ((ratios.opRemoveHost != -1) && id%ratios.opRemoveHost == 0) {
				tl.RemoveTabletsWithHost(hosts[rnd.Intn(len(hosts))])
			}
		}
	})
}
