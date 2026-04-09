//go:build unit
// +build unit

package gocql

import (
	"fmt"
	"math"
	"net"
	"runtime"
	"testing"

	"github.com/gocql/gocql/tablets"
)

// setupTabletAwareBench creates a tokenAwareHostPolicy with the given number
// of hosts and tablets, wired up to a mock session with tabletsRoutingV1.
// It returns the policy, session, and a slice of pre-built queries that hit
// different tokens spread across the tablet range.
func setupTabletAwareBench(b *testing.B, numHosts, numTablets, rf int) (HostSelectionPolicy, *Session, []*Query) {
	b.Helper()

	const keyspace = "benchks"
	const table = "benchtbl"

	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }

	policyInternal.getKeyspaceMetadata = func(ks string) (*KeyspaceMetadata, error) {
		return &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": rf,
			},
		}, nil
	}

	// Create hosts with binary UUIDs (matching what tablets use).
	hosts := make([]*HostInfo, numHosts)
	for i := range hosts {
		hosts[i] = &HostInfo{
			hostId:         tUUID(i),
			connectAddress: net.IPv4(10, 0, byte(i>>8), byte(i)),
			tokens:         []string{fmt.Sprintf("%d", int64(math.MinInt64)+int64(i)*100)},
		}
		policy.AddHost(hosts[i])
	}
	policy.SetPartitioner("Murmur3Partitioner")
	policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace})

	// Set up mock session with tablet routing.
	ctrl := &schemaDataMock{knownKeyspaces: map[string][]tableInfo{}}
	s := newSchemaEventTestSession(ctrl, &trackingPolicy{}, "")
	s.useSystemSchema = true
	s.isInitialized = true
	s.tabletsRoutingV1 = true

	// Build tablets covering the full token range, each with `rf` replicas
	// drawn round-robin from the host list.
	step := uint64(math.MaxUint64) / uint64(numTablets)
	firstToken := int64(math.MinInt64)

	tabletList := make(tablets.TabletInfoList, numTablets)
	for i := 0; i < numTablets; i++ {
		lastToken := firstToken + int64(step)
		if i == numTablets-1 {
			lastToken = math.MaxInt64
		}

		reps := make([][]interface{}, rf)
		for r := 0; r < rf; r++ {
			hostIdx := (i + r) % numHosts
			reps[r] = []interface{}{hosts[hostIdx].hostId, 0}
		}
		ti, err := tablets.TabletInfoBuilder{
			KeyspaceName: keyspace,
			TableName:    table,
			FirstToken:   firstToken,
			LastToken:    lastToken,
			Replicas:     reps,
		}.Build()
		if err != nil {
			b.Fatal(err)
		}
		tabletList[i] = ti
		firstToken = lastToken
	}

	s.metadataDescriber.metadata.tabletsMetadata.BulkAddTablets(tabletList)
	s.metadataDescriber.metadata.tabletsMetadata.Flush()

	// Pre-build queries that hit evenly spaced tokens.
	const numQueries = 256
	queries := make([]*Query, numQueries)
	tokenStep := uint64(math.MaxUint64) / uint64(numQueries)
	for i := range queries {
		token := int64(math.MinInt64) + int64(uint64(i)*tokenStep) + 1
		queries[i] = &Query{
			routingInfo: &queryRoutingInfo{
				keyspace:    keyspace,
				table:       table,
				partitioner: fixedInt64Partitioner(token),
			},
			session: s,
		}
		queries[i].getKeyspace = func() string { return keyspace }
		queries[i].routingKey = []byte("key")
	}

	return policy, s, queries
}

// BenchmarkTabletAwarePick benchmarks the full tokenAwareHostPolicy.Pick()
// path with tablet routing, varying the number of hosts in the cluster.
// This measures the O(RF * H) host resolution loop in Pick().
func BenchmarkTabletAwarePick(b *testing.B) {
	for _, numHosts := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("Hosts%d", numHosts), func(b *testing.B) {
			const numTablets = 10000
			const rf = 3
			policy, s, queries := setupTabletAwareBench(b, numHosts, numTablets, rf)
			defer s.Close()

			runtime.GC()
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				qry := queries[i%len(queries)]
				iter := policy.Pick(qry)
				// Consume the first host (happy path — one NextHost call).
				h := iter()
				if h == nil {
					b.Fatal("Pick returned nil on first call")
				}
			}
		})
	}
}

// BenchmarkTabletAwarePickAllReplicas benchmarks exhausting all replicas
// from Pick(), simulating the retry/unhappy path.
func BenchmarkTabletAwarePickAllReplicas(b *testing.B) {
	for _, numHosts := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("Hosts%d", numHosts), func(b *testing.B) {
			const numTablets = 10000
			const rf = 3
			policy, s, queries := setupTabletAwareBench(b, numHosts, numTablets, rf)
			defer s.Close()

			runtime.GC()
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				qry := queries[i%len(queries)]
				iter := policy.Pick(qry)
				// Exhaust all hosts from the iterator.
				for h := iter(); h != nil; h = iter() {
				}
			}
		})
	}
}

// BenchmarkHostIdComparison is a micro-benchmark for isolated host-ID
// comparisons: string==string (current) baseline.
func BenchmarkHostIdComparison(b *testing.B) {
	id1 := "00000000-0000-0000-0000-000000000001"
	id2 := "00000000-0000-0000-0000-000000000001"

	b.Run("StringEqual", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if id1 != id2 {
				b.Fatal("should be equal")
			}
		}
	})

	// Benchmark with UUIDs for comparison (what the proposed change uses).
	uuid1 := UUID{}
	uuid1[15] = 1
	uuid2 := UUID{}
	uuid2[15] = 1

	b.Run("UUIDEqual", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if uuid1 != uuid2 {
				b.Fatal("should be equal")
			}
		}
	})
}
