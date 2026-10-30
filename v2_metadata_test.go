package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"github.com/scylladb/gocql/v2/tablets"
	"testing"
	"time"
)

func TestV2MetadataSnapshotsDetachNestedSchema(t *testing.T) {
	column := &ColumnMetadata{Name: "id", Type: "int", Index: ColumnIndexMetadata{Options: map[string]any{"nested": map[string]any{"bytes": []byte{1}}}}}
	ks := &KeyspaceMetadata{Name: "ks", StrategyOptions: map[string]any{"replication_factor": "1"}, Tables: map[string]*TableMetadata{"t": {Columns: map[string]*ColumnMetadata{"id": column}, PartitionKey: []*ColumnMetadata{column}, Extensions: map[string]any{"bytes": []byte{2}}, Options: TableMetadataOptions{CDC: map[string]string{"enabled": "true"}}}}, Types: map[string]*TypeMetadata{"u": {FieldNames: []string{"a"}, FieldTypes: []string{"int"}}}, Functions: map[string]*FunctionMetadata{"f": {ArgumentNames: []string{"a"}, ArgumentTypes: []string{"int"}}}}
	m := routing.NewMetadataSnapshot(routing.SnapshotState{InternalGeneration: 8, InternalKeyspaces: map[string]*KeyspaceMetadata{"ks": cloneV2Keyspace(ks)}, InternalHosts: []HostSnapshot{{Tokens: []int64{1}}}})
	ks.Tables["t"].Columns["id"].Name = "mutated"
	ks.Tables["t"].Extensions["bytes"].([]byte)[0] = 9
	first, _ := m.Keyspace("ks")
	if first.Tables["t"].Columns["id"].Name != "id" || first.Tables["t"].Extensions["bytes"].([]byte)[0] != 2 {
		t.Fatal("snapshot retained mutable schema")
	}
	first.Types["u"].FieldNames[0] = "changed"
	first.Functions["f"].ArgumentNames[0] = "changed"
	first.Tables["t"].PartitionKey[0].Index.Options["nested"].(map[string]any)["bytes"].([]byte)[0] = 9
	second, _ := m.Keyspace("ks")
	if second.Types["u"].FieldNames[0] != "a" || second.Functions["f"].ArgumentNames[0] != "a" || second.Tables["t"].Columns["id"].Index.Options["nested"].(map[string]any)["bytes"].([]byte)[0] != 1 {
		t.Fatal("getter leaked mutable snapshot")
	}
	hosts := m.Hosts()
	hosts[0].Tokens[0] = 9
	if m.Hosts()[0].Tokens[0] != 1 {
		t.Fatal("host tokens alias")
	}
}
func TestV2MetadataRetainedAcrossPublicationAndClose(t *testing.T) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		return opaqueStorageValue
	}()

	first := s.Metadata()
	implCluster.InternalSessionPublishV2Metadata(s)
	second := s.Metadata()
	if first == second || second.Generation() <= first.Generation() {
		t.Fatal("publication did not advance")
	}
	implCluster.InternalSessionCloseV2Peripheral(s)
	if first.Generation() == 0 {
		t.Fatal("retained snapshot invalidated")
	}
}

type panicLifecycle struct{}

func (panicLifecycle) ObserveLifecycle(LifecycleEvent) { panic("observer failure") }
func TestV2LifecyclePanicContained(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuex := new(Session)
		return opaqueStorageValuex
	}()

	p := implCluster.InternalSessionPeripheral(s)
	p.InternalConfig.LifecycleObserver = panicLifecycle{}
	implCluster.InternalSessionObserveV2Lifecycle(s, LifecycleEvent{Kind: "test", At: time.Now()})
	implCluster.InternalSessionCloseV2Peripheral(s)
}

func TestV2SchemaValidationRejectsBrokenIdentityAndOrdering(t *testing.T) {
	ks := &KeyspaceMetadata{Name: "ks"}
	tables := []TableMetadata{{Name: "t"}}
	columns := []ColumnMetadata{{Table: "t", Name: "p", Type: "int", Kind: ColumnPartitionKey, ComponentIndex: 0}}
	check := func(cols []ColumnMetadata, types []TypeMetadata) error {
		return validateV2SchemaParts(ks, tables, cols, types, nil, nil, nil, nil)
	}
	if err := check(columns, nil); err != nil {
		t.Fatal(err)
	}
	malformed := append([]ColumnMetadata(nil), columns...)
	malformed[0].ComponentIndex = 2
	if err := check(malformed, nil); err == nil {
		t.Fatal("key gap accepted")
	}
	malformed = append(columns, columns[0])
	if err := check(malformed, nil); err == nil {
		t.Fatal("duplicate identity accepted")
	}
	types := []TypeMetadata{{Name: "a", FieldNames: []string{"b"}, FieldTypes: []string{"frozen<b>"}}, {Name: "b", FieldNames: []string{"a"}, FieldTypes: []string{"frozen<a>"}}}
	if err := check(columns, types); err == nil {
		t.Fatal("UDT cycle accepted")
	}
	types = []TypeMetadata{{Name: "a", FieldNames: []string{"x", "x"}, FieldTypes: []string{"int", "int"}}}
	if err := check(columns, types); err == nil {
		t.Fatal("duplicate UDT field accepted")
	}
}
func TestV2SchemaTypeSyntax(t *testing.T) {
	for _, typ := range []string{"int", "empty", "map<text, frozen<tuple<int, text>>>", "vector<float, 3>", `frozen<"quoted-type">`} {
		if _, err := validateV2CQLType("ks", typ, map[string]bool{"quoted-type": true}); err != nil {
			t.Fatal(typ, err)
		}
	}
	for _, typ := range []string{"map<int>", "list<>", "tuple<int", "vector<float, 0>", "vector<float, 65536>", "int garbage", "frozen<missing>", "map<int,text>>"} {
		if _, err := validateV2CQLType("ks", typ, nil); err == nil {
			t.Fatal("invalid type accepted", typ)
		}
	}
}
func TestV2MetricsPercentilesAndSchemaGeneration(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexx := new(Session)
		return opaqueStorageValuexx
	}()

	for i := 1; i <= 100; i++ {
		implCluster.InternalSessionRecordV2Latency(s, time.Duration(i)*time.Millisecond)
	}
	implCluster.InternalSessionRecordV2Orphan(s)
	implCluster.
		// Production publishes the bounded percentile summary once per second.
		InternalSessionPeripheral(s).InternalPublishLatencySummary()
	m := s.Metrics()
	if m.LatencyP50 != 50*time.Millisecond || m.LatencyP95 != 95*time.Millisecond || m.LatencyP99 != 99*time.Millisecond || m.Orphans != 1 {
		t.Fatal(m)
	}
	generation := implCluster.InternalSessionTypedSchemaGeneration(s)
	implCluster.InternalSessionPublishV2Metadata(s)
	if implCluster.InternalSessionTypedSchemaGeneration(s) != generation {
		t.Fatal("topology invalidated prepared schema")
	}
}

func TestV2TabletFeedbackOverflowResyncAndEpoch(t *testing.T) {
	s := func() *Session {
		opaqueStorageValuexxx := new(Session)
		return opaqueStorageValuexxx
	}()
	implCluster.SessionData(s).InternalMetadataDescriber = newMetadataDescriber(s)
	defer implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.Close()
	defer implCluster.InternalSessionCloseV2Peripheral(s)
	implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalKeyspaceMetadata.InternalSet("ks", &KeyspaceMetadata{Name: "ks", Tables: map[string]*TableMetadata{"t": {Name: "t"}}})
	p := implCluster.InternalSessionPeripheral(s)
	p.InternalFeedbackOnce.Do(func() {}) // Hold the consumer while filling the bounded queue.
	sub, err := s.Subscribe(EventFilter{Kinds: []EventKind{OverflowEvent}, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	for i := 0; i < 1025; i++ {
		hint, err := tablets.NewTabletInfo("ks", "t", int64(i), int64(i+1), []tablets.ReplicaInfo{tablets.NewReplicaInfo(tablets.HostUUID{1}, 0)})
		if err != nil {
			t.Fatal(err)
		}
		implCluster.InternalSessionApplyV2TabletHint(s, hint)
	}
	p.InternalFeedbackMu.Lock()
	if len(p.InternalFeedback) > 1024 || !p.InternalFeedbackResync {
		t.Fatal("feedback did not bound and mark overflow")
	}
	p.InternalFeedbackMu.Unlock()
	// A newer schema event must win over every already queued hint.
	p.InternalMu.Lock()
	p.InternalSchemaEpoch["ks"]++
	p.InternalMu.Unlock()
	go implCluster.InternalSessionRunV2TabletFeedback(s, p)
	select {
	case event := <-sub.Events():
		if event.Kind != OverflowEvent || event.Change != "TABLET_RESYNC" {
			t.Fatal(event)
		}
	case <-time.After(time.Second):
		t.Fatal("feedback resync was not published")
	}
	if got := implCluster.MetadataData(implCluster.SessionData(s).InternalMetadataDescriber.InternalMetadata).InternalTabletsMetadata.GetTableTablets("ks", "t"); len(got) != 0 {
		t.Fatal("stale hint restored tablet", got)
	}
	snapshot := s.Metadata()
	if _, known := routing.InternalMetadataSnapshotTabletRoot(snapshot, metadataTable{InternalKeyspace: "ks", InternalTable: "t"}); !known {
		t.Fatal("resync lost tablet cache-miss marker")
	}
	if replicas, err := snapshot.Replicas("ks", "t", 4); err != nil || len(replicas) != 0 {
		t.Fatal("tablet cache miss invented vnode replicas", replicas, err)
	}
}
