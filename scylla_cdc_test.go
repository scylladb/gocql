//go:build integration
// +build integration

package gocql

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/events"
)

// TestCDCTableMetadata verifies that CDC log tables appear in session metadata
// when a base table is created with CDC enabled, and that CDC options are
// correctly reflected in the metadata for various scenarios.
func TestCDCTableMetadata(t *testing.T) {
	if *flagDistribution != "scylla" {
		t.Skip("CDC is a ScyllaDB-specific feature")
	}

	cluster := createCluster()
	cluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	const ks = "gocql_test"
	waitForSchemaRefresh := func() { time.Sleep(2 * time.Second) }

	t.Run("create_table_with_cdc_enabled", func(t *testing.T) {
		table := "tbl_cdc_basic"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata for base table failed: %v", err)
		}

		// Base table should have CDC extension set
		if _, ok := tm.Extensions["cdc"]; !ok {
			t.Error("expected 'cdc' key in base table Extensions")
		}

		// CDC log table should exist in keyspace metadata
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table %q in keyspace metadata, got tables: %v", cdcLogTable, tableNames(km.Tables))
		}

		// CDC log table should have the CDC partitioner
		logTM := km.Tables[cdcLogTable]
		if logTM.Options.Partitioner != scyllaCDCPartitionerFullName {
			t.Errorf("expected CDC log table partitioner %q, got %q", scyllaCDCPartitionerFullName, logTM.Options.Partitioner)
		}
	})

	t.Run("create_table_without_cdc", func(t *testing.T) {
		table := "tbl_cdc_none"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
			t.Fatalf("create table: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)

		// Base table should not have CDC extension
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata failed: %v", err)
		}
		if _, ok := tm.Extensions["cdc"]; ok {
			t.Errorf("expected no 'cdc' extension on table without CDC, got extensions: %v", extensionKeys(tm.Extensions))
		}

		// CDC log table should NOT exist
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; ok {
			t.Errorf("CDC log table %q should not exist for table created without CDC", cdcLogTable)
		}
	})

	t.Run("enable_cdc_via_alter_table", func(t *testing.T) {
		table := "tbl_cdc_alter_on"
		cdcLogTable := table + "_scylla_cdc_log"

		// Create table without CDC
		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int)", ks, table)); err != nil {
			t.Fatalf("create table: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; ok {
			t.Fatalf("CDC log table should not exist before ALTER")
		}

		// Enable CDC via ALTER TABLE
		if err := createTable(session, fmt.Sprintf(
			"ALTER TABLE %s.%s WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("alter table to enable cdc: %v", err)
		}

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)

		// Base table should now have CDC extension
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata failed: %v", err)
		}
		if _, ok := tm.Extensions["cdc"]; !ok {
			t.Error("expected 'cdc' extension after enabling CDC via ALTER")
		}

		// CDC log table should now exist
		km, err = session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table %q after ALTER, got tables: %v", cdcLogTable, tableNames(km.Tables))
		}
	})

	t.Run("disable_cdc_via_alter_table", func(t *testing.T) {
		table := "tbl_cdc_alter_off"
		cdcLogTable := table + "_scylla_cdc_log"

		// Create table WITH CDC
		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table before disabling CDC")
		}

		// Disable CDC via ALTER TABLE
		if err := createTable(session, fmt.Sprintf(
			"ALTER TABLE %s.%s WITH cdc = {'enabled': false}", ks, table)); err != nil {
			t.Fatalf("alter table to disable cdc: %v", err)
		}

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)

		// After disabling CDC, the CDC log table should eventually be removed.
		// ScyllaDB may keep it briefly, but a fresh metadata load should not include it.
		// The base table's CDC extension may still be present with enabled=false.
		km, err = session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}

		if _, ok := km.Tables[cdcLogTable]; ok {
			// CDC log table still present — ScyllaDB may retain it briefly.
			// Verify that the CDC log table partitioner is no longer the CDC partitioner,
			// which would indicate it's been properly decommissioned.
			logTM := km.Tables[cdcLogTable]
			t.Logf("CDC log table %q still present after disabling CDC (partitioner=%q)", cdcLogTable, logTM.Options.Partitioner)
		}

		// Base table should still exist
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata failed: %v", err)
		}
		if tm.Name != table {
			t.Errorf("expected table name %q, got %q", table, tm.Name)
		}
	})

	t.Run("cdc_with_preimage_and_ttl", func(t *testing.T) {
		table := "tbl_cdc_opts"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true, 'preimage': true, 'ttl': 3600}", ks, table)); err != nil {
			t.Fatalf("create table with cdc options: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata failed: %v", err)
		}

		// Base table should have CDC extension
		if _, ok := tm.Extensions["cdc"]; !ok {
			t.Fatal("expected 'cdc' extension on table with CDC options")
		}

		// CDC log table should exist
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table %q, got tables: %v", cdcLogTable, tableNames(km.Tables))
		}
	})

	t.Run("alter_cdc_options", func(t *testing.T) {
		table := "tbl_cdc_alter_opts"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true, 'ttl': 3600}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata failed: %v", err)
		}
		if _, ok := tm.Extensions["cdc"]; !ok {
			t.Fatal("expected 'cdc' extension before alter")
		}
		cdcExtBefore := tm.Extensions["cdc"]

		// Alter CDC options - change TTL and enable preimage
		if err := createTable(session, fmt.Sprintf(
			"ALTER TABLE %s.%s WITH cdc = {'enabled': true, 'ttl': 7200, 'preimage': true}", ks, table)); err != nil {
			t.Fatalf("alter table cdc options: %v", err)
		}

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		tm, err = session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata after alter failed: %v", err)
		}
		if _, ok := tm.Extensions["cdc"]; !ok {
			t.Fatal("expected 'cdc' extension after alter")
		}
		cdcExtAfter := tm.Extensions["cdc"]

		// The CDC extension should have changed after ALTER
		if fmt.Sprintf("%v", cdcExtBefore) == fmt.Sprintf("%v", cdcExtAfter) {
			t.Error("expected CDC extension to change after ALTER TABLE with different options")
		}

		// CDC log table should still exist
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table %q after alter, got tables: %v", cdcLogTable, tableNames(km.Tables))
		}
	})

	t.Run("cdc_log_table_discoverable_without_cache", func(t *testing.T) {
		// Reproduces the scylla-cdc-go failure path:
		//   1. Session connects → gocql caches the keyspace (no CDC tables yet)
		//   2. CREATE TABLE ... WITH cdc creates both base + log table
		//   3. session.TableMetadata("..._scylla_cdc_log") — table is not in
		//      the cache and not in tablesInvalidated
		//
		// GetTable must fetch the unknown table from the server. Without the
		// fix, the tablesInvalidated gate returns ErrNotFound immediately.
		table := "tbl_cdc_nocache"
		cdcLogTable := table + "_scylla_cdc_log"

		// Use a fresh session so the keyspace cache is populated at connect
		// time, before the CDC table exists. This is what scylla-cdc-go does:
		// cluster.CreateSession() first, then CREATE TABLE ... WITH cdc.
		freshCluster := createCluster()
		freshCluster.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
		freshCluster.Keyspace = ks
		freshSession, err := freshCluster.CreateSession()
		if err != nil {
			t.Fatalf("create fresh session: %v", err)
		}
		defer freshSession.Close()

		// Subscribe to all schema change table events to log what ScyllaDB sends.
		var collectedEvents []string
		var eventsMu sync.Mutex
		sub := freshSession.SubscribeToEvents("cdc-test-schema-events", 100, func(ev events.Event) bool {
			return ev.Type() == events.ClusterEventTypeSchemaChangeTable
		})
		defer sub.Stop()
		go func() {
			for ev := range sub.Events() {
				eventsMu.Lock()
				collectedEvents = append(collectedEvents, ev.String())
				eventsMu.Unlock()
			}
		}()

		// Create the CDC-enabled table using raw Exec — no awaitSchemaAgreement
		// so we can race against the event debouncer.
		if err := freshSession.Query(fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)).Exec(); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}
		defer freshSession.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		// No waitForSchemaRefresh() — we intentionally skip the sleep to
		// test whether the event has been processed by the time we query.

		// Retry up to 3 times with backoff. The event debouncer has a 1s
		// window, so the CDC log table may not be in tablesInvalidated yet.
		var tm *TableMetadata
		for attempt := 1; attempt <= 3; attempt++ {
			tm, err = freshSession.TableMetadata(ks, cdcLogTable)
			if err == nil {
				break
			}
			t.Logf("attempt %d: TableMetadata failed: %v (retrying in %v)", attempt, err, time.Duration(attempt)*time.Second)
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		// Dump all schema change events received from the cluster.
		eventsMu.Lock()
		t.Logf("Schema change table events received from cluster:")
		for i, ev := range collectedEvents {
			t.Logf("  [%d] %s", i, ev)
		}
		eventsMu.Unlock()

		if err != nil {
			t.Fatalf("TableMetadata for CDC log table (cold) failed after 3 attempts: %v", err)
		}
		if tm.Name != cdcLogTable {
			t.Errorf("expected table name %q, got %q", cdcLogTable, tm.Name)
		}
		if tm.Options.Partitioner != scyllaCDCPartitionerFullName {
			t.Errorf("expected partitioner %q, got %q", scyllaCDCPartitionerFullName, tm.Options.Partitioner)
		}
	})

	t.Run("cdc_log_table_has_expected_columns", func(t *testing.T) {
		table := "tbl_cdc_cols"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		tm, err := session.TableMetadata(ks, cdcLogTable)
		if err != nil {
			t.Fatalf("TableMetadata for CDC log table failed: %v", err)
		}

		// CDC log tables should have special CDC columns
		expectedCDCColumns := []string{"cdc$stream_id", "cdc$time", "cdc$operation"}
		for _, col := range expectedCDCColumns {
			if _, ok := tm.Columns[col]; !ok {
				t.Errorf("expected CDC log column %q, got columns: %v", col, columnNames(tm.Columns))
			}
		}
	})

	t.Run("drop_base_table_removes_cdc_log", func(t *testing.T) {
		table := "tbl_cdc_drop"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table before drop")
		}

		// Drop the base table
		if err := createTable(session, fmt.Sprintf("DROP TABLE %s.%s", ks, table)); err != nil {
			t.Fatalf("drop table: %v", err)
		}

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		km, err = session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata after drop failed: %v", err)
		}

		// Both the base table and CDC log table should be gone
		if _, ok := km.Tables[table]; ok {
			t.Errorf("base table %q should not exist after DROP", table)
		}
		if _, ok := km.Tables[cdcLogTable]; ok {
			t.Errorf("CDC log table %q should not exist after dropping base table", cdcLogTable)
		}
	})

	t.Run("drop_base_table_cdc_log_not_stale_in_cache", func(t *testing.T) {
		// Verifies that when a base table with CDC is dropped, the CDC log
		// table does not remain as a stale entry in the metadata cache.
		// This test does NOT manually invalidate the keyspace — it relies on
		// schema change events to propagate and invalidate the cache.
		table := "tbl_cdc_cache"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}

		waitForSchemaRefresh()

		// Populate the cache by reading metadata
		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table in cache before drop")
		}

		// Drop the base table — ScyllaDB should send schema change events
		// for both the base table and the CDC log table
		if err := createTable(session, fmt.Sprintf("DROP TABLE %s.%s", ks, table)); err != nil {
			t.Fatalf("drop table: %v", err)
		}

		// Wait for schema events to propagate and invalidate the cache
		waitForSchemaRefresh()

		// Do NOT manually invalidate — rely on schema event propagation
		km, err = session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata after drop failed: %v", err)
		}

		if _, ok := km.Tables[table]; ok {
			t.Errorf("base table %q should not be in cache after DROP", table)
		}
		if _, ok := km.Tables[cdcLogTable]; ok {
			t.Errorf("CDC log table %q is stale in cache after dropping base table", cdcLogTable)
		}
	})

	t.Run("scyllaIsCdcTable_detection", func(t *testing.T) {
		table := "tbl_cdc_detect"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		// Force metadata refresh
		session.metadataDescriber.invalidateKeyspaceSchema(ks)

		// The CDC log table should be detected as a CDC table
		isCdc, err := scyllaIsCdcTable(session, ks, cdcLogTable)
		if err != nil {
			t.Fatalf("scyllaIsCdcTable failed: %v", err)
		}
		if !isCdc {
			t.Error("expected scyllaIsCdcTable to return true for CDC log table")
		}

		// The base table should NOT be detected as a CDC table
		isCdc, err = scyllaIsCdcTable(session, ks, table)
		if err != nil {
			t.Fatalf("scyllaIsCdcTable for base table failed: %v", err)
		}
		if isCdc {
			t.Error("expected scyllaIsCdcTable to return false for base table")
		}
	})

	t.Run("cdc_with_postimage", func(t *testing.T) {
		table := "tbl_cdc_postimg"
		cdcLogTable := table + "_scylla_cdc_log"

		if err := createTable(session, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk int PRIMARY KEY, v int) WITH cdc = {'enabled': true, 'postimage': true}", ks, table)); err != nil {
			t.Fatalf("create table with cdc postimage: %v", err)
		}
		defer session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", ks, table)).Exec()

		waitForSchemaRefresh()

		session.metadataDescriber.invalidateKeyspaceSchema(ks)
		tm, err := session.TableMetadata(ks, table)
		if err != nil {
			t.Fatalf("TableMetadata failed: %v", err)
		}
		if _, ok := tm.Extensions["cdc"]; !ok {
			t.Error("expected 'cdc' extension on table with postimage")
		}

		// CDC log table should exist
		km, err := session.KeyspaceMetadata(ks)
		if err != nil {
			t.Fatalf("KeyspaceMetadata failed: %v", err)
		}
		if _, ok := km.Tables[cdcLogTable]; !ok {
			t.Fatalf("expected CDC log table %q, got tables: %v", cdcLogTable, tableNames(km.Tables))
		}
	})
}

func extensionKeys(extensions map[string]interface{}) []string {
	keys := make([]string, 0, len(extensions))
	for k := range extensions {
		keys = append(keys, k)
	}
	return keys
}
