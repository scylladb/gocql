package lz4

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/testutils"
)

func TestBlobCompressor(t *testing.T) {
	session := testutils.CreateSession(t)
	defer session.Close()

	originalBlob := strings.Repeat("1234567890", 20)

	lz4Compressor, err := NewBlobCompressor([]byte("lz4:"), CompressorSizeLimit(1))
	if err != nil {
		t.Fatal("create lz4 compressor")
	}

	rtBlob := StatsBasedThreadSafeRateEvaluator{}
	rtAscii := StatsBasedThreadSafeRateEvaluator{}
	expectedUUID := gocql.TimeUUID()
	expectedBlob := lz4Compressor.Blob([]byte(originalBlob)).SetRatioStats(&rtBlob)
	expectedText := lz4Compressor.String(originalBlob).SetRatioStats(&rtAscii)
	expectedASCII := lz4Compressor.String(originalBlob).SetRatioStats(&rtAscii)
	expectedVarchar := lz4Compressor.String(originalBlob).SetRatioStats(&rtAscii)

	t.Run("prepare", func(t *testing.T) {
		// TypeVarchar, TypeAscii, TypeBlob, TypeText
		err := testutils.CreateTable(session, `CREATE TABLE gocql_test.test_blob_compressor (
			testuuid       timeuuid PRIMARY KEY,
			testblob       blob,
			testtext       text,
			testascii      ascii,
			testvarchar    varchar,
		)`)
		if err != nil {
			t.Fatal("create table:", err)
		}

		err = session.Query(
			`INSERT INTO gocql_test.test_blob_compressor (testuuid, testblob, testtext, testascii, testvarchar) VALUES (?,?,?,?,?)`,
			expectedUUID, expectedBlob, expectedText, expectedASCII, expectedVarchar,
		).Exec()
		if err != nil {
			t.Fatal("insert:", err)
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("CheckIfCompressed", func(t *testing.T) {
		testMap := make(map[string]interface{})
		if session.Query(`SELECT * FROM test_blob_compressor`).MapScan(testMap) != nil {
			t.Fatal("MapScan failed to work with one row")
		}
		if !lz4Compressor.IsDataCompressed(testMap["testblob"]) {
			t.Errorf("expected blob to be compressed, but it is not: %v", testMap["testblob"])
		}
		if !lz4Compressor.IsDataCompressed(testMap["testtext"]) {
			t.Errorf("expected text to be compressed, but it is not: %v", testMap["testtext"])
		}
		if !lz4Compressor.IsDataCompressed(testMap["testascii"]) {
			t.Errorf("expected text to be compressed, but it is not: %v", testMap["testascii"])
		}
		if !lz4Compressor.IsDataCompressed(testMap["testvarchar"]) {
			t.Errorf("expected text to be compressed, but it is not: %v", testMap["testvarchar"])
		}
	})
	if t.Failed() {
		t.FailNow()
	}

	t.Run("MapScan", func(t *testing.T) {
		uuid := &gocql.UUID{}
		blob := lz4Compressor.Blob(nil).SetRatioStats(&rtBlob)
		text := lz4Compressor.String("").SetRatioStats(&rtAscii)
		ascii := lz4Compressor.String("").SetRatioStats(&rtAscii)
		varchar := lz4Compressor.String("").SetRatioStats(&rtAscii)
		iter := session.Query(`SELECT testuuid, testblob, testtext, testascii, testvarchar FROM test_blob_compressor`).Iter()
		if !iter.MapScan(map[string]interface{}{
			"testuuid":    uuid,
			"testblob":    blob,
			"testtext":    text,
			"testascii":   ascii,
			"testvarchar": varchar,
		}) {
			t.Fatalf("MapScan failed to work with one row: %v", iter.Close())
		}
		if diff := cmp.Diff([]byte(originalBlob), blob.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff(originalBlob, text.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff(originalBlob, ascii.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff(originalBlob, varchar.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
	})

	t.Run("Scan", func(t *testing.T) {
		uuid := &gocql.UUID{}
		blob := lz4Compressor.Blob(nil).SetRatioStats(&rtBlob)
		text := lz4Compressor.String("").SetRatioStats(&rtAscii)
		ascii := lz4Compressor.String("").SetRatioStats(&rtAscii)
		varchar := lz4Compressor.String("").SetRatioStats(&rtAscii)
		iter := session.Query(`SELECT testuuid, testblob, testtext, testascii, testvarchar FROM test_blob_compressor`).Iter()
		if !iter.Scan(uuid, blob, text, ascii, varchar) {
			t.Fatalf("MapScan failed to work with one row: %v", iter.Close())
		}
		if diff := cmp.Diff(expectedUUID.String(), uuid.String()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff([]byte(originalBlob), blob.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff(originalBlob, text.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff(originalBlob, ascii.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}
		if diff := cmp.Diff(originalBlob, varchar.Value()); diff != "" {
			t.Fatal("mismatch in returned map", diff)
		}

	})
}
