package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// Decode the response independently of the emulator's request reader.
func fixtureRows(t *testing.T, b []byte) (next []byte, values [][][]byte) {
	t.Helper()
	take := func(n int) []byte {
		if n < 0 || n > len(b) {
			t.Fatal("truncated fixture")
		}
		v := b[:n]
		b = b[n:]
		return v
	}
	int32value := func() int { return int(int32(binary.BigEndian.Uint32(take(4)))) }
	cell := func() []byte {
		n := int32value()
		if n < 0 {
			return nil
		}
		return take(n)
	}
	if int32value() != 2 {
		t.Fatal("expected ROWS")
	}
	flags, columns := int32value(), int32value()
	if flags&4 == 0 || columns != 3 {
		t.Fatal("expected three columns, metadata omitted")
	}
	if flags&2 != 0 {
		next = bytes.Clone(cell())
	}
	count := int32value()
	for i := 0; i < count; i++ {
		var row [][]byte
		for j := 0; j < columns; j++ {
			row = append(row, bytes.Clone(cell()))
		}
		values = append(values, row)
	}
	if len(b) != 0 {
		t.Fatal("trailing fixture bytes")
	}
	return next, values
}

func TestFixtureOverwriteInvalidatesCachedResponse(t *testing.T) {
	k := int32(9)
	payloads[0][variant(k, 0)] = bytes.Repeat([]byte{42}, 1024)
	points[k] = dataset(false, k, 0, 1, true, nil)
	p := params{values: [][]byte{i32(int(k))}, size: 32, skip: true}
	_, b := query("SELECT k,r,p FROM bench_compare.point WHERE k=?", p, 0)
	_, got := fixtureRows(t, b)
	if len(got) != 1 || len(got[0][2]) != 1024 {
		t.Fatal("canonical point fixture")
	}
	changed := []byte("written payload must supersede precomputed response")
	_, ack := query("INSERT INTO bench_compare.point (k,r,p) VALUES (?,?,?)", params{values: [][]byte{i32(int(k)), i32(0), changed}}, 0)
	if !bytes.Equal(ack, []byte{0, 0, 0, 1}) {
		t.Fatal("expected VOID acknowledgment")
	}
	_, b = query("SELECT k,r,p FROM bench_compare.point WHERE k=?", p, 0)
	_, got = fixtureRows(t, b)
	if len(got) != 1 || !bytes.Equal(got[0][2], changed) {
		t.Fatal("read ignored acknowledged overwrite")
	}
	query("INSERT INTO bench_compare.point (k,r,p) VALUES (?,?,?)", params{values: [][]byte{i32(int(k)), i32(0), payloads[0][variant(k, 0)]}}, 0)
	_, b = query("SELECT k,r,p FROM bench_compare.point WHERE k=?", p, 0)
	_, got = fixtureRows(t, b)
	if !bytes.Equal(got[0][2], payloads[0][variant(k, 0)]) {
		t.Fatal("canonical restoration")
	}
}

func TestFixtureFourPopulatedPagesAndEmptyTerminal(t *testing.T) {
	k := int32(10)
	for r := int32(0); r < 128; r++ {
		payloads[1][variant(k, r)] = bytes.Repeat([]byte{byte(r)}, 4096)
	}
	for page := 0; page < 5; page++ {
		var next []byte
		if page < 4 {
			next = i32((page + 1) * 32)
		}
		pages[k][page] = dataset(true, k, page*32, min(128, (page+1)*32), true, next)
	}
	p := params{values: [][]byte{i32(int(k))}, size: 32, skip: true}
	count := 0
	for page := 0; page < 5; page++ {
		_, body := query("SELECT k,r,p FROM bench_compare.paged WHERE k=?", p, 0)
		next, values := fixtureRows(t, body)
		expected := 32
		if page == 4 {
			expected = 0
		}
		if len(values) != expected {
			t.Fatalf("page %d rows %d", page, len(values))
		}
		for _, row := range values {
			if int(binary.BigEndian.Uint32(row[0])) != int(k) || int(binary.BigEndian.Uint32(row[1])) != count || len(row[2]) != 4096 {
				t.Fatal("incorrect row identity")
			}
			count++
		}
		if page < 4 {
			if len(next) != 4 || int(binary.BigEndian.Uint32(next)) != (page+1)*32 {
				t.Fatal("paging token")
			}
		} else if next != nil {
			t.Fatal("terminal page has continuation")
		}
		p.page = next
	}
	if count != 128 {
		t.Fatal("incorrect partition size")
	}
}
