// Command emulator implements only the native-v4 benchmark fixtures. It does
// not implement replication, persistence, consistency, or a general CQL engine.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type col struct {
	name string
	typ  []byte
}
type table struct {
	cols []col
	rows [][][]byte
}
type params struct {
	values [][]byte
	page   []byte
	size   int
	skip   bool
}

var requests, failures atomic.Uint64
var payloads [2][256][]byte
var points [100000][]byte
var pages [1024][5][]byte
var storeMu sync.RWMutex
var overrides = map[[3]int32][]byte{}
var prepared sync.Map
var listeners []net.Listener
var active sync.Map
var workers sync.WaitGroup

func u16(n int) []byte    { return binary.BigEndian.AppendUint16(nil, uint16(n)) }
func i32(n int) []byte    { return binary.BigEndian.AppendUint32(nil, uint32(n)) }
func str(s string) []byte { return append(u16(len(s)), s...) }
func blob(b []byte) []byte {
	if b == nil {
		return i32(-1)
	}
	return append(i32(len(b)), b...)
}
func typ(n int) []byte     { return u16(n) }
func text(n string) col    { return col{n, typ(13)} }
func integer(n string) col { return col{n, typ(9)} }
func list(values ...string) []byte {
	b := i32(len(values))
	for _, v := range values {
		b = append(b, blob([]byte(v))...)
	}
	return b
}
func textmap(values ...string) []byte {
	b := i32(len(values) / 2)
	for _, v := range values {
		b = append(b, blob([]byte(v))...)
	}
	return b
}
func uuid(n byte) []byte { b := make([]byte, 16); b[6] = 0x40; b[8] = 0x80; b[15] = n; return b }
func colsBytes(cols []col, ks, name string) []byte {
	b := append(str(ks), str(name)...)
	for _, c := range cols {
		b = append(b, str(c.name)...)
		b = append(b, c.typ...)
	}
	return b
}
func metadata(cols []col, skip bool, page []byte) []byte {
	f := 1
	if len(cols) == 0 {
		f = 0
	}
	if skip {
		f = 4
	}
	if page != nil {
		f |= 2
	}
	b := append(i32(f), i32(len(cols))...)
	if page != nil {
		b = append(b, blob(page)...)
	}
	if !skip && len(cols) > 0 {
		b = append(b, colsBytes(cols, "bench_compare", "fixture")...)
	}
	return b
}
func rows(t table, skip bool, page []byte) []byte {
	b := append(i32(2), metadata(t.cols, skip, page)...)
	b = append(b, i32(len(t.rows))...)
	for _, r := range t.rows {
		for _, v := range r {
			b = append(b, blob(v)...)
		}
	}
	return b
}

var dataCols = []col{integer("k"), integer("r"), {"p", typ(3)}}

func variant(k, r int32) int { return int((uint32(k)*131 + uint32(r)*17) % 256) }
func dataset(pg bool, k int32, start, end int, skip bool, next []byte) []byte {
	s := 0
	if pg {
		s = 1
	}
	t := table{cols: dataCols}
	storeMu.RLock()
	defer storeMu.RUnlock()
	for r := start; r < end; r++ {
		p := payloads[s][variant(k, int32(r))]
		if v, ok := overrides[[3]int32{int32(s), k, int32(r)}]; ok {
			p = v
		} else if k < 0 {
			continue
		}
		t.rows = append(t.rows, [][]byte{i32(int(k)), i32(r), p})
	}
	return rows(t, skip, next)
}
func initData() {
	for s := 0; s < 2; s++ {
		for id := 0; id < 256; id++ {
			b := make([]byte, 1024*(1+3*s))
			x := uint64(id + 1)
			for i := range b {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				b[i] = byte(x)
			}
			payloads[s][id] = b
		}
	}
	for k := range points {
		points[k] = dataset(false, int32(k), 0, 1, true, nil)
	}
	for k := range pages {
		for p := 0; p < 5; p++ {
			var next []byte
			if p < 4 {
				next = i32((p + 1) * 32)
			}
			pages[k][p] = dataset(true, int32(k), p*32, min((p+1)*32, 128), true, next)
		}
	}
}
func topology(node int, peers bool) table {
	t := table{cols: []col{{"host_id", typ(12)}, {"schema_version", typ(12)}, text("data_center"), text("rack"), text("release_version"), text("cluster_name"), text("partitioner"), {"rpc_address", typ(16)}, {"native_address", typ(16)}, {"peer", typ(16)}, integer("native_port"), {"tokens", append(typ(34), typ(13)...)}}}
	tokens := []string{"-6000000000000000000", "0", "6000000000000000000"}
	for n := 0; n < 3; n++ {
		if (n == node) == peers {
			continue
		}
		ip := []byte{127, 0, 0, byte(n + 2)}
		t.rows = append(t.rows, [][]byte{uuid(byte(n + 1)), uuid(99), []byte("datacenter1"), []byte("rack1"), []byte("4.0.0"), []byte("local-native-v4-emulator"), []byte("org.apache.cassandra.dht.Murmur3Partitioner"), ip, ip, ip, i32(9042), list(tokens[n])})
	}
	return t
}
func schema(q string, args [][]byte, node int) (table, bool) {
	switch {
	case strings.Contains(q, "from system.local"):
		return topology(node, false), true
	case strings.Contains(q, "from system.peers"):
		return topology(node, true), true
	case strings.Contains(q, "from system_schema.keyspaces"):
		return table{[]col{text("keyspace_name"), {"durable_writes", typ(4)}, {"replication", append(append(typ(33), typ(13)...), typ(13)...)}}, [][][]byte{{[]byte("bench_compare"), {1}, textmap("class", "org.apache.cassandra.locator.NetworkTopologyStrategy", "datacenter1", "3")}}}, true
	case strings.Contains(q, "from system_schema.tables"):
		t := table{cols: []col{text("keyspace_name"), text("table_name"), {"id", typ(12)}, {"flags", append(typ(34), typ(13)...)}}}
		for i, n := range []string{"point", "paged"} {
			if len(args) > 1 && string(args[1]) != n {
				continue
			}
			t.rows = append(t.rows, [][]byte{[]byte("bench_compare"), []byte(n), uuid(byte(20 + i)), list("compound")})
		}
		return t, true
	case strings.Contains(q, "from system_schema.columns"):
		t := table{cols: []col{text("keyspace_name"), text("table_name"), text("column_name"), text("clustering_order"), text("type"), text("kind"), integer("position")}}
		for _, n := range []string{"point", "paged"} {
			if len(args) > 1 && string(args[1]) != n {
				continue
			}
			for _, c := range []string{"k", "r", "p"} {
				kind, pos, order, ty := "regular", -1, "none", "int"
				if c == "k" {
					kind, pos = "partition_key", 0
				}
				if c == "r" && n == "paged" {
					kind, pos, order = "clustering", 0, "asc"
				}
				if c == "p" {
					ty = "blob"
				}
				t.rows = append(t.rows, [][]byte{[]byte("bench_compare"), []byte(n), []byte(c), []byte(order), []byte(ty), []byte(kind), i32(pos)})
			}
		}
		return t, true
	case strings.Contains(q, "from system_schema."):
		// Empty auxiliary schema tables retain the identity columns used by each decoder.
		t := table{cols: []col{text("keyspace_name")}}
		for _, n := range []string{"type_name", "table_name", "function_name", "aggregate_name", "index_name", "view_name", "body", "language", "return_type", "state_func", "state_type", "kind", "base_table_name", "where_clause"} {
			t.cols = append(t.cols, text(n))
		}
		for _, n := range []string{"argument_types", "argument_names", "field_names", "field_types"} {
			t.cols = append(t.cols, col{n, append(typ(32), typ(13)...)})
		}
		for _, n := range []string{"called_on_null_input", "include_all_columns"} {
			t.cols = append(t.cols, col{n, typ(4)})
		}
		for _, n := range []string{"id", "base_table_id"} {
			t.cols = append(t.cols, col{n, typ(12)})
		}
		t.cols = append(t.cols, col{"options", append(append(typ(33), typ(13)...), typ(13)...)})
		return t, true
	}
	return table{}, false
}

// Honor SELECT projection order: scylla-go-driver decodes topology by position.
func project(q string, t table) table {
	end := strings.Index(q, " from ")
	if end < 0 || !strings.HasPrefix(q, "select ") || strings.TrimSpace(q[7:end]) == "*" {
		return t
	}
	var indices []int
	result := table{}
	for _, name := range strings.Split(q[7:end], ",") {
		name = strings.TrimSpace(name)
		for i, c := range t.cols {
			if c.name == name {
				indices = append(indices, i)
				result.cols = append(result.cols, c)
				break
			}
		}
	}
	for _, row := range t.rows {
		var selected [][]byte
		for _, i := range indices {
			selected = append(selected, row[i])
		}
		result.rows = append(result.rows, selected)
	}
	return result
}

func supported() []byte {
	options := [][2]string{{"CQL_VERSION", "3.0.0"}, {"SCYLLA_SHARD", "0"}, {"SCYLLA_NR_SHARDS", "1"}, {"SCYLLA_PARTITIONER", "org.apache.cassandra.dht.Murmur3Partitioner"}, {"SCYLLA_SHARDING_ALGORITHM", "biased-token-round-robin"}, {"SCYLLA_SHARDING_IGNORE_MSB", "12"}, {"SCYLLA_SHARD_AWARE_PORT", "9042"}}
	b := u16(len(options))
	for _, option := range options {
		b = append(b, str(option[0])...)
		b = append(b, u16(1)...)
		b = append(b, str(option[1])...)
	}
	return b
}

type reader struct {
	b []byte
	i int
}

func (r *reader) take(n int) []byte {
	if n < 0 || r.i+n > len(r.b) {
		panic("truncated request")
	}
	b := r.b[r.i : r.i+n]
	r.i += n
	return b
}
func (r *reader) short() int { return int(binary.BigEndian.Uint16(r.take(2))) }
func (r *reader) int() int   { return int(int32(binary.BigEndian.Uint32(r.take(4)))) }
func (r *reader) bytes() []byte {
	n := r.int()
	if n < 0 {
		return nil
	}
	return r.take(n)
}
func (r *reader) shortbytes() []byte { return r.take(r.short()) }
func (r *reader) params() params {
	r.take(2)
	f := r.take(1)[0]
	p := params{size: 32, skip: f&2 != 0}
	if f&1 != 0 {
		n := r.short()
		for i := 0; i < n; i++ {
			if f&64 != 0 {
				r.shortbytes()
			}
			p.values = append(p.values, r.bytes())
		}
	}
	if f&4 != 0 {
		p.size = r.int()
	}
	if f&8 != 0 {
		p.page = r.bytes()
	}
	if f&16 != 0 {
		r.take(2)
	}
	if f&32 != 0 {
		r.take(8)
	}
	return p
}
func prepare(q string, node int) []byte {
	id := sha256.Sum256([]byte(q))
	prepared.Store(hex.EncodeToString(id[:16]), q)
	lower := strings.ToLower(q)
	var paramsCols, resultCols []col
	if strings.Contains(lower, "bench_compare.") {
		if strings.Contains(q, "?") {
			paramsCols = []col{integer("k")}
		}
		if strings.HasPrefix(lower, "insert") {
			paramsCols = dataCols
		} else if strings.HasPrefix(lower, "select") {
			resultCols = dataCols
		}
	} else {
		for i := 0; i < strings.Count(q, "?"); i++ {
			n := "keyspace_name"
			if i > 0 {
				n = "table_name"
			}
			paramsCols = append(paramsCols, text(n))
		}
		t, _ := schema(lower, nil, node)
		resultCols = project(lower, t).cols
	}
	b := append(i32(4), append(u16(16), id[:16]...)...)
	flags := 0
	if len(paramsCols) > 0 {
		flags = 1
	}
	b = append(b, i32(flags)...)
	b = append(b, i32(len(paramsCols))...)
	if len(paramsCols) > 0 && paramsCols[0].name == "k" {
		b = append(b, i32(1)...)
		b = append(b, u16(0)...)
	} else {
		b = append(b, i32(0)...)
	}
	if len(paramsCols) > 0 {
		b = append(b, colsBytes(paramsCols, "bench_compare", map[bool]string{true: "paged", false: "point"}[strings.Contains(lower, "paged")])...)
	}
	b = append(b, metadata(resultCols, false, nil)...)
	return b
}
func query(q string, p params, node int) (byte, []byte) {
	l := strings.ToLower(strings.TrimSpace(q))
	if t, ok := schema(l, p.values, node); ok {
		return 8, rows(project(l, t), p.skip, nil)
	}
	if strings.HasPrefix(l, "use ") {
		return 8, append(i32(3), str(strings.Trim(strings.TrimSpace(q[4:]), "\""))...)
	}
	if strings.HasPrefix(l, "describe ") {
		return 8, rows(table{cols: []col{text("create_statement")}}, false, nil)
	}
	pg := strings.Contains(l, "bench_compare.paged")
	s := 0
	if pg {
		s = 1
	}
	if strings.HasPrefix(l, "delete from bench_compare.") {
		storeMu.Lock()
		for k := range overrides {
			if k[0] == int32(s) && k[1] == -7 {
				delete(overrides, k)
			}
		}
		storeMu.Unlock()
		return 8, i32(1)
	}
	if strings.Contains(l, "bench_compare.") && len(p.values) > 0 {
		k := int32(binary.BigEndian.Uint32(p.values[0]))
		if strings.HasPrefix(l, "insert") {
			if len(p.values) != 3 {
				panic("invalid insert")
			}
			r := int32(binary.BigEndian.Uint32(p.values[1]))
			key := [3]int32{int32(s), k, r}
			// Canonical positive keys are already materialized. Store every differing value.
			storeMu.Lock()
			if k >= 0 && bytes.Equal(p.values[2], payloads[s][variant(k, r)]) {
				delete(overrides, key)
			} else {
				overrides[key] = bytes.Clone(p.values[2])
			}
			storeMu.Unlock()
			return 8, i32(1)
		}
		if strings.HasPrefix(l, "select") {
			start := 0
			if p.page != nil {
				start = int(binary.BigEndian.Uint32(p.page))
			}
			if p.size != 32 {
				panic("unexpected page size")
			}
			storeMu.RLock()
			modified := false
			for key := range overrides {
				if key[0] == int32(s) && key[1] == k {
					modified = true
					break
				}
			}
			storeMu.RUnlock()
			if k >= 0 && p.skip && !modified {
				if pg && k < 1024 && start/32 < 5 {
					return 8, pages[k][start/32]
				}
				if !pg && k < 100000 {
					return 8, points[k]
				}
			}
			end := 1
			var next []byte
			if pg {
				end = min(start+32, 128)
				if start < 128 {
					next = i32(start + 32)
				}
			}
			return 8, dataset(pg, k, start, end, p.skip, next)
		}
	}
	failures.Add(1)
	log.Printf("unsupported query: %s", q)
	return 0, append(i32(0x2200), str("emulator unsupported query: "+q)...)
}
func serve(c net.Conn, node int) {
	defer workers.Done()
	active.Store(c, true)
	defer active.Delete(c)
	defer c.Close()
	defer func() {
		if e := recover(); e != nil {
			failures.Add(1)
			log.Printf("protocol error: %v", e)
		}
	}()
	r := bufio.NewReaderSize(c, 65536)
	var header [9]byte
	var body []byte
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return
		}
		if header[0] != 4 || header[1] != 0 {
			panic("requires uncompressed unextended native v4")
		}
		n := int(binary.BigEndian.Uint32(header[5:]))
		if n > 16<<20 {
			panic("oversized request")
		}
		if cap(body) < n {
			body = make([]byte, n)
		}
		body = body[:n]
		if _, err := io.ReadFull(r, body); err != nil {
			return
		}
		requests.Add(1)
		input := reader{b: body}
		op := byte(8)
		var out []byte
		switch header[4] {
		case 1, 11:
			op = 2
		case 5:
			op = 6
			out = supported()
		case 7:
			q := string(input.take(input.int()))
			op, out = query(q, input.params(), node)
		case 9:
			q := string(input.take(input.int()))
			out = prepare(q, node)
		case 10:
			id := hex.EncodeToString(input.shortbytes())
			q, ok := prepared.Load(id)
			if !ok {
				panic("unknown prepared ID")
			}
			op, out = query(q.(string), input.params(), node)
		default:
			panic(fmt.Sprintf("unsupported opcode %d", header[4]))
		}
		header[0] = 0x84
		header[1] = 0
		header[4] = op
		binary.BigEndian.PutUint32(header[5:], uint32(len(out)))
		buffers := net.Buffers{header[:], out}
		if _, err := buffers.WriteTo(c); err != nil {
			return
		}
	}
}
func main() {
	initData()
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"requests": requests.Load(), "failures": failures.Load()})
	})
	admin := &http.Server{Addr: "127.0.0.1:19080"}
	go func() {
		if e := admin.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			log.Print(e)
		}
	}()
	for n := 0; n < 3; n++ {
		l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.%d:9042", n+2))
		if e != nil {
			log.Fatal(e)
		}
		listeners = append(listeners, l)
		go func(node int, l net.Listener) {
			for {
				c, e := l.Accept()
				if e != nil {
					return
				}
				workers.Add(1)
				go serve(c, node)
			}
		}(n, l)
	}
	log.Print("ready: 127.0.0.2,127.0.0.3,127.0.0.4:9042; synthetic native-v4 fixtures; no replication or persistence")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	for _, l := range listeners {
		l.Close()
	}
	active.Range(func(k, v any) bool { k.(net.Conn).Close(); return true })
	workers.Wait()
	admin.Close()
}
