package redis

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const defaultTable = "redis_compat"

// Row kinds inside a key partition. The meta row sorts first, so a read that
// wants "the key and its first element" gets both from one clustering slice.
const (
	kindMeta   int8 = 0
	kindField  int8 = 1
	kindMember int8 = 2
	kindPos    int8 = 3
)

// metaSubLiteral is the clustering value of the meta row, inlined in every
// statement rather than bound. A one byte constant avoids relying on how the
// driver and the server treat an empty blob in a primary key.
const metaSubLiteral = "0x00"

// metaCols is the projection every meta read shares.
const metaCols = "type, value, version, size, head, tail, expires_at"

// identifierPattern is the allowlist for keyspace and table names. CQL cannot
// bind identifiers, so they are the one place where interpolation is
// unavoidable; restricting them to plain identifiers removes the injection
// surface without changing case folding behaviour.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,47}$`)

// schema owns table names and every CQL statement the client issues.
//
// One table holds a key and its elements in a single partition: the meta row
// carries the type, the string value, a version, the element count, the two
// list bounds and the logical expiry, and the element rows carry hash fields,
// set members and list positions. That is what lets a command assert a key's
// type, mutate its elements and maintain its size in one conditional batch,
// which is the atomicity the previous layout could not express.
//
// Two side tables exist only to answer questions the key partition cannot:
// index enumerates keys for Keys and Scan, and expiry lets Sweep find keys
// whose logical expiry has passed. Neither is authoritative; both are supersets
// that enumeration repairs as it goes.
type schema struct {
	// bucketed reports whether a bucket column participates in the key.
	bucketed bool
	// grouped reports whether the bucket alone is the partition key, which
	// puts every key of a bucket in one partition and is what makes a
	// transaction over several keys expressible.
	grouped bool

	table  string
	index  string
	expiry string
	wakeup string

	// Fragments shared by the statements built below, kept so the multi-column
	// IN statement can be assembled per arity without re-deriving them.
	keyCols     string
	keyVals     string
	keyCond     string
	metaKeyCond string

	// Reads.
	metaRead string
	keyRead  string
	kindRead string
	edgeRead string
	edgeLast string
	listRead string

	// Meta writes.
	strWrite      string
	strWriteTTL   string
	strWriteNX    string
	strWriteNXTTL string
	strCAS        string
	strCASTTL     string
	collCreate    string
	collCAS       string
	expireCAS     string
	absentCAS     string
	metaDeleteIf  string
	metaDeleteCAS string

	// Element writes.
	elemWrite   string
	elemDelete  string
	elemsDelete string
	keyDelete   string

	// Side tables.
	indexWrite  string
	indexDelete string
	indexScan   string
	expiryWrite string
	expiryScan  string
	expiryDrop  string
	wakeupWrite string
	wakeupScan  string

	ddl []string

	// picks caches the multi-column IN statement for each arity, so a command
	// that reads a key's meta row together with n named elements issues one
	// query without rebuilding the statement text per call.
	picks sync.Map
}

func newSchema(table string, bucketed, grouped bool) *schema {
	s := &schema{
		bucketed: bucketed || grouped,
		grouped:  grouped,
		table:    table,
		index:    table + "_index",
		expiry:   table + "_expiry",
		wakeup:   table + "_wakeup",
	}

	keyCols, keyVals, keyCond := "key", "?", "key = ?"
	if s.bucketed {
		keyCols, keyVals, keyCond = "bucket, key", "?, ?", "bucket = ? AND key = ?"
	}
	meta := keyCond + " AND kind = " + strconv.Itoa(int(kindMeta)) + " AND sub = " + metaSubLiteral
	s.metaKeyCond = meta
	s.keyCond = keyCond
	s.keyCols = keyCols
	s.keyVals = keyVals

	s.metaRead = "SELECT " + metaCols + " FROM " + s.table + " WHERE " + meta
	s.keyRead = "SELECT kind, sub, " + metaCols + " FROM " + s.table + " WHERE " + keyCond
	s.kindRead = "SELECT sub, value FROM " + s.table + " WHERE " + keyCond + " AND kind = ?"
	// A list pop needs the meta row and one end of the list. Ascending, the
	// meta row sorts before every element, so one bounded slice returns both.
	s.edgeRead = "SELECT kind, sub, " + metaCols + " FROM " + s.table +
		" WHERE " + keyCond + " AND kind IN (" + strconv.Itoa(int(kindMeta)) + ", " + strconv.Itoa(int(kindPos)) + ") LIMIT 2"
	s.edgeLast = "SELECT sub, value FROM " + s.table + " WHERE " + keyCond +
		" AND kind = " + strconv.Itoa(int(kindPos)) + " ORDER BY sub DESC LIMIT 1"
	s.listRead = "SELECT sub, value FROM " + s.table + " WHERE " + keyCond +
		" AND kind = " + strconv.Itoa(int(kindPos))

	strCols := "(" + keyCols + ", kind, sub, type, value, version, expires_at)"
	strVals := "(" + keyVals + ", " + strconv.Itoa(int(kindMeta)) + ", " + metaSubLiteral + ", '" + string(typeString) + "', ?, ?, ?)"
	s.strWrite = "INSERT INTO " + s.table + " " + strCols + " VALUES " + strVals
	s.strWriteTTL = s.strWrite + " USING TTL ?"
	s.strWriteNX = s.strWrite + " IF NOT EXISTS"
	// The server accepts the TTL only after the condition.
	s.strWriteNXTTL = s.strWrite + " IF NOT EXISTS USING TTL ?"
	s.strCAS = "UPDATE " + s.table + " SET type = '" + string(typeString) +
		"', value = ?, version = ?, expires_at = ? WHERE " + meta + " IF version = ?"
	s.strCASTTL = "UPDATE " + s.table + " USING TTL ? SET type = '" + string(typeString) +
		"', value = ?, version = ?, expires_at = ? WHERE " + meta + " IF version = ?"

	s.collCreate = "INSERT INTO " + s.table + " (" + keyCols + ", kind, sub, type, version, size, head, tail, expires_at) VALUES (" +
		keyVals + ", " + strconv.Itoa(int(kindMeta)) + ", " + metaSubLiteral + ", ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
	s.collCAS = "UPDATE " + s.table + " SET version = ?, size = ?, head = ?, tail = ? WHERE " + meta +
		" IF type = ? AND version = ?"
	s.expireCAS = "UPDATE " + s.table + " SET version = ?, expires_at = ? WHERE " + meta + " IF version = ?"
	// absentCAS asserts that a key is still absent, which is what a WATCH on a
	// key that did not exist has to guard. Setting a column to null writes a
	// cell tombstone and no row marker, so the row stays absent for reads: the
	// guard cannot bring into existence the key whose absence it asserts.
	s.absentCAS = "UPDATE " + s.table + " SET value = null WHERE " + meta + " IF version = null"
	s.metaDeleteIf = "DELETE FROM " + s.table + " WHERE " + meta + " IF EXISTS"
	s.metaDeleteCAS = "DELETE FROM " + s.table + " WHERE " + meta + " IF version = ?"

	s.elemWrite = "INSERT INTO " + s.table + " (" + keyCols + ", kind, sub, value) VALUES (" + keyVals + ", ?, ?, ?)"
	s.elemDelete = "DELETE FROM " + s.table + " WHERE " + keyCond + " AND kind = ? AND sub = ?"
	// Every element kind sorts after the meta row, so one slice delete removes
	// a key's contents while leaving the meta row to carry the condition.
	s.elemsDelete = "DELETE FROM " + s.table + " WHERE " + keyCond + " AND kind >= " + strconv.Itoa(int(kindField))
	s.keyDelete = "DELETE FROM " + s.table + " WHERE " + keyCond

	s.indexWrite = "INSERT INTO " + s.index + " (" + keyCols + ") VALUES (" + keyVals + ")"
	s.indexDelete = "DELETE FROM " + s.index + " WHERE " + keyCond
	s.indexScan = "SELECT key FROM " + s.index
	if s.bucketed {
		s.indexScan += " WHERE bucket = ?"
	}

	s.expiryWrite = "INSERT INTO " + s.expiry + " (slot, bucket, key) VALUES (?, ?, ?) USING TTL ?"
	s.expiryScan = "SELECT bucket, key FROM " + s.expiry + " WHERE slot = ?"
	s.expiryDrop = "DELETE FROM " + s.expiry + " WHERE slot = ? AND bucket = ? AND key = ?"

	s.wakeupWrite = "INSERT INTO " + s.wakeup + " (slot, bucket, key) VALUES (?, ?, ?) USING TTL ?"
	s.wakeupScan = "SELECT bucket, key FROM " + s.wakeup + " WHERE slot = ?"

	cols := "key text, kind tinyint, sub blob, value blob, type text, " +
		"version bigint, size bigint, head bigint, tail bigint, expires_at timestamp"
	switch {
	case s.grouped:
		s.ddl = []string{
			"CREATE TABLE IF NOT EXISTS " + s.table + " (bucket text, " + cols +
				", PRIMARY KEY ((bucket), key, kind, sub))",
			"CREATE TABLE IF NOT EXISTS " + s.index + " (bucket text, key text, PRIMARY KEY ((bucket), key))",
		}
	case s.bucketed:
		s.ddl = []string{
			"CREATE TABLE IF NOT EXISTS " + s.table + " (bucket text, " + cols +
				", PRIMARY KEY ((bucket, key), kind, sub))",
			"CREATE TABLE IF NOT EXISTS " + s.index + " (bucket text, key text, PRIMARY KEY ((bucket), key))",
		}
	default:
		s.ddl = []string{
			"CREATE TABLE IF NOT EXISTS " + s.table + " (" + cols + ", PRIMARY KEY ((key), kind, sub))",
			"CREATE TABLE IF NOT EXISTS " + s.index + " (key text PRIMARY KEY)",
		}
	}
	s.ddl = append(s.ddl,
		"CREATE TABLE IF NOT EXISTS "+s.expiry+
			" (slot timestamp, bucket text, key text, PRIMARY KEY ((slot), bucket, key))",
		"CREATE TABLE IF NOT EXISTS "+s.wakeup+
			" (slot timestamp, bucket text, key text, PRIMARY KEY ((slot), bucket, key))",
	)

	return s
}

// pick returns the statement that reads a key's meta row together with n named
// elements. Reading them in one multi-column IN is what keeps HGet, SIsMember
// and every element mutation at a single read, type assertion included.
//
// A multi-column relation may not skip a clustering column, so the tuple spans
// whatever the layout leaves clustered: with the bucket as the whole partition
// key the key itself is a clustering column and has to be inside the tuple,
// while otherwise the key is part of the partition and the tuple starts at kind.
func (s *schema) pick(n int) string {
	if cached, ok := s.picks.Load(n); ok {
		return cached.(string)
	}
	var b strings.Builder
	b.WriteString("SELECT kind, sub, ")
	b.WriteString(metaCols)
	b.WriteString(" FROM ")
	b.WriteString(s.table)
	if s.grouped {
		b.WriteString(" WHERE bucket = ? AND (key, kind, sub) IN ((?, ")
	} else {
		b.WriteString(" WHERE ")
		b.WriteString(s.keyCond)
		b.WriteString(" AND (kind, sub) IN ((")
	}
	b.WriteString(strconv.Itoa(int(kindMeta)))
	b.WriteString(", ")
	b.WriteString(metaSubLiteral)
	b.WriteString(")")
	element := ", (?, ?)"
	if s.grouped {
		element = ", (?, ?, ?)"
	}
	for i := 0; i < n; i++ {
		b.WriteString(element)
	}
	b.WriteString(")")
	stmt := b.String()
	s.picks.Store(n, stmt)
	return stmt
}

func (s *schema) create(ctx context.Context, runner queryRunner) error {
	for _, stmt := range s.ddl {
		if err := runner.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// qualifyTable validates the caller supplied identifiers and returns the name
// used in statements. A table containing a dot is treated as keyspace.table.
func qualifyTable(keyspace, table string, haveSession bool) (string, error) {
	if strings.Contains(table, ".") {
		parts := strings.Split(table, ".")
		if len(parts) != 2 {
			return "", Error("rediscompat: table must be \"table\" or \"keyspace.table\"")
		}
		for _, part := range parts {
			if !identifierPattern.MatchString(part) {
				return "", Error("rediscompat: invalid identifier " + quoteForError(part))
			}
		}
		return parts[0] + "." + parts[1], nil
	}

	if !identifierPattern.MatchString(table) {
		return "", Error("rediscompat: invalid table name " + quoteForError(table))
	}
	if keyspace != "" {
		if !identifierPattern.MatchString(keyspace) {
			return "", Error("rediscompat: invalid keyspace name " + quoteForError(keyspace))
		}
		return keyspace + "." + table, nil
	}
	if haveSession {
		// The supplied session already selected a keyspace.
		return table, nil
	}
	return "", Error("rediscompat: keyspace is required unless table is fully qualified")
}

func quoteForError(s string) string {
	if len(s) > 64 {
		s = s[:64] + "..."
	}
	return "\"" + strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, s) + "\""
}

// Argument builders. Each mirrors one statement above and is the only place
// that knows whether the bucket column participates in the primary key.

func (c *Client) keyArgs(key string) []any {
	if c.core.schema.bucketed {
		return []any{c.bucket, key}
	}
	return []any{key}
}

func (c *Client) scanArgs() []any {
	if c.core.schema.bucketed {
		return []any{c.bucket}
	}
	return nil
}

func (c *Client) pickArgs(key string, kind int8, subs [][]byte) []any {
	if c.core.schema.grouped {
		// The key rides inside every tuple, including the meta row's.
		args := []any{c.bucket, key}
		for _, sub := range subs {
			args = append(args, key, kind, sub)
		}
		return args
	}
	args := c.keyArgs(key)
	for _, sub := range subs {
		args = append(args, kind, sub)
	}
	return args
}

// strWriteArgs matches strWrite, strWriteNX and their TTL variants.
func (c *Client) strWriteArgs(key string, value []byte, version int64, expires any, ttl int) []any {
	args := append(c.keyArgs(key), value, version, expires)
	if ttl > 0 {
		args = append(args, ttl)
	}
	return args
}

func (c *Client) strCASArgs(key string, value []byte, version int64, expires any, expect int64, ttl int) []any {
	var args []any
	if ttl > 0 {
		args = append(args, ttl)
	}
	args = append(args, value, version, expires)
	args = append(args, c.keyArgs(key)...)
	return append(args, expect)
}

func (c *Client) collCreateArgs(key string, kt keyType, m keyMeta) []any {
	return append(c.keyArgs(key), string(kt), m.version, m.size, m.head, m.tail, expiryArg(m.expires))
}

func (c *Client) collCASArgs(key string, kt keyType, m keyMeta, expect int64) []any {
	args := []any{m.version, m.size, m.head, m.tail}
	args = append(args, c.keyArgs(key)...)
	return append(args, string(kt), expect)
}

func (c *Client) expireCASArgs(key string, version int64, expires any, expect int64) []any {
	args := []any{version, expires}
	args = append(args, c.keyArgs(key)...)
	return append(args, expect)
}

func (c *Client) metaDeleteCASArgs(key string, expect int64) []any {
	return append(c.keyArgs(key), expect)
}

func (c *Client) elemWriteArgs(key string, kind int8, sub, value []byte) []any {
	return append(c.keyArgs(key), kind, sub, value)
}

func (c *Client) elemDeleteArgs(key string, kind int8, sub []byte) []any {
	return append(c.keyArgs(key), kind, sub)
}

func (c *Client) kindReadArgs(key string, kind int8) []any {
	return append(c.keyArgs(key), kind)
}
