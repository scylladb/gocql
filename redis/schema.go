package redis

import (
	"context"
	"regexp"
	"strings"
)

const defaultTable = "redis_compat_kv"

// guardKey names the bookkeeping row used by atomic MSet. It lives in the kv
// table so the conditional batch stays inside a single partition, and it is
// filtered out of every key enumeration.
const guardKey = "\x00rediscompat:guard"

// identifierPattern is the allowlist for keyspace and table names. CQL cannot
// bind identifiers, so they are the one place where interpolation is
// unavoidable; restricting them to plain identifiers removes the injection
// surface without changing case folding behaviour.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,47}$`)

// schema owns table names and every CQL statement the client issues.
// Statements are built once at construction so no command has to concatenate
// strings on the hot path, and so the bucketed and non bucketed layouts differ
// in exactly one place instead of being re-derived at each call site.
type schema struct {
	bucketed bool

	kvTable   string
	hashTable string
	setTable  string
	listTable string

	kvUpsert         string
	kvUpsertTTL      string
	kvInsertNX       string
	kvInsertNXTTL    string
	kvMarkerNX       string
	kvUpdateCAS      string
	kvUpdateCASTTL   string
	kvSelect         string
	kvSelectTTL      string
	kvSelectType     string
	kvSelectKey      string
	kvDelete         string
	kvDeleteIfExists string
	kvDeleteCAS      string
	kvScan           string
	kvGuardCAS       string

	hashUpsert    string
	hashInsertNX  string
	hashSelect    string
	hashDeleteIf  string
	hashDeleteKey string
	hashSelectAll string

	setInsertNX  string
	setSelect    string
	setDeleteIf  string
	setDeleteKey string
	setSelectAll string
	setCount     string

	listInsertNX  string
	listEdgeAsc   string
	listEdgeDesc  string
	listDeleteIf  string
	listDeleteKey string
	listSelectAll string
	listCount     string

	ddl    []string
	alters []string
}

func newSchema(table string, bucketed bool) *schema {
	s := &schema{
		bucketed:  bucketed,
		kvTable:   table,
		hashTable: table + "_hash",
		setTable:  table + "_set",
		listTable: table + "_list",
	}

	keyCols, keyVals, keyCond := "key", "?", "key = ?"
	if bucketed {
		keyCols, keyVals, keyCond = "bucket, key", "?, ?", "bucket = ? AND key = ?"
	}

	s.kvUpsert = "INSERT INTO " + s.kvTable + " (" + keyCols + ", type, value) VALUES (" + keyVals + ", ?, ?)"
	s.kvUpsertTTL = s.kvUpsert + " USING TTL ?"
	s.kvInsertNX = s.kvUpsert + " IF NOT EXISTS"
	s.kvInsertNXTTL = s.kvUpsert + " IF NOT EXISTS USING TTL ?"
	s.kvMarkerNX = "INSERT INTO " + s.kvTable + " (" + keyCols + ", type) VALUES (" + keyVals + ", ?) IF NOT EXISTS"
	s.kvUpdateCAS = "UPDATE " + s.kvTable + " SET type = ?, value = ? WHERE " + keyCond + " IF value = ?"
	s.kvUpdateCASTTL = "UPDATE " + s.kvTable + " USING TTL ? SET type = ?, value = ? WHERE " + keyCond + " IF value = ?"
	s.kvSelect = "SELECT type, value FROM " + s.kvTable + " WHERE " + keyCond
	s.kvSelectTTL = "SELECT type, value, TTL(value) FROM " + s.kvTable + " WHERE " + keyCond
	s.kvSelectType = "SELECT type FROM " + s.kvTable + " WHERE " + keyCond
	s.kvSelectKey = "SELECT key FROM " + s.kvTable + " WHERE " + keyCond
	s.kvDelete = "DELETE FROM " + s.kvTable + " WHERE " + keyCond
	s.kvDeleteIfExists = s.kvDelete + " IF EXISTS"
	s.kvDeleteCAS = s.kvDelete + " IF value = ?"
	s.kvGuardCAS = "UPDATE " + s.kvTable + " SET value = ? WHERE " + keyCond + " IF value = ?"
	s.kvScan = "SELECT key, type FROM " + s.kvTable
	if bucketed {
		s.kvScan += " WHERE bucket = ?"
	}

	s.hashUpsert = "INSERT INTO " + s.hashTable + " (" + keyCols + ", field, value) VALUES (" + keyVals + ", ?, ?)"
	s.hashInsertNX = s.hashUpsert + " IF NOT EXISTS"
	s.hashSelect = "SELECT value FROM " + s.hashTable + " WHERE " + keyCond + " AND field = ?"
	s.hashDeleteIf = "DELETE FROM " + s.hashTable + " WHERE " + keyCond + " AND field = ? IF EXISTS"
	s.hashDeleteKey = "DELETE FROM " + s.hashTable + " WHERE " + keyCond
	s.hashSelectAll = "SELECT field, value FROM " + s.hashTable + " WHERE " + keyCond

	s.setInsertNX = "INSERT INTO " + s.setTable + " (" + keyCols + ", member) VALUES (" + keyVals + ", ?) IF NOT EXISTS"
	s.setSelect = "SELECT member FROM " + s.setTable + " WHERE " + keyCond + " AND member = ?"
	s.setDeleteIf = "DELETE FROM " + s.setTable + " WHERE " + keyCond + " AND member = ? IF EXISTS"
	s.setDeleteKey = "DELETE FROM " + s.setTable + " WHERE " + keyCond
	s.setSelectAll = "SELECT member FROM " + s.setTable + " WHERE " + keyCond
	s.setCount = "SELECT COUNT(*) FROM " + s.setTable + " WHERE " + keyCond

	s.listInsertNX = "INSERT INTO " + s.listTable + " (" + keyCols + ", pos, value) VALUES (" + keyVals + ", ?, ?) IF NOT EXISTS"
	s.listEdgeAsc = "SELECT pos, value FROM " + s.listTable + " WHERE " + keyCond + " ORDER BY pos ASC LIMIT 1"
	s.listEdgeDesc = "SELECT pos, value FROM " + s.listTable + " WHERE " + keyCond + " ORDER BY pos DESC LIMIT 1"
	s.listDeleteIf = "DELETE FROM " + s.listTable + " WHERE " + keyCond + " AND pos = ? IF EXISTS"
	s.listDeleteKey = "DELETE FROM " + s.listTable + " WHERE " + keyCond
	s.listSelectAll = "SELECT value FROM " + s.listTable + " WHERE " + keyCond + " ORDER BY pos ASC"
	s.listCount = "SELECT COUNT(*) FROM " + s.listTable + " WHERE " + keyCond

	if bucketed {
		s.ddl = []string{
			"CREATE TABLE IF NOT EXISTS " + s.kvTable + " (bucket text, key text, type text, value blob, PRIMARY KEY ((bucket), key))",
			"CREATE TABLE IF NOT EXISTS " + s.hashTable + " (bucket text, key text, field text, value blob, PRIMARY KEY ((bucket, key), field))",
			"CREATE TABLE IF NOT EXISTS " + s.setTable + " (bucket text, key text, member text, PRIMARY KEY ((bucket, key), member))",
			"CREATE TABLE IF NOT EXISTS " + s.listTable + " (bucket text, key text, pos bigint, value blob, PRIMARY KEY ((bucket, key), pos))",
		}
	} else {
		s.ddl = []string{
			"CREATE TABLE IF NOT EXISTS " + s.kvTable + " (key text PRIMARY KEY, type text, value blob)",
			"CREATE TABLE IF NOT EXISTS " + s.hashTable + " (key text, field text, value blob, PRIMARY KEY (key, field))",
			"CREATE TABLE IF NOT EXISTS " + s.setTable + " (key text, member text, PRIMARY KEY (key, member))",
			"CREATE TABLE IF NOT EXISTS " + s.listTable + " (key text, pos bigint, value blob, PRIMARY KEY (key, pos))",
		}
	}

	// Deployments created before the key type registry existed have a kv table
	// without the type column. CREATE TABLE IF NOT EXISTS is a no-op for them,
	// so the column is added separately; the statement fails harmlessly once
	// the column is present.
	s.alters = []string{"ALTER TABLE " + s.kvTable + " ADD type text"}

	return s
}

func (s *schema) create(ctx context.Context, runner queryRunner) error {
	for _, stmt := range s.ddl {
		if err := runner.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	for _, stmt := range s.alters {
		// Best effort: an error here means the column already exists.
		_ = runner.Exec(ctx, stmt)
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

func (c *Client) kvWriteArgs(key string, kt keyType, value []byte) []any {
	if c.core.schema.bucketed {
		return []any{c.bucket, key, string(kt), value}
	}
	return []any{key, string(kt), value}
}

func (c *Client) kvWriteTTLArgs(key string, kt keyType, value []byte, ttl int) []any {
	return append(c.kvWriteArgs(key, kt, value), ttl)
}

func (c *Client) kvMarkerArgs(key string, kt keyType) []any {
	if c.core.schema.bucketed {
		return []any{c.bucket, key, string(kt)}
	}
	return []any{key, string(kt)}
}

func (c *Client) kvCASArgs(key string, kt keyType, value, expect []byte) []any {
	if c.core.schema.bucketed {
		return []any{string(kt), value, c.bucket, key, expect}
	}
	return []any{string(kt), value, key, expect}
}

func (c *Client) kvCASTTLArgs(key string, kt keyType, value, expect []byte, ttl int) []any {
	return append([]any{ttl}, c.kvCASArgs(key, kt, value, expect)...)
}

func (c *Client) kvDeleteCASArgs(key string, expect []byte) []any {
	return append(c.keyArgs(key), expect)
}

func (c *Client) kvScanArgs() []any {
	if c.core.schema.bucketed {
		return []any{c.bucket}
	}
	return nil
}

func (c *Client) hashFieldArgs(key, field string) []any {
	return append(c.keyArgs(key), field)
}

func (c *Client) hashWriteArgs(key, field string, value []byte) []any {
	return append(c.keyArgs(key), field, value)
}

func (c *Client) setMemberArgs(key, member string) []any {
	return append(c.keyArgs(key), member)
}

func (c *Client) listPosArgs(key string, pos int64) []any {
	return append(c.keyArgs(key), pos)
}

func (c *Client) listWriteArgs(key string, pos int64, value []byte) []any {
	return append(c.keyArgs(key), pos, value)
}

func (c *Client) guardCASArgs(next, expect []byte) []any {
	if c.core.schema.bucketed {
		return []any{next, c.bucket, guardKey, expect}
	}
	return []any{next, guardKey, expect}
}
