package redis

import (
	"context"

	gocql "github.com/gocql/gocql"
)

// errNotFound is the "no rows" sentinel every read path checks for. It is
// aliased so fakes and tests do not have to reach into gocql.
var errNotFound = gocql.ErrNotFound

// iterOptions controls server side paging for multi row reads.
type iterOptions struct {
	// pageSize bounds how many rows the server returns per page.
	pageSize int
	// pageState resumes a previous iteration.
	pageState []byte
	// singlePage disables driver side auto paging so exactly one page is
	// fetched and the resulting page state can be handed back to the caller.
	singlePage bool
}

type rowIterator interface {
	Scan(dest ...any) bool
	PageState() []byte
	Close() error
}

type batchStatement struct {
	stmt string
	args []any
}

// queryRunner is the full storage seam used by the client. Every path that
// talks to Scylla goes through it, including conditional writes, batches and
// multi row reads, so the whole command surface can be exercised without a
// cluster.
type queryRunner interface {
	Exec(ctx context.Context, stmt string, args ...any) error
	ScanOne(ctx context.Context, stmt string, args []any, dest ...any) error
	// ExecCAS runs a conditional statement and reports only whether it
	// applied. A rejected conditional may return anything from [applied] alone
	// to the entire row depending on the condition and the server, so the
	// result is never scanned positionally.
	ExecCAS(ctx context.Context, stmt string, args []any) (bool, error)
	// MapScanCAS runs a conditional statement whose rejected result is needed,
	// addressed by column name.
	MapScanCAS(ctx context.Context, stmt string, args []any, dest map[string]any) (bool, error)
	Iterate(ctx context.Context, stmt string, args []any, opt iterOptions) rowIterator
	BatchCAS(ctx context.Context, batchType gocql.BatchType, stmts []batchStatement) (bool, error)
}

type sessionRunner struct {
	session *gocql.Session
	serial  gocql.Consistency
}

// query builds a non conditional query. Plain inserts, deletes and reads in
// this package are upserts or pure reads, so they are safe to replay and are
// marked idempotent to let the driver use speculative execution and retries.
func (r *sessionRunner) query(ctx context.Context, stmt string, args []any) *gocql.Query {
	return r.session.Query(stmt, args...).WithContext(ctx).Idempotent(true)
}

// casQuery builds a conditional query. Lightweight transactions are never
// marked idempotent: replaying one can apply an update the caller already
// observed as failed.
func (r *sessionRunner) casQuery(ctx context.Context, stmt string, args []any) *gocql.Query {
	q := r.session.Query(stmt, args...).WithContext(ctx)
	if r.serial != 0 {
		q = q.SerialConsistency(r.serial)
	}
	return q
}

func (r *sessionRunner) Exec(ctx context.Context, stmt string, args ...any) error {
	return r.query(ctx, stmt, args).Exec()
}

func (r *sessionRunner) ScanOne(ctx context.Context, stmt string, args []any, dest ...any) error {
	return r.query(ctx, stmt, args).Scan(dest...)
}

func (r *sessionRunner) ExecCAS(ctx context.Context, stmt string, args []any) (bool, error) {
	return r.casQuery(ctx, stmt, args).MapScanCAS(map[string]any{})
}

func (r *sessionRunner) MapScanCAS(ctx context.Context, stmt string, args []any, dest map[string]any) (bool, error) {
	return r.casQuery(ctx, stmt, args).MapScanCAS(dest)
}

func (r *sessionRunner) Iterate(ctx context.Context, stmt string, args []any, opt iterOptions) rowIterator {
	q := r.query(ctx, stmt, args)
	if opt.pageSize > 0 {
		q = q.PageSize(opt.pageSize)
	}
	if opt.singlePage {
		// Setting the page state also disables auto paging, which is what
		// makes a resumable cursor possible.
		q = q.PageState(opt.pageState)
	}
	return &sessionIter{iter: q.Iter()}
}

func (r *sessionRunner) BatchCAS(ctx context.Context, batchType gocql.BatchType, stmts []batchStatement) (bool, error) {
	batch := r.session.Batch(batchType).WithContext(ctx)
	if r.serial != 0 {
		batch = batch.SerialConsistency(r.serial)
	}
	for i := range stmts {
		batch.Query(stmts[i].stmt, stmts[i].args...)
	}
	// A rejected conditional batch returns the current row, whose shape depends
	// on the condition, so the result is read by column name. Scanning
	// positionally fails as soon as the server sends more than [applied].
	applied, iter, err := r.session.MapExecuteBatchCAS(batch, map[string]any{})
	if iter != nil {
		_ = iter.Close()
	}
	return applied, err
}

type sessionIter struct {
	iter *gocql.Iter
}

func (i *sessionIter) Scan(dest ...any) bool { return i.iter.Scan(dest...) }
func (i *sessionIter) PageState() []byte     { return i.iter.PageState() }
func (i *sessionIter) Close() error          { return i.iter.Close() }
