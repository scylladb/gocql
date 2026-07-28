package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/gocql/gocql"
)

// Timing constants shared by the expiry index and the wakeup channel. Both are
// time bucketed so a sweeper or a tailer reads a bounded number of partitions
// instead of scanning a namespace.
const (
	expirySlotSeconds  = 60
	expiryGraceSeconds = 10
	wakeupSlotSeconds  = 30
)

// Options configures the Scylla backed Redis compatibility client.
// The public shape mirrors go-redis NewClient style while exposing
// Scylla specific keyspace, table and consistency settings.
type Options struct {
	// Addr is a single Scylla contact point (host:port).
	Addr string
	// Addrs are Scylla contact points.
	Addrs []string
	// Username for PasswordAuthenticator.
	Username string
	// Password for PasswordAuthenticator.
	Password string
	// TLSConfig encrypts connections opened by this package. Credentials are
	// sent on the first frame of a connection, so a session created with
	// Username/Password and no TLS transmits them in the clear; supply this
	// whenever the cluster is not reached over a trusted network. Ignored when
	// Session is supplied, since that session carries its own transport.
	TLSConfig *tls.Config
	// AllowPlaintextCredentials permits Username/Password without TLSConfig.
	// It exists so local development and tests do not need certificates; it is
	// required in every other case so an unencrypted credential is a decision
	// rather than an oversight.
	AllowPlaintextCredentials bool
	// Keyspace containing the tables. Required unless Table is fully qualified
	// or an existing Session is supplied.
	Keyspace string
	// Table is the base name for storage. Default: redis_compat. Keys and their
	// elements live in this table; "_index", "_expiry" and "_wakeup" side
	// tables are derived from it. If it includes '.', it is treated as fully
	// qualified keyspace.table. Keyspace and table must be plain CQL
	// identifiers.
	Table string
	// Consistency overrides default query consistency.
	Consistency gocql.Consistency
	// SerialConsistency overrides the consistency of the Paxos phase used by
	// every conditional write. Must be Serial or LocalSerial when set.
	SerialConsistency gocql.Consistency
	// Timeout configures session timeout when session is created internally.
	Timeout time.Duration
	// Session allows reusing an existing gocql session.
	Session *gocql.Session
	// DisableAutoCreateTable disables automatic CREATE TABLE on first
	// operation. When set, create the schema documented in the README first.
	DisableAutoCreateTable bool
	// DisableTokenAwareRouting turns off the token aware, shard aware host
	// policy applied to sessions created by this package.
	DisableTokenAwareRouting bool

	// PartitionByBucket prefixes the partition key with a bucket, which routes
	// a tenant's keys together while keeping one key per partition. It scopes
	// key enumeration to the active bucket.
	PartitionByBucket bool
	// TransactionsByBucket makes the bucket the whole partition key, so every
	// key in a bucket shares one partition and Multi/Watch/Exec can span them.
	// It implies PartitionByBucket.
	//
	// The cost is that a bucket partition then holds every element of every key
	// it contains: size buckets by total elements, not by key count.
	TransactionsByBucket bool
	// Bucket selects the partition used when either bucket mode is enabled.
	// Default: "default". Use Bucketed to derive per bucket views.
	Bucket string

	// MaxConcurrency bounds the in flight requests a single multi key command
	// (MGet, Del, Exists...) may issue. Default: 16.
	MaxConcurrency int
	// CASMaxRetries bounds retries for a contended key. Every mutation of a
	// key is guarded by its version, so concurrent writers to the same key
	// retry; unrelated keys never contend. Default: 8.
	CASMaxRetries int
	// CASRetryBackoff is the initial backoff between retries. Default: 2ms.
	CASRetryBackoff time.Duration
	// CASMaxRetryBackoff caps the retry delay after exponential growth.
	// Default: 20x CASRetryBackoff. Contention on a single row clears in
	// milliseconds, so a long delay here only stretches command latency.
	CASMaxRetryBackoff time.Duration

	// MaxBatchStatements caps how many statements one command may put in a
	// single batch. A command that would exceed it is refused rather than split
	// across batches, so a command that succeeds was applied atomically.
	// Default: 200.
	MaxBatchStatements int

	// ScanPageSize is the server side page size used by Keys and Scan.
	// Default: 500.
	ScanPageSize int
	// MaxScanPageSize caps the COUNT a caller may request from Scan. A page is
	// assembled by the coordinator before it is sent, so an unbounded COUNT
	// turns one call into a whole-namespace response. Default: 10000.
	MaxScanPageSize int
	// MaxKeysScan caps how many keys a single Keys call may accumulate.
	// Zero inherits MaxCollectionScan; a negative value means unlimited.
	// Default: 0.
	MaxKeysScan int
	// MaxCollectionScan caps how many elements a command that materializes a
	// whole collection (HGetAll, SMembers, Sort, list reads) may accumulate
	// before failing with ErrResultTooLarge. A negative value means unlimited.
	// Default: 100000.
	MaxCollectionScan int
	// MaxValueSize caps the size of a single stored value in bytes. A negative
	// value means unlimited. Default: 16MiB, the point past which a CQL cell
	// cannot be written reliably.
	MaxValueSize int
	// MaxScanCursors bounds how many live Scan cursors are retained.
	// Default: 1024.
	MaxScanCursors int
	// ScanCursorTTL is how long an idle Scan cursor stays valid.
	// Default: 5m.
	ScanCursorTTL time.Duration

	// BlockingPollInterval is the delay between BLPop/BRPop polls when no
	// wakeup arrives. Default: 250ms with EnableWakeupChannel, 5ms without,
	// because the poll is a fallback in the first case and the only mechanism
	// in the second.
	BlockingPollInterval time.Duration
	// BlockingPollMaxInterval caps the poll delay after exponential backoff.
	// Default: 250ms.
	BlockingPollMaxInterval time.Duration
	// EnableWakeupChannel lets a producer in one process wake a blocked
	// consumer in another. A push records the key in a time bucketed wakeup
	// partition, and a client with waiters tails that one partition, so
	// notification cost is constant in the number of waiters and keys rather
	// than one query per waiter.
	//
	// Wakeups are best effort: the poll above remains the correctness floor, so
	// a lost wakeup delays a delivery and never drops one. Waiters in the same
	// process as the producer are always woken directly, with no query at all.
	EnableWakeupChannel bool
	// WakeupPollInterval is how often a client with waiters tails the wakeup
	// partition. Default: 20ms, which is affordable precisely because the cost
	// does not scale with the number of waiters.
	WakeupPollInterval time.Duration

	// SweepLookback bounds how far back Sweep looks for expired keys on its
	// first run. Default: 1h.
	SweepLookback time.Duration

	// InitTimeout bounds automatic schema creation. Schema initialization runs
	// on a context detached from the first caller, so a cancelled request
	// cannot leave the client permanently unusable. Default: 10s.
	InitTimeout time.Duration
	// InitRetryCooldown is how long a failed schema initialization is
	// remembered before another caller retries it. Without a cooldown every
	// concurrent request retries DDL against a cluster that just failed one.
	// Default: 1s.
	InitRetryCooldown time.Duration

	// clock overrides time.Now for tests.
	clock func() time.Time
}

// clientCore holds everything shared between a client and the bucket views
// derived from it: the session, schema, registries and initialization state.
type clientCore struct {
	runner       queryRunner
	session      *gocql.Session
	ownedSession *gocql.Session
	schema       *schema

	autoCreate    bool
	maxConcurrent int
	casRetries    int
	casBackoff    time.Duration
	casBackoffMax time.Duration
	maxBatchStmts int

	scanPageSize    int
	maxScanPageSize int
	maxKeysScan     int
	maxCollection   int
	maxValueSize    int

	blockPollMin  time.Duration
	blockPollMax  time.Duration
	wakeupEnabled bool
	wakeupPoll    time.Duration

	sweepLookback time.Duration
	sweepMu       sync.Mutex
	sweepMark     time.Time

	initTimeout  time.Duration
	initCooldown time.Duration
	clock        func() time.Time

	cursors *cursorRegistry
	waiters *waiterRegistry

	initMu      sync.Mutex
	initialized atomic.Bool
	initFailed  time.Time
	closed      atomic.Bool
	closeOnce   sync.Once
	done        chan struct{}
}

// Client is a Redis compatible facade over a Scylla keyspace.
//
// A Client is safe for concurrent use. Bucket views created with Bucketed
// share the underlying session, schema and registries with the client they came
// from.
type Client struct {
	core      *clientCore
	bucket    string
	root      bool
	configErr error
}

func NewClient(opt *Options) *Client {
	client := newConfiguredClient(opt)
	if client.configErr != nil {
		return client
	}
	core := client.core

	if opt.Session != nil {
		core.session = opt.Session
		core.runner = &sessionRunner{session: opt.Session, serial: opt.SerialConsistency}
		return client
	}

	hosts := make([]string, 0, len(opt.Addrs)+1)
	if opt.Addr != "" {
		hosts = append(hosts, opt.Addr)
	}
	hosts = append(hosts, opt.Addrs...)
	if len(hosts) == 0 {
		client.configErr = errors.New("rediscompat: at least one host must be provided in Addr/Addrs")
		return client
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = strings.TrimSpace(opt.Keyspace)
	if opt.Username != "" || opt.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: opt.Username,
			Password: opt.Password,
		}
	}
	if opt.TLSConfig != nil {
		cluster.SslOpts = &gocql.SslOptions{Config: opt.TLSConfig}
	}
	if opt.Timeout > 0 {
		cluster.Timeout = opt.Timeout
	}
	if opt.Consistency != 0 {
		cluster.Consistency = opt.Consistency
	}
	if opt.SerialConsistency != 0 {
		cluster.SerialConsistency = opt.SerialConsistency
	}
	if !opt.DisableTokenAwareRouting {
		// Route each request to a replica that owns the token, which is what
		// makes the shard aware fork worthwhile and keeps load spread evenly
		// instead of funnelling every request through one coordinator.
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 3,
		Min:        100 * time.Millisecond,
		Max:        2 * time.Second,
	}

	session, err := gocql.NewSession(*cluster)
	if err != nil {
		client.configErr = fmt.Errorf("rediscompat: create session: %w", err)
		return client
	}

	core.ownedSession = session
	core.session = session
	core.runner = &sessionRunner{session: session, serial: opt.SerialConsistency}
	return client
}

// newConfiguredClient validates options and assembles the shared core. It stops
// short of wiring a storage runner so the transport can be supplied separately.
func newConfiguredClient(opt *Options) *Client {
	client := &Client{root: true}

	if opt == nil {
		client.configErr = errors.New("rediscompat: options cannot be nil")
		return client
	}

	table := strings.TrimSpace(opt.Table)
	if table == "" {
		table = defaultTable
	}
	keyspace := strings.TrimSpace(opt.Keyspace)

	qualified, err := qualifyTable(keyspace, table, opt.Session != nil)
	if err != nil {
		client.configErr = err
		return client
	}

	if opt.SerialConsistency != 0 && !opt.SerialConsistency.IsSerial() {
		client.configErr = fmt.Errorf("rediscompat: SerialConsistency must be Serial or LocalSerial, got %s", opt.SerialConsistency)
		return client
	}

	// A session this package creates sends the credential on connection setup,
	// so refusing the combination is the only place it can be caught.
	if opt.Session == nil && (opt.Username != "" || opt.Password != "") &&
		opt.TLSConfig == nil && !opt.AllowPlaintextCredentials {
		client.configErr = errors.New("rediscompat: Username/Password without TLSConfig would send credentials in cleartext; " +
			"set TLSConfig, or set AllowPlaintextCredentials to accept it")
		return client
	}

	bucket := strings.TrimSpace(opt.Bucket)
	if bucket == "" {
		bucket = "default"
	}

	blockPollDefault := 5 * time.Millisecond
	if opt.EnableWakeupChannel {
		// With a wakeup channel the poll is only there to catch a lost
		// notification, so it can be slow instead of hot.
		blockPollDefault = 250 * time.Millisecond
	}

	core := &clientCore{
		schema:          newSchema(qualified, opt.PartitionByBucket, opt.TransactionsByBucket),
		autoCreate:      !opt.DisableAutoCreateTable,
		maxConcurrent:   firstPositive(opt.MaxConcurrency, 16),
		casRetries:      firstPositive(opt.CASMaxRetries, 8),
		casBackoff:      firstPositiveDuration(opt.CASRetryBackoff, 2*time.Millisecond),
		maxBatchStmts:   limitOrDefault(opt.MaxBatchStatements, 200),
		scanPageSize:    firstPositive(opt.ScanPageSize, 500),
		maxScanPageSize: firstPositive(opt.MaxScanPageSize, 10_000),
		maxCollection:   limitOrDefault(opt.MaxCollectionScan, 100_000),
		maxValueSize:    limitOrDefault(opt.MaxValueSize, 16<<20),
		blockPollMin:    firstPositiveDuration(opt.BlockingPollInterval, blockPollDefault),
		blockPollMax:    firstPositiveDuration(opt.BlockingPollMaxInterval, 250*time.Millisecond),
		wakeupEnabled:   opt.EnableWakeupChannel,
		wakeupPoll:      firstPositiveDuration(opt.WakeupPollInterval, 20*time.Millisecond),
		sweepLookback:   firstPositiveDuration(opt.SweepLookback, time.Hour),
		initTimeout:     firstPositiveDuration(opt.InitTimeout, 10*time.Second),
		initCooldown:    firstPositiveDuration(opt.InitRetryCooldown, time.Second),
		clock:           opt.clock,
		done:            make(chan struct{}),
	}
	core.maxKeysScan = opt.MaxKeysScan
	if core.maxKeysScan == 0 {
		core.maxKeysScan = core.maxCollection
	}
	core.casBackoffMax = firstPositiveDuration(opt.CASMaxRetryBackoff, 20*core.casBackoff)
	if core.casBackoffMax < core.casBackoff {
		core.casBackoffMax = core.casBackoff
	}
	if core.blockPollMax < core.blockPollMin {
		core.blockPollMax = core.blockPollMin
	}
	core.cursors = newCursorRegistry(
		firstPositive(opt.MaxScanCursors, 1024),
		firstPositiveDuration(opt.ScanCursorTTL, 5*time.Minute),
	)
	core.waiters = newWaiterRegistry()

	client.core = core
	client.bucket = bucket
	return client
}

// Close releases the session created by NewClient and stops any background
// work this client started. Calling Close on a view returned by Bucketed is a
// no-op: views borrow the session of the client they were derived from, which
// stays valid until that client is closed.
func (c *Client) Close() error {
	if c.core == nil || !c.root {
		return nil
	}
	c.core.closeOnce.Do(func() {
		c.core.closed.Store(true)
		close(c.core.done)
		if c.core.ownedSession != nil {
			c.core.ownedSession.Close()
		}
	})
	return nil
}

// Bucketed returns a client view bound to one bucket partition. It shares the
// session, schema initialization state and registries of the parent, so
// creating per tenant views is cheap and does not repeat schema work.
func (c *Client) Bucketed(bucket string) *Client {
	view := &Client{core: c.core, bucket: c.bucket, configErr: c.configErr}
	if view.configErr != nil {
		return view
	}
	if c.core == nil {
		view.configErr = ErrNotInitialized
		return view
	}
	if !c.core.schema.bucketed {
		view.configErr = errors.New("rediscompat: Bucketed requires PartitionByBucket or TransactionsByBucket mode")
		return view
	}
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		view.configErr = errors.New("rediscompat: bucket cannot be empty")
		return view
	}
	view.bucket = bucket
	return view
}

// Bucket reports the partition this client view is bound to.
func (c *Client) Bucket() string { return c.bucket }

// ensureReady validates client state and creates the schema once.
//
// A failure is remembered only for a cooldown: a transient timeout during the
// first request must not make the client permanently unusable, and it must not
// make every concurrent request pile more DDL onto a cluster that just failed
// one. Schema creation runs on a context detached from the caller so one
// cancelled request cannot abort setup for everyone else.
func (c *Client) ensureReady(ctx context.Context) error {
	if c.configErr != nil {
		return c.configErr
	}
	core := c.core
	if core == nil || core.runner == nil {
		return ErrNotInitialized
	}
	if core.closed.Load() {
		return ErrClosed
	}
	if !core.autoCreate || core.initialized.Load() {
		return nil
	}

	core.initMu.Lock()
	defer core.initMu.Unlock()
	if core.initialized.Load() {
		return nil
	}
	if !core.initFailed.IsZero() && c.now().Sub(core.initFailed) < core.initCooldown {
		return fmt.Errorf("rediscompat: schema initialization failed recently, retrying after %s: %w",
			core.initCooldown, ErrNotInitialized)
	}

	initCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), core.initTimeout)
	defer cancel()

	if err := core.schema.create(initCtx, core.runner); err != nil {
		core.initFailed = c.now()
		return fmt.Errorf("rediscompat: schema initialization failed, will retry after %s: %w", core.initCooldown, err)
	}
	core.initFailed = time.Time{}
	core.initialized.Store(true)
	return nil
}

func firstPositive(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func firstPositiveDuration(v, fallback time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return fallback
}

// limitOrDefault resolves a resource ceiling where zero means "use the
// default" and a negative value means "no limit", reported as 0 internally.
func limitOrDefault(v, fallback int) int {
	switch {
	case v > 0:
		return v
	case v < 0:
		return 0
	default:
		return fallback
	}
}
