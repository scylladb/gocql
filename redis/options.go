package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/gocql/gocql"
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
	// Keyspace containing the kv table. Required unless Table is fully
	// qualified or an existing Session is supplied.
	Keyspace string
	// Table name for kv storage. Default: redis_compat_kv.
	// If it includes '.', it is treated as fully qualified keyspace.table.
	// Keyspace and table must be plain CQL identifiers.
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

	// PartitionByBucket stores every key under a bucket partition key. This is
	// a routing decision (for example one partition per tenant) and is
	// independent of atomicity. Note that it changes the physical schema and
	// scopes key enumeration to the active bucket, and that a single bucket is
	// a single partition: size buckets so they stay within partition limits.
	PartitionByBucket bool
	// Bucket selects the partition used when PartitionByBucket is enabled.
	// Default: "default". Use Bucketed to derive per bucket views.
	Bucket string

	// AtomicMSetByBucket makes MSet atomic through a lightweight transaction.
	// It implies PartitionByBucket because the batch must stay within one
	// partition. Atomicity covers MSet against MSet only; see the package
	// documentation.
	AtomicMSetByBucket bool
	// AtomicBucket is a deprecated alias for Bucket.
	AtomicBucket string
	// AtomicMSetMaxRetries controls CAS conflict retries for atomic MSet.
	// Default: 16.
	//
	// The bucket guard admits one writer per round, so a burst of N concurrent
	// atomic MSets in the same bucket needs a budget of roughly N. Raise this
	// for hot buckets, or spread writes over more buckets.
	AtomicMSetMaxRetries int
	// AtomicMSetRetryBackoff controls initial backoff between retries.
	// Default: 5ms.
	AtomicMSetRetryBackoff time.Duration
	// AtomicMSetMaxPairs caps how many pairs one atomic MSet may carry, so a
	// large argument list cannot build a batch that overloads the coordinator.
	// Default: 100.
	AtomicMSetMaxPairs int

	// MaxConcurrency bounds the in flight requests a single multi key command
	// (MGet, Del, Exists, HSet, SAdd...) may issue. Default: 16.
	MaxConcurrency int
	// CASMaxRetries bounds compare-and-set retries for contended single key
	// mutations such as Incr and Append. Default: 8.
	CASMaxRetries int
	// CASRetryBackoff is the initial backoff between CAS retries.
	// Default: 2ms.
	CASRetryBackoff time.Duration

	// ScanPageSize is the server side page size used by Keys and Scan.
	// Default: 500.
	ScanPageSize int
	// MaxKeysScan caps how many keys a single Keys call may accumulate.
	// Zero means unlimited. Default: 0.
	MaxKeysScan int
	// MaxScanCursors bounds how many live Scan cursors are retained.
	// Default: 1024.
	MaxScanCursors int
	// ScanCursorTTL is how long an idle Scan cursor stays valid.
	// Default: 5m.
	ScanCursorTTL time.Duration

	// BlockingPollInterval is the first delay between BLPop/BRPop polls.
	// Default: 5ms.
	BlockingPollInterval time.Duration
	// BlockingPollMaxInterval caps the poll delay after exponential backoff.
	// Default: 250ms.
	BlockingPollMaxInterval time.Duration

	// InitTimeout bounds automatic schema creation. Schema initialization runs
	// on a context detached from the first caller, so a cancelled request
	// cannot leave the client permanently unusable. Default: 10s.
	InitTimeout time.Duration

	// DisableKeyTypeRegistry turns off the key type column. This removes one
	// read on the first write to a cold key, at the cost of WRONGTYPE
	// detection and of Del/Exists/Keys seeing hash, set and list keys.
	DisableKeyTypeRegistry bool
	// KeyTypeCacheSize bounds the in process key type cache. Default: 4096.
	KeyTypeCacheSize int
}

// clientCore holds everything shared between a client and the bucket views
// derived from it: the session, schema, caches and initialization state.
type clientCore struct {
	runner       queryRunner
	session      *gocql.Session
	ownedSession *gocql.Session
	schema       *schema

	autoCreate       bool
	enforceTypes     bool
	atomicMSet       bool
	atomicRetries    int
	atomicBackoff    time.Duration
	atomicBackoffMax time.Duration
	atomicMaxPairs   int

	maxConcurrency int
	casRetries     int
	casBackoff     time.Duration
	scanPageSize   int
	maxKeysScan    int
	blockPollMin   time.Duration
	blockPollMax   time.Duration
	initTimeout    time.Duration

	types   *typeCache
	cursors *cursorRegistry

	initMu      sync.Mutex
	initialized atomic.Bool
	closed      atomic.Bool
	closeOnce   sync.Once

	// guardBuckets records which buckets already have an atomic MSet guard row.
	guardBuckets sync.Map
}

// Client is a Redis compatible facade over a Scylla keyspace.
//
// A Client is safe for concurrent use. Bucket views created with Bucketed
// share the underlying session, schema and caches with the client they came
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

	bucketed := opt.PartitionByBucket || opt.AtomicMSetByBucket
	bucket := strings.TrimSpace(opt.Bucket)
	if bucket == "" {
		bucket = strings.TrimSpace(opt.AtomicBucket)
	}
	if bucket == "" {
		bucket = "default"
	}

	core := &clientCore{
		schema:         newSchema(qualified, bucketed),
		autoCreate:     !opt.DisableAutoCreateTable,
		enforceTypes:   !opt.DisableKeyTypeRegistry,
		atomicMSet:     opt.AtomicMSetByBucket,
		atomicRetries:  firstPositive(opt.AtomicMSetMaxRetries, 16),
		atomicBackoff:  firstPositiveDuration(opt.AtomicMSetRetryBackoff, 5*time.Millisecond),
		atomicMaxPairs: firstPositive(opt.AtomicMSetMaxPairs, 100),
		maxConcurrency: firstPositive(opt.MaxConcurrency, 16),
		casRetries:     firstPositive(opt.CASMaxRetries, 8),
		casBackoff:     firstPositiveDuration(opt.CASRetryBackoff, 2*time.Millisecond),
		scanPageSize:   firstPositive(opt.ScanPageSize, 500),
		maxKeysScan:    max(opt.MaxKeysScan, 0),
		blockPollMin:   firstPositiveDuration(opt.BlockingPollInterval, 5*time.Millisecond),
		blockPollMax:   firstPositiveDuration(opt.BlockingPollMaxInterval, 250*time.Millisecond),
		initTimeout:    firstPositiveDuration(opt.InitTimeout, 10*time.Second),
	}
	if core.blockPollMax < core.blockPollMin {
		core.blockPollMax = core.blockPollMin
	}
	// Guard contention resolves in milliseconds, so the retry delay is capped
	// well below the blocking poll ceiling: a long backoff here only widens the
	// window for the next writer to jump the queue.
	core.atomicBackoffMax = 20 * core.atomicBackoff
	if core.enforceTypes {
		core.types = newTypeCache(firstPositive(opt.KeyTypeCacheSize, 4096))
	}
	core.cursors = newCursorRegistry(
		firstPositive(opt.MaxScanCursors, 1024),
		firstPositiveDuration(opt.ScanCursorTTL, 5*time.Minute),
	)

	client.core = core
	client.bucket = bucket
	return client
}

// Close releases the session created by NewClient. Calling Close on a view
// returned by Bucketed is a no-op: views borrow the session of the client they
// were derived from, which stays valid until that client is closed.
func (c *Client) Close() error {
	if c.core == nil || !c.root {
		return nil
	}
	c.core.closeOnce.Do(func() {
		c.core.closed.Store(true)
		if c.core.ownedSession != nil {
			c.core.ownedSession.Close()
		}
	})
	return nil
}

// Bucketed returns a client view bound to one bucket partition. It shares the
// session, schema initialization state and caches of the parent, so creating
// per tenant views is cheap and does not repeat schema work.
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
		view.configErr = errors.New("rediscompat: Bucketed requires PartitionByBucket or AtomicMSetByBucket mode")
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
// Initialization failures are not cached: a transient timeout during the first
// request must not make the client permanently unusable, so the next call
// retries. Schema creation also runs on a context detached from the caller so
// one cancelled request cannot abort setup for everyone else.
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

	initCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), core.initTimeout)
	defer cancel()

	if err := core.schema.create(initCtx, core.runner); err != nil {
		return fmt.Errorf("rediscompat: schema initialization failed, will retry on next call: %w", err)
	}
	core.initialized.Store(true)
	return nil
}

// validateKey rejects keys reserved for internal bookkeeping.
func validateKey(key string) error {
	if key == guardKey {
		return ErrReservedKey
	}
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
