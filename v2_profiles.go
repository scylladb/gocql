package gocql

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

type Options struct {
	connection *Conn
	// IncludeMetadata requests full result metadata even when a validated cache exists.
	IncludeMetadata      bool
	Profile              *ProfileHandle
	Consistency          Option[Consistency]
	SerialConsistency    Option[Option[Consistency]]
	Timeout, PageTimeout Option[time.Duration]
	PageSize             Option[int32]
	Prefetch             Option[int]
	Idempotent           bool
	Timestamp            Option[int64]
	NowInSeconds         Option[int32]
	Keyspace             string
	Trace                bool
	CustomPayload        []PayloadEntry
	HostID               Option[UUID]
	ShardID              Option[uint32]
	Retry                RetryPolicy
	Observer             Observer
}
type Profile struct {
	Consistency          Consistency
	SerialConsistency    Option[Consistency]
	Timeout, PageTimeout time.Duration
	PageSize             int32
	Prefetch             int
	Retry                RetryPolicy
	Routing              RoutingPolicy
	Speculation          SpeculationPolicy
}
type ProfileHandle struct{ current atomic.Pointer[Profile] }

func NewProfile(profile Profile) (*ProfileHandle, error) {
	p := new(ProfileHandle)
	if err := p.Replace(profile); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *ProfileHandle) Snapshot() Profile {
	if p == nil || p.current.Load() == nil {
		return defaultProfile()
	}
	return *p.current.Load()
}
func (p *ProfileHandle) Replace(profile Profile) error {
	if p == nil {
		return typeValidation("profile", "nil handle")
	}
	if err := validateProfile(profile); err != nil {
		return err
	}
	p.current.Store(&profile)
	return nil
}
func defaultProfile() Profile {
	return Profile{Consistency: LocalQuorum, SerialConsistency: Option[Consistency]{Value: LocalSerial, Set: true}, Timeout: 30 * time.Second, PageSize: 5000, Prefetch: 1, Retry: SafeRetryPolicy{MaxAttempts: 3}}
}
func validateProfile(p Profile) error {
	if !validConsistency(p.Consistency) {
		return typeValidation("consistency", "invalid consistency")
	}
	if p.SerialConsistency.Set && p.SerialConsistency.Value != Serial && p.SerialConsistency.Value != LocalSerial {
		return typeValidation("serial consistency", "must be SERIAL or LOCAL_SERIAL")
	}
	if p.Timeout < 0 || p.PageTimeout < 0 {
		return typeValidation("timeout", "must not be negative")
	}
	if p.PageSize <= 0 {
		return typeValidation("page size", "must be positive")
	}
	if p.Prefetch < 0 || p.Prefetch > 1 {
		return typeValidation("prefetch", "must be zero or one")
	}
	return nil
}
func validConsistency(c Consistency) bool { return c >= Any && c <= LocalOne }

type resolvedOptions struct {
	profile Profile
	options Options
}

func resolveOptions(s *Session, options Options) (resolvedOptions, error) {
	p := defaultProfile()
	if s != nil {
		rt := s.typedRuntime()
		if rt.profile != nil {
			p = rt.profile.Snapshot()
		}
	}
	if options.Profile != nil {
		p = options.Profile.Snapshot()
	}
	if options.Consistency.Set {
		p.Consistency = options.Consistency.Value
	}
	if options.SerialConsistency.Set {
		p.SerialConsistency = options.SerialConsistency.Value
	}
	if options.Timeout.Set {
		p.Timeout = options.Timeout.Value
	}
	if options.PageTimeout.Set {
		p.PageTimeout = options.PageTimeout.Value
	}
	if options.PageSize.Set {
		p.PageSize = options.PageSize.Value
	}
	if options.Prefetch.Set {
		p.Prefetch = options.Prefetch.Value
	}
	if options.Retry != nil {
		p.Retry = options.Retry
	}
	if err := validateProfile(p); err != nil {
		return resolvedOptions{}, err
	}
	if options.ShardID.Set && !options.HostID.Set {
		return resolvedOptions{}, typeValidation("shard", "requires a host ID")
	}
	keys := make(map[string]bool, len(options.CustomPayload))
	for _, entry := range options.CustomPayload {
		if len(entry.Key) > 65535 || keys[entry.Key] {
			return resolvedOptions{}, typeValidation("custom payload", "invalid or duplicate key")
		}
		keys[entry.Key] = true
		if entry.Value.Valid {
			if err := entry.Value.Value.checkAlive(); err != nil {
				return resolvedOptions{}, err
			}
		}
	}
	options.CustomPayload = append([]PayloadEntry(nil), options.CustomPayload...)
	return resolvedOptions{profile: p, options: options}, nil
}

type RoutingInfo struct {
	Keyspace, Table string
	Token           Option[int64]
	Conditional     bool
	Consistency     Consistency
	HostID          Option[UUID]
	ShardID         Option[uint32]
}
type Target struct {
	HostID  UUID
	ShardID Option[uint32]
}
type QueryPlan interface{ Next() (Target, bool) }
type RoutingPolicy interface {
	Validate(*MetadataSnapshot) error
	Plan(RoutingInfo, *MetadataSnapshot) QueryPlan
	Observe(AttemptEvent)
}
type RetryDecision struct {
	Action      RetryAction
	Consistency Option[Consistency]
	Delay       time.Duration
}
type RetryAction uint8

const (
	StopRetry RetryAction = iota
	RetrySame
	RetryNext
	IgnoreWriteError
)

type RetryPolicy interface {
	Decide(AttemptError) RetryDecision
}
type SpeculationPolicy interface {
	Delay(attempt int, metrics MetricsSnapshot) (time.Duration, bool)
}
type AttemptError struct {
	Err          error
	Attempt      int
	Idempotent   bool
	Consistency  Consistency
	Conditional  bool
	BytesWritten bool
}
type Observer interface {
	ObserveRequest(RequestEvent)
	ObserveAttempt(AttemptEvent)
}
type RequestEvent struct {
	ID              uint64
	Statement       string
	BatchStatements []string
	Parameters      Columns
	Routing         RoutingInfo
	Result          Result
	Err             error
}
type AttemptEvent struct {
	RequestID              uint64
	Attempt                int
	Target                 Target
	Started                time.Time
	Latency                time.Duration
	Err                    error
	Cancelled, Speculative bool
}

// SafeRetryPolicy retries bounded transient failures only when replay is safe.
type SafeRetryPolicy struct{ MaxAttempts int }

func (p SafeRetryPolicy) Decide(a AttemptError) RetryDecision {
	limit := p.MaxAttempts
	if limit <= 0 {
		limit = 3
	}
	if a.Attempt >= limit || errors.Is(a.Err, context.Canceled) || errors.Is(a.Err, context.DeadlineExceeded) {
		return RetryDecision{}
	}
	var unavailable *RequestErrUnavailable
	if errors.As(a.Err, &unavailable) {
		return RetryDecision{Action: RetryNext}
	}
	var read *RequestErrReadTimeout
	if errors.As(a.Err, &read) && read.Received >= read.BlockFor && read.DataPresent == 0 {
		return RetryDecision{Action: RetrySame}
	}
	if !a.Idempotent {
		return RetryDecision{}
	}
	var server RequestError
	if errors.As(a.Err, &server) {
		switch server.GetCode() {
		case ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeServer:
			return RetryDecision{Action: RetryNext}
		default:
			return RetryDecision{}
		}
	}
	var schema *SchemaChangedError
	var validation *ValidationError
	var protocol *ProtocolError
	if errors.As(a.Err, &schema) || errors.As(a.Err, &validation) || errors.As(a.Err, &protocol) {
		return RetryDecision{}
	}
	return RetryDecision{Action: RetryNext}
}
