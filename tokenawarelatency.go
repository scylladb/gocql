package gocql

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/klauspost/cpuid/v2"
	"golang.org/x/sys/cpu"
)

const (
	// bucketCount used for measuring latency.
	// each bucket represents 1/DecayPeriod amount of time.
	bucketCount = 64

	// bucketHistoryCount is how many buckets are not the current one.
	bucketHistoryCount = bucketCount - 1
)

// TokenAwareLatencyHostPolicyOptions contains configuration for TokenAwareLatencyHostPolicy.
type TokenAwareLatencyHostPolicyOptions struct {
	// Logger to use.
	// If not provided, the global Logger is used.
	Logger StdLogger

	// ExplorationPortion is a portion of queries (number (0..1)) to use for exploration.
	// We need some portion of queries to determine the latency of other nodes.
	// If not set, defaults to 0.1 (10%).
	ExplorationPortion float32

	// DecayPeriod is duration over which the sampled latency values are stored.
	// The weight of sampled values is linear over this time.
	// Defaults to 5 minutes.
	DecayPeriod time.Duration

	// LocalDatacenter is name of the local datacenter.
	LocalDatacenter string

	// RemoteDatacenterPenalty is latency duration that is added to the measurement of nodes in remote datacenter.
	// In other words, how much less latency a remote node must have to consider switching to it instead of a node
	// in local DC. Can be used to prefer local DC in case cross-DC traffic costs more money or if the latency is
	// too variable.
	RemoteDatacenterPenalty time.Duration
}

// TokenAwareLatencyHostPolicy is a token aware host selection policy that takes latency into account.
//
// TokenAwareLatencyHostPolicy uses weighted average of query latencies for each host to pick the best
// node for a query. Latency is tracked over a configured decay period and the weight of samples decreases
// linearly with age. The policy uses Epsilon-greedy strategy: Most of the time, we send queries to the best node
// as determined by latency. Small portion if queries (configured by ExplorationPortion) is sent to random replica
// so that can measure query latency for other nodes as well.
//
// Hosts are primarily picked among replicas corresponding to the given token based on latency.
// Secondarily, other hosts are considered based on latency.
// If we don't have latency information for a host, it's considered after hosts for which we have latency data
// (as if the latency for that host was infinite). If we need to select among multiple hosts without latency data,
// hosts in local datacenter are preferred.
type TokenAwareLatencyHostPolicy struct {
	// getKeyspaceMetadata is used to mock Session.KeyspaceMetadata in tests.
	getKeyspaceMetadata func(keyspace string) (*KeyspaceMetadata, error)
	// afterReplicasUpdates is called after replicas are updated, used only for tests.
	afterReplicasUpdated func(keyspace string)

	logger StdLogger
	// explorationPortion is a portion of queries (number 0..1) to use for exploration.
	// We need some portion of queries to determine the latency of other nodes.
	explorationPortion float32
	// decayPeriod is duration over which the sampled latency values linearly decay.
	decayPeriod time.Duration
	// localDatacenter is name of the local datacenter.
	localDatacenter string
	// remoteDatacenterPenalty is added to the latency of nodes in remote datacenters.
	remoteDatacenterPenalty time.Duration

	// stopChan is closed when Stop is called.
	stopChan chan struct{}
	// stopWG is used to wait for background goroutines to stop.
	stopWG sync.WaitGroup
	// stopMu protects stopWG and stopped.
	stopMu sync.Mutex
	// stopped is true once user called Stop.
	stopped bool

	// mu protects fields below.
	// If you need to hold both TokenAwareLatencyHostPolicy.mu and tokenAwareLatencyHost.mu,
	// TokenAwareLatencyHostPolicy.mu must be locked before tokenAwareLatencyHost.mu.
	mu sync.RWMutex

	// hosts by HostID().
	// contents of tokenAwareLatencyHost are not protected by mu as it has its own lock.
	hosts map[string]*tokenAwareLatencyHost
	// keyspaces by name.
	keyspaces map[string]*tokenAwareLatencyKeyspace
	// tokenRing to use for mapping tokens to hosts.
	tokenRing *tokenRing
	// partitioner used to map partition keys to tokens.
	partitioner string
}

// NewTokenAwareLatencyHostPolicy creates a new TokenAwareLatencyHostPolicy.
func NewTokenAwareLatencyHostPolicy(options TokenAwareLatencyHostPolicyOptions) (*TokenAwareLatencyHostPolicy, error) {
	if options.Logger == nil {
		options.Logger = Logger
	}
	if options.ExplorationPortion == 0 {
		options.ExplorationPortion = 0.1
	}
	if options.ExplorationPortion <= 0 || options.ExplorationPortion > 1 {
		return nil, fmt.Errorf("tokenawarelatency: ExplorationPortion must be in range (0, 1]")
	}
	if options.DecayPeriod == 0 {
		options.DecayPeriod = 5 * time.Minute
	}
	if options.DecayPeriod <= 0 {
		return nil, fmt.Errorf("tokenawarelatency: DecayPeriod must be positive")
	}
	if options.RemoteDatacenterPenalty < 0 {
		return nil, fmt.Errorf("tokenawarelatency: RemoteDatacenterPenalty must be positive")
	}
	if options.RemoteDatacenterPenalty > 0 && options.LocalDatacenter == "" {
		return nil,
			fmt.Errorf("tokenawarelatency: non-zero RemoteDatacenterPenalty requires LocalDatacenter to be set")
	}
	return &TokenAwareLatencyHostPolicy{
		hosts:                   make(map[string]*tokenAwareLatencyHost),
		keyspaces:               make(map[string]*tokenAwareLatencyKeyspace),
		stopChan:                make(chan struct{}),
		logger:                  options.Logger,
		explorationPortion:      options.ExplorationPortion,
		decayPeriod:             options.DecayPeriod,
		localDatacenter:         options.LocalDatacenter,
		remoteDatacenterPenalty: options.RemoteDatacenterPenalty,
	}, nil
}

// Init is called by the driver once to bind the session to the policy.
func (t *TokenAwareLatencyHostPolicy) Init(s *Session) {
	t.getKeyspaceMetadata = s.KeyspaceMetadata
	t.stopMu.Lock()
	defer t.stopMu.Unlock()
	if t.stopped {
		return
	}
	t.stopWG.Add(1)
	go t.decayLoop()
}

// Stop can be used by the user to clean up resources (like goroutines) used by TokenAwareLatencyHostPolicy.
// Stop waits until all resources are cleaned up.
// Features of the policy like background updates / latency measurements will stop working the first time Stop is
// called, subsequent invocations are no-op (but can still be used to wait for being done).
func (t *TokenAwareLatencyHostPolicy) Stop() {
	t.stopMu.Lock()
	if !t.stopped {
		t.stopped = true
		close(t.stopChan)
	}
	t.stopMu.Unlock()
	t.stopWG.Wait()
}

// decayLoop periodically calls decay.
func (t *TokenAwareLatencyHostPolicy) decayLoop() {
	defer t.stopWG.Done()
	ticker := time.NewTicker(t.decayPeriod / bucketCount)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.decay()
		case <-t.stopChan:
			return
		}
	}
}

// decay removes the oldest time bucket and adds a new current one for all hosts.
func (t *TokenAwareLatencyHostPolicy) decay() {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, host := range t.hosts {
		host.decay()
	}
}

// IsLocal returns true if the host is in local datacenter.
func (t *TokenAwareLatencyHostPolicy) IsLocal(host *HostInfo) bool {
	return t.localDatacenter != "" && host.DataCenter() == t.localDatacenter
}

// KeyspaceChanged is called during session initialization and whenever the server sends us event that the keyspace
// metadata has changed.
func (t *TokenAwareLatencyHostPolicy) KeyspaceChanged(update KeyspaceUpdateEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.updateReplicas(update.Keyspace)
}

// SetPartitioner is called by the driver the partitioner used by the cluster is discovered or when it changes.
func (t *TokenAwareLatencyHostPolicy) SetPartitioner(partitioner string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.partitioner == partitioner {
		return
	}

	t.partitioner = partitioner

	// When a partitioner changes, we need to recompute token ring, because the tokens in token ring and token order
	// depend on the partitioner used.
	err := t.resetRing()
	if err != nil {
		t.logger.Printf("reset ring after partitioner change: %v", err)
	}
}

// AddHost adds a single host to the policy after it was discovered/added to the cluster.
// AddHost implements the HostStateNotifier interface embedded in HostSelectionPolicy.
func (t *TokenAwareLatencyHostPolicy) AddHost(host *HostInfo) {
	t.AddHosts([]*HostInfo{host})
}

// AddHosts adds multiple hosts to the policy after they were discovered/added to the cluster.
// AddHosts implements bulkAddHosts interface from session.go.
func (t *TokenAwareLatencyHostPolicy) AddHosts(hosts []*HostInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	added := false

	for _, host := range hosts {
		hostID := host.HostID()
		if hostID == "" {
			continue
		}
		_, ok := t.hosts[hostID]
		if !ok {
			t.hosts[hostID] = &tokenAwareLatencyHost{
				hostInfo: host,
				current:  newBigWriterBucket(),
			}
			added = true
		}
	}

	if !added {
		return
	}

	err := t.resetRing()
	if err != nil {
		t.logger.Printf("reset ring after adding hosts: %v", err)
	}
}

// RemoveHost from the policy after it was removed from the cluster.
func (t *TokenAwareLatencyHostPolicy) RemoveHost(host *HostInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	hostID := host.HostID()
	if hostID == "" {
		return
	}

	if _, ok := t.hosts[hostID]; !ok {
		return
	}

	delete(t.hosts, hostID)
	err := t.resetRing()
	if err != nil {
		t.logger.Printf("reset ring after removing hosts: %v", err)
	}
}

// HostUp is called by the driver when a host changes state to up.
func (t *TokenAwareLatencyHostPolicy) HostUp(host *HostInfo) {
	// no-op. We check host.IsUp() where necessary instead.
}

// HostUp is called by the driver when a host changes state to down.
func (t *TokenAwareLatencyHostPolicy) HostDown(host *HostInfo) {
	// no-op. We check host.IsUp() where necessary instead.
}

type TokenAwareLatencyHostInfo struct {
	// Host information.
	Host *HostInfo

	// Latency currently recorded for this host.
	Latency time.Duration

	// LatencyOk is true if Latency is valid.
	LatencyOk bool
}

// Hosts returns a snapshot of all hosts and their recorded latencies for observability purposes.
func (t *TokenAwareLatencyHostPolicy) Hosts() []TokenAwareLatencyHostInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]TokenAwareLatencyHostInfo, 0, len(t.hosts))
	for _, host := range t.hosts {
		out = append(out, host.export())
	}
	return out
}

// resetRing replaces the token ring with a new one.
// Must be called with t.mu lock held for writing.
func (t *TokenAwareLatencyHostPolicy) resetRing() error {
	newTR, err := newTokenRing(t.partitioner, t.hostInfoSlice())
	if err != nil {
		// If we use unsupported partitioner, set t.tokenRing to nil to make findReplicas always return nil.
		// This will cause queries to go to node with lowest latency selected from whole cluster.
		t.tokenRing = nil
	} else {
		t.tokenRing = newTR
	}

	// Replica maps for keyspaces depend on the token ring used, trigger background reload of all of them.
	for keyspaceName := range t.keyspaces {
		t.updateReplicas(keyspaceName)
	}

	return err
}

// updateReplicas initiates updating of replicas for the given keyspace.
// It must be called while the caller holds t.mu mutex locked for writes.
func (t *TokenAwareLatencyHostPolicy) updateReplicas(keyspace string) {
	ks, ok := t.keyspaces[keyspace]
	if !ok {
		ks = &tokenAwareLatencyKeyspace{}
		t.keyspaces[keyspace] = ks
	}

	ks.needsUpdate = true

	if ks.updating {
		return
	}

	ks.updating = true
	t.stopMu.Lock()
	defer t.stopMu.Unlock()
	if t.stopped {
		return
	}
	t.stopWG.Add(1)
	go t.updateReplicasLoop(ks, keyspace)
}

// updateReplicasLoop performs actual update of replicas for a keyspace.
// updateReplicasLoop must be called in a goroutine not holding the t.mu mutex.
func (t *TokenAwareLatencyHostPolicy) updateReplicasLoop(ks *tokenAwareLatencyKeyspace, keyspaceName string) {
	defer t.stopWG.Done()
	for t.replicasNeedUpdate(ks) {
		// We can't call getKeyspaceMetadata while holding the lock since getKeyspaceMetadata can do I/O.
		metadata, err := t.getKeyspaceMetadata(keyspaceName)
		if err != nil {
			t.logger.Printf("can't get metadata for keyspace %q, skipped replica map update: %v", keyspaceName,
				err)
			continue
		}

		strategy := getStrategy(metadata, t.logger)
		var replicas tokenRingReplicas

		if strategy != nil {
			t.mu.RLock()
			tokenRing := t.tokenRing
			t.mu.RUnlock()
			if tokenRing != nil {
				replicas = strategy.replicaMap(tokenRing)
			}
		}

		t.mu.Lock()
		ks.replicas = replicas
		t.mu.Unlock()
	}
	if t.afterReplicasUpdated != nil {
		t.afterReplicasUpdated(keyspaceName)
	}
}

// replicasNeedUpdate checks if the keyspace needs updating.
// If the keyspace does not need to be updated, it resets the updating flag.
func (t *TokenAwareLatencyHostPolicy) replicasNeedUpdate(ks *tokenAwareLatencyKeyspace) bool {
	t.stopMu.Lock()
	stopped := t.stopped
	t.stopMu.Unlock()
	if stopped {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if ks.needsUpdate {
		ks.needsUpdate = false
		return true
	}
	ks.updating = false
	return false
}

// findReplicas gets replicas for the query.
// If for any reason we can't find replicas, the returned slice's length will be zero.
// t.mu must not be held.
func (t *TokenAwareLatencyHostPolicy) findReplicas(qry ExecutableQuery) (token, []*HostInfo) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if qry == nil {
		return nil, nil
	}

	routingKey, err := qry.GetRoutingKey()
	if err != nil {
		t.logger.Printf("findReplicas: get routing key for query: %v", err)
		return nil, nil
	}
	if routingKey == nil {
		return nil, nil
	}

	if t.tokenRing == nil {
		return nil, nil
	}

	partitioner := qry.GetCustomPartitioner()
	if partitioner == nil {
		partitioner = t.tokenRing.partitioner
	}

	token := partitioner.Hash(routingKey)

	var ringReplicas tokenRingReplicas
	if keyspace, ok := t.keyspaces[qry.Keyspace()]; ok {
		ringReplicas = keyspace.replicas
	}

	// it's safe to call replicasFor on nil value.
	ht := ringReplicas.replicasFor(token)

	if ht == nil {
		// If we don't have information about replicas yet, fall back to primary replica for the token.
		// This can happen before we know the topology strategy for a keyspace.
		host, _ := t.tokenRing.GetHostForToken(token)
		return token, []*HostInfo{host}
	}
	return token, ht.hosts
}

func (t *TokenAwareLatencyHostPolicy) hostsToChooseFrom(used map[*tokenAwareLatencyHost]struct{},
	replicas []*HostInfo) []tokenAwareLatencyHostStats {
	chooseFrom := make([]tokenAwareLatencyHostStats, 0, len(replicas))

	t.mu.RLock()
	defer t.mu.RUnlock()

	// Primarily select among unused replicas based on token.
	for _, replica := range replicas {
		if !replica.IsUp() {
			continue
		}
		h, ok := t.hosts[replica.HostID()]
		if !ok {
			continue
		}
		if _, isUsed := used[h]; isUsed {
			continue
		}
		rv := tokenAwareLatencyHostStats{
			host:     h,
			hostInfo: replica,
		}
		h.mu.RLock()
		rv.latency, rv.latencyOk = h.latency()
		h.mu.RUnlock()
		if t.localDatacenter != "" {
			rv.local = replica.DataCenter() == t.localDatacenter
			if !rv.local {
				rv.latency += t.remoteDatacenterPenalty
			}
		}
		chooseFrom = append(chooseFrom, rv)
	}
	if len(chooseFrom) > 0 {
		return chooseFrom
	}
	// Secondarily select among other unused hosts.
	for _, h := range t.hosts {
		if _, ok := used[h]; ok {
			continue
		}
		rv := tokenAwareLatencyHostStats{
			host: h,
		}
		h.mu.RLock()
		rv.hostInfo = h.hostInfo
		rv.latency, rv.latencyOk = h.latency()
		h.mu.RUnlock()
		if !rv.hostInfo.IsUp() {
			continue
		}
		if t.localDatacenter != "" {
			rv.local = rv.hostInfo.DataCenter() == t.localDatacenter
			if !rv.local {
				rv.latency += t.remoteDatacenterPenalty
			}
		}
		chooseFrom = append(chooseFrom, rv)
	}
	return chooseFrom
}

// hostInfoSlice converts t.hosts to a slice of *HostInfo.
// It must be called with t.mu held for reading.
func (t *TokenAwareLatencyHostPolicy) hostInfoSlice() []*HostInfo {
	hosts := make([]*HostInfo, 0, len(t.hosts))
	for _, host := range t.hosts {
		host.mu.RLock()
		hostInfo := host.hostInfo
		host.mu.RUnlock()
		hosts = append(hosts, hostInfo)
	}
	return hosts
}

func (t *TokenAwareLatencyHostPolicy) Pick(qry ExecutableQuery) NextHost {
	iter := &tokenAwareLatencyHostIterator{
		policy: t,
	}
	iter.token, iter.replicas = t.findReplicas(qry)
	iter.used = make(map[*tokenAwareLatencyHost]struct{}, len(iter.replicas))
	return iter.NextHost
}

type tokenAwareLatencyHostIterator struct {
	policy   *TokenAwareLatencyHostPolicy
	token    token
	replicas []*HostInfo
	used     map[*tokenAwareLatencyHost]struct{}
}

func (hi *tokenAwareLatencyHostIterator) selectHost(host tokenAwareLatencyHostStats) SelectedHost {
	hi.used[host.host] = struct{}{}
	return tokenAwareLatencySelectedHost{
		host:      host.host,
		info:      host.hostInfo,
		token:     hi.token,
		startTime: time.Now(),
	}
}

// len(hosts) must be >1, otherwise this panics.
func (hi *tokenAwareLatencyHostIterator) randomHost(hosts []tokenAwareLatencyHostStats) SelectedHost {
	index := rand.Intn(len(hosts) - 1)
	return hi.selectHost(hosts[index])
}

func (hi *tokenAwareLatencyHostIterator) NextHost() SelectedHost {
	hosts := hi.policy.hostsToChooseFrom(hi.used, hi.replicas)

	switch len(hosts) {
	case 0:
		return nil
	case 1:
		return hi.selectHost(hosts[0])
	}

	if rand.Float32() < hi.policy.explorationPortion {
		// Exploration phase.
		// We send some portion of queries to random nodes so that we have stats on the latency
		// from all nodes.
		return hi.randomHost(hosts)
	}

	// Otherwise we send the query to the best node (if it exists).
	minIndex := -1
	var minLatency time.Duration
	for i := 0; i < len(hosts); i++ {
		if !hosts[i].latencyOk {
			// no data for this node.
			continue
		}
		if minIndex == -1 || hosts[i].latency < minLatency {
			minIndex = i
			minLatency = hosts[i].latency
		}
	}

	if minIndex >= 0 {
		return hi.selectHost(hosts[minIndex])
	}
	// There is no host with min latency (we don't have data).
	// Return the local one or random otherwise.
	for i := range hosts {
		if hosts[i].local {
			return hi.selectHost(hosts[i])
		}
	}
	return hi.randomHost(hosts)
}

type tokenAwareLatencyHostStats struct {
	// host is pointer to our information about the host.
	host *tokenAwareLatencyHost
	// hostInfo of the host.
	// This is a copy of host.hostInfo so that we don't need to lock host multiple times.
	hostInfo *HostInfo
	// latency of the host plus penalty (if applicable).
	latency time.Duration
	// latencyOk indicates whether we have latency data.
	latencyOk bool
	// local indicates whether the host is in the local datacenter.
	local bool
}

// tokenAwareLatencySelectedHost implements SelectedHost for TokenAwareLatencyHostPolicy
type tokenAwareLatencySelectedHost struct {
	host      *tokenAwareLatencyHost
	info      *HostInfo
	token     token
	startTime time.Time
}

func (sh tokenAwareLatencySelectedHost) Info() *HostInfo {
	return sh.info
}

func (sh tokenAwareLatencySelectedHost) Token() token {
	return sh.token
}

func (sh tokenAwareLatencySelectedHost) Mark(err error) {
	latency := time.Since(sh.startTime)
	sh.host.recordLatency(latency)
}

type tokenAwareLatencyKeyspace struct {
	// replicas allows to get replicas for token.
	replicas tokenRingReplicas

	// updating is true if we are currently updating the replica map for this keyspace.
	updating bool

	// needsUpdate is true if something changed while updating the replica map.
	needsUpdate bool
}

// tokenAwareLatencyHost keeps track of per-host state.
type tokenAwareLatencyHost struct {
	// mu protects fields in tokenAwareLatencyHost.
	// If you need to hold both TokenAwareLatencyHostPolicy.mu and tokenAwareLatencyHost.mu,
	// TokenAwareLatencyHostPolicy.mu must be locked before tokenAwareLatencyHost.mu.
	mu sync.RWMutex
	// latencyBuckets is a cyclic buffer of time buckets that store latency measurements for a host.
	// latencyBuckets[newestBucketIndex] is the newest entry, the one to the right (with wrap) is the oldest entry.
	latencyBuckets [bucketHistoryCount]latencyBucket
	// newestBucketIndex points to the newest bucket in latencyBuckets.
	newestBucketIndex int
	// current is the bucket we are writing latency measurements to.
	current bigWriterBucket
	// hostInfo of the host.
	hostInfo *HostInfo
}

// latency returns weighted average t of latency measurements over time.
// If there is no data in any bucket at all, ok is false.
// Weight of samples is linear from oldest bucket (1) to newest bucket (bucketCount).
// th.mu must be held at least read-only.
func (th *tokenAwareLatencyHost) latency() (t time.Duration, ok bool) {
	// Weighted average of a multiset {x_1, x_2, …, x_n} with weights {w_1, w_2, …, w_n} is defined as
	//
	//        w_1 * x_1 + w_2 * x_2 + … + w_n * x_n
	// wAvg = ―――――――――――――――――――――――――――――――――――――
	//                 w_1 + w_2 + … + w_n
	//
	// See https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Mathematical_definition
	//
	// We store multiple samples per bucket, but all samples in a bucket have the same weight.
	// Given bucket weights {bw_1, bw_2, …, bw_m}, the formula thus looks like:
	//
	//        bw_1 * x_1 + bw_1 * x_2 + … + bw_2 * x_k + … + bw_m * x_n
	// wAvg = ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――
	//                    bw_1 + bw_1 + … + bw_2 + … + w_n
	//
	// Which can be expressed as:
	//
	//        bw_1 * (x_1 + x_2 + …) + bw_2 * (x_k + …) + … + bw_m * (… + x_n)
	// wAvg = ――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
	//                        bw_1 + bw_1 + … + bw_2 + … + w_n
	//
	// And if we substitute sum of elements of j-th bucket with bs_j and count of elements of j-th bucket with bc_j:
	//
	//        bw_1 * bs_1 + bw_2 * bs_2 + … + bw_m * bs_m
	// wAvg = ―――――――――――――――――――――――――――――――――――――――――――
	//               bw_1 * bc_1 + … + bw_m * bc_m
	var sum time.Duration
	var sumWeights int64
	for i := 1; i <= bucketHistoryCount; i++ {
		bucketIndex := (th.newestBucketIndex + i) % bucketHistoryCount
		if th.latencyBuckets[bucketIndex].count == 0 {
			continue
		}
		sum += time.Duration(i) * th.latencyBuckets[bucketIndex].sum
		sumWeights += int64(i) * th.latencyBuckets[bucketIndex].count
	}
	currentBucket := th.current.readLocal()
	sum += bucketCount * currentBucket.sum
	sumWeights += bucketCount * currentBucket.count
	if sumWeights == 0 {
		return 0, false
	}
	return sum / time.Duration(sumWeights), true
}

// export a read-only static copy of this host for observability purposes.
func (th *tokenAwareLatencyHost) export() TokenAwareLatencyHostInfo {
	th.mu.RLock()
	defer th.mu.RUnlock()
	lat, ok := th.latency()
	return TokenAwareLatencyHostInfo{
		Host:      th.hostInfo,
		Latency:   lat,
		LatencyOk: ok,
	}
}

// recordLatency tracks the latency data point for this host.
func (th *tokenAwareLatencyHost) recordLatency(latency time.Duration) {
	th.current.add(latency)
}

// decay removes the oldest time bucket and adds a new current one.
func (th *tokenAwareLatencyHost) decay() {
	th.mu.Lock()
	defer th.mu.Unlock()
	// latencyBuckets is a cyclic buffer, move the current index to remove from end and add to beginning.
	th.newestBucketIndex = (th.newestBucketIndex + 1) % bucketHistoryCount
	th.latencyBuckets[th.newestBucketIndex] = th.current.reset()
}

// latencyBucket stores information about average latency in a DecayPeriod/bucketCount long time window.
type latencyBucket struct {
	sum   time.Duration
	count int64
}

// add a sample of latency to the bucket.
func (lb *latencyBucket) add(latency time.Duration) {
	lb.sum += latency
	lb.count += 1
}

// bigWriterBucket keeps counts of latency with reduced lock contention.
type bigWriterBucket struct {
	cpus []bigWriterPerCPU
}

func newBigWriterBucket() bigWriterBucket {
	cores := cpuid.CPU.LogicalCores
	if cores == 0 {
		// fall back to single locked bucket.
		cores = 1
	}
	return bigWriterBucket{
		cpus: make([]bigWriterPerCPU, cores),
	}
}

// selectCPU selects which index to update.
func (bw *bigWriterBucket) selectCPU() int {
	id := cpuid.CPU.LogicalCPU()
	if id == -1 {
		// fall back to single locked bucket.
		id = 0
	}
	// Make sure we don't overflow, e.g. if a CPU is hot-plugged.
	return id % len(bw.cpus)
}

// add adds a latency sample.
func (bw *bigWriterBucket) add(latency time.Duration) {
	id := bw.selectCPU()
	bw.cpus[id].mu.Lock()
	bw.cpus[id].value.add(latency)
	bw.cpus[id].mu.Unlock()
}

// readLocal reads the bucket state for the local CPU only.
func (bw *bigWriterBucket) readLocal() latencyBucket {
	id := bw.selectCPU()
	bw.cpus[id].mu.Lock()
	defer bw.cpus[id].mu.Unlock()
	return bw.cpus[id].value
}

// reset reads the bucket state from all CPUs and resets the bucket to zero.
func (bw *bigWriterBucket) reset() latencyBucket {
	var oldValue latencyBucket
	for i := 0; i < len(bw.cpus); i++ {
		bw.cpus[i].mu.Lock()
		oldValue.sum += bw.cpus[i].value.sum
		oldValue.count += bw.cpus[i].value.count
		bw.cpus[i].value = latencyBucket{}
		bw.cpus[i].mu.Unlock()
	}
	return oldValue
}

type bigWriterPerCPU struct {
	// padding to prevent false sharing of cache lines.
	// https://en.wikipedia.org/wiki/False_sharing
	_ cpu.CacheLinePad
	// mu locks access to the value.
	// If you need to acquire multiple per cpu locks, you need to acquire them in increasing order by index.
	mu sync.Mutex
	// value of the bucket for this CPU.
	value latencyBucket
	// padding to prevent false sharing of cache lines.
	_ cpu.CacheLinePad
}
