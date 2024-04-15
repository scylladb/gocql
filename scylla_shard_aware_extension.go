package gocql

import (
	"errors"
	"fmt"
	"sort"
)

// ShardAwareRoutingInfo - information about the routing of the request (host and shard on which the request must be made).
// This information will help group requests (or keys) into batches by host and/or shard.

type ShardAwareRoutingInfo struct {
	// RoutingKey - is bytes of primary key
	RoutingKey []byte
	// Host - is node to connect (HostAware policy)
	Host *HostInfo
	// Shard - is shard ID of node to connect (ShardAware policy)
	Shard int
}

// GetShardAwareRoutingInfo - identifies a node/shard by PK key.
//
// The driver may not always receive routing information and this is normal.
// In this case, the function will return an error and your application needs to process it normally.
//
// Example for SELECT WHERE IN:
/*
	const shardsAbout = 100 // node * (cpu-1)

	// Split []T by chunks
	var (
		queryBatches = make(map[string][]T, shardsAbout) // []T grouped by chunks
		routingKeys  = make(map[string][]byte, shardsAbout) // routing key for query
	)

	for _, pk := range pks {
		var (
			shardID string
			routingKey []byte
		)
		// We receive information about the routing of our keys.
		// In this example, PRIMARY KEY consists of one column pk_column_name.
		info, err := session.GetShardAwareRoutingInfo(keyspaceName, tableName, []string{"pk_column_name"}, pk)
		if err != nil || info.Host == nil {
			// We may not get routing information for various reasons (change shema topology, etc).
			// It is important to understand the reason when testing (for example, you are not using tokenAwarePolicy)
			log.Printf("can't get shard id of pk '%d': %v", pk, err)
		} else {
			// build key: host + "/" + vShard (127.0.0.1/1)
			shardID = info.Host.Hostname() + "/" + strconv.Itoa(info.Shard)
			routingKey = info.RoutingKey
		}

		// Put key to corresponding batch
		batch := queryBatches[shardID]
		if batch == nil {
			batch = make([]int64, 0, len(pks)/shardsAbout)
		}
		batch = append(batch, pk)

		queryBatches[shardID] = batch
		routingKeys[shardID] = rk
	}

	const query = "SELECT * FROM table_name WHERE pk IN (?)"

	var wg sync.WaitGroup
	// we go through all the batches to execute queries in parallel
	for shard, batch := range batches {
		// We divide large batches into smaller chunks, since large batches in SELECT queries have a bad effect on RT scylla
		for _, chunk := range slices.ChunkSlice(batch, 10) { // slices.ChunkSlice some function that splits slice by N slices of M or less lenght (in our example M=10)
			wg.Add(1)
			go func(shard string, chunk []int64) {
				defer wg.Done()

				rk := keys[shard] // get our routing key

				scanner := r.session.Query(query, chunk).RoutingKey(rk).Iter().Scanner() // use RoutingKey

				for scanner.Next() {
					// ...
				}

				if err := scanner.Err(); err != nil {
					// ...
				}
			}(shard, chunk)
		}
	}
	// wait for all answers
	wg.Wait()
	// NOTE: this is not the most optimal strategy 'cause we're waiting for all queries done.
	// If at least one query has long response time it will affects on the response time of our method. (RT our method = max RT of queries)
    // The best approach is to build pipeline handling your results using golang channels and so on...
*/
func (s *Session) GetShardAwareRoutingInfo(table string, colums []string, values ...interface{}) (ShardAwareRoutingInfo, error) {
	keyspace := s.cfg.Keyspace

	// fail fast
	if len(keyspace) == 0 || len(table) == 0 || len(colums) == 0 || len(values) == 0 {
		return ShardAwareRoutingInfo{}, errors.New("missing keyspace, table, columns or values")
	}

	// check that host policy is TokenAwareHostPolicy
	tokenAwarePolicy, ok := s.policy.(*tokenAwareHostPolicy)
	if !ok {
		// host policy is not TokenAwareHostPolicy
		return ShardAwareRoutingInfo{}, fmt.Errorf("unsupported host policy type %T, must be tokenAwareHostPolicy", s.policy)
	}

	// get keyspace metadata
	keyspaceMetadata, err := s.KeyspaceMetadata(keyspace)
	if err != nil {
		return ShardAwareRoutingInfo{}, fmt.Errorf("can't get keyspace %v metadata", keyspace)
	}

	// get table metadata
	tableMetadata, ok := keyspaceMetadata.Tables[table]
	if !ok {
		return ShardAwareRoutingInfo{}, fmt.Errorf("table %v metadata not found", table)
	}

	// get token metadata
	tokenMetadata := tokenAwarePolicy.getMetadataReadOnly()
	if tokenMetadata == nil || tokenMetadata.tokenRing == nil {
		return ShardAwareRoutingInfo{}, errors.New("can't get token ring metadata")
	}

	// get routing key
	routingKey, err := getRoutingKey(tableMetadata.PartitionKey, s.connCfg.ProtoVersion, s.logger, colums, values...)
	if err != nil {
		return ShardAwareRoutingInfo{}, err
	}

	// get token from partition key
	token := tokenMetadata.tokenRing.partitioner.Hash(routingKey)
	mm3token, ok := token.(int64Token) // check if that's murmur3 token
	if !ok {
		return ShardAwareRoutingInfo{}, fmt.Errorf("unsupported token type %T, must be int64Token", token)
	}

	// get hosts by token
	var hosts []*HostInfo
	if ht := tokenMetadata.replicas[keyspace].replicasFor(mm3token); ht != nil {
		hosts = make([]*HostInfo, len(ht.hosts))
		copy(hosts, ht.hosts) // need copy because of later we will sort hosts
	} else {
		host, _ := tokenMetadata.tokenRing.GetHostForToken(mm3token)
		hosts = []*HostInfo{host}
	}

	getHostTier := func(h *HostInfo) uint {
		if tierer, tiererOk := tokenAwarePolicy.fallback.(HostTierer); tiererOk { // e.g. RackAware
			return tierer.HostTier(h)
		} else if tokenAwarePolicy.fallback.IsLocal(h) { // e.g. DCAware
			return 0
		} else { // e.g. RoundRobin
			return 1
		}
	}

	// sortable hosts according to the host policy (e.g. local DC places first, then the rest)
	sort.Slice(hosts, func(i, j int) bool {
		return getHostTier(hosts[i]) < getHostTier(hosts[j])
	})

	// select host
	for _, host := range hosts {
		if !host.IsUp() {
			// host is not ready to accept our query, skip it
			s.logger.Printf("GetShardAwareRoutingInfo: skip host %s: host is not ready", host.Hostname())
			continue
		}

		// get host connection pool
		pool, ok := s.pool.getPool(host)
		if !ok {
			s.logger.Printf("GetShardAwareRoutingInfo: skip host %s: can't get host connection pool", host.Hostname())
			continue
		}

		// check that connection pool is scylla pool
		cp, ok := pool.connPicker.(*scyllaConnPicker)
		if !ok {
			s.logger.Printf("GetShardAwareRoutingInfo: skip host %s: unsupported connection picker type %T, must be scyllaConnPicker", host.Hostname(), pool.connPicker)
			continue
		}

		// return Shard Aware info
		return ShardAwareRoutingInfo{
			RoutingKey: routingKey,           // routing key
			Host:       host,                 // host by key (for HostAware policy)
			Shard:      cp.shardOf(mm3token), // calculate shard id (for ShardAware policy)
		}, nil
	}

	return ShardAwareRoutingInfo{}, fmt.Errorf("no avilable hosts for token %d", mm3token)
}

func getRoutingKey(
	partitionKey []*ColumnMetadata,
	protoVersion int,
	logger StdLogger,
	columns []string,
	values ...interface{},
) ([]byte, error) {
	var (
		indexes = make([]int, len(partitionKey))
		types   = make([]TypeInfo, len(partitionKey))
	)
	for keyIndex, keyColumn := range partitionKey {
		// set an indicator for checking if the mapping is missing
		indexes[keyIndex] = -1

		// find the column in the query info
		for argIndex, boundColumnName := range columns {
			if keyColumn.Name == boundColumnName {
				// there may be many such bound columns, pick the first
				indexes[keyIndex] = argIndex
				types[keyIndex] = getCassandraTypeWithVersion(keyColumn.Type, logger, byte(protoVersion))
				break
			}
		}

		if indexes[keyIndex] == -1 {
			// missing a routing key column mapping
			// no routing key
			return nil, errors.New("missing a routing key column")
		}
	}

	// create routing key
	return createRoutingKey(&routingKeyInfo{
		indexes: indexes,
		types:   types,
	}, values)
}
