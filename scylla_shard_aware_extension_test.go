//go:build all || unit
// +build all unit

package gocql

import (
	"math"
	"reflect"
	"sync/atomic"
	"testing"
)

type partitionerMock struct {
	t token
}

func (pm partitionerMock) Name() string {
	return "mock"
}

func (pm partitionerMock) Hash([]byte) token {
	return pm.t
}

func (pm partitionerMock) ParseString(string) token {
	return pm.t
}

func Benchmark_GetShardAwareRoutingInfo(b *testing.B) {
	const (
		keyspaceName     = "keyspace"
		tableName        = "table"
		partitionKeyName = "pk_column"
		host1ID          = "host1"
		host2ID          = "host2"
		protoVersion     = 4
	)

	type any = interface{} // remove in go 1.18+

	tt := struct {
		schemaDescriber *schemaDescriber
		connCfg         *ConnConfig
		pool            *policyConnPool
		policy          HostSelectionPolicy
		isClosed        bool
	}{
		policy: &tokenAwareHostPolicy{
			fallback: RoundRobinHostPolicy(),
			metadata: func(val any) atomic.Value {
				av := atomic.Value{}
				av.Store(val)
				return av
			}(&clusterMeta{
				tokenRing: &tokenRing{
					partitioner: partitionerMock{t: scyllaCDCMinToken},
					tokens: []hostToken{
						{
							token: scyllaCDCMinToken,
							host: &HostInfo{
								hostId: host1ID,
								state:  NodeUp,
							},
						},
						{
							token: scyllaCDCMinToken,
							host: &HostInfo{
								hostId: host2ID,
								state:  NodeDown,
							},
						},
					},
				},
				replicas: map[string]tokenRingReplicas{
					keyspaceName: {
						{
							token: scyllaCDCMinToken,
							hosts: []*HostInfo{
								{
									hostId: host1ID,
									state:  NodeUp,
								},
								{
									hostId: host2ID,
									state:  NodeDown,
								},
							},
						},
					},
				},
			},
			),
		},
		connCfg: &ConnConfig{
			ProtoVersion: protoVersion, // no panic in marshal
		},
		schemaDescriber: &schemaDescriber{
			cache: map[string]*KeyspaceMetadata{
				keyspaceName: {
					Tables: map[string]*TableMetadata{
						tableName: {
							PartitionKey: []*ColumnMetadata{
								{
									Name: partitionKeyName,
									Type: "int",
								},
							},
						},
					},
				},
			},
		},
		pool: &policyConnPool{
			hostConnPools: map[string]*hostConnPool{
				host1ID: {
					connPicker: &scyllaConnPicker{
						nrShards:  1,
						msbIgnore: 1,
					},
				},
			},
		},
	}

	s := &Session{
		schemaDescriber: tt.schemaDescriber,
		connCfg:         tt.connCfg,
		pool:            tt.pool,
		policy:          tt.policy,
		isClosed:        tt.isClosed,
		logger:          Logger,
	}
	s.cfg.Keyspace = keyspaceName

	var (
		columns = []string{partitionKeyName}
		values  = []interface{}{1}
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := s.GetShardAwareRoutingInfo(tableName, columns, values...)
		if err != nil {
			b.Error(err)
		}
	}

}

func TestSession_GetShardAwareRoutingInfo(t *testing.T) {
	type any = interface{} // remove in go 1.18+

	const (
		keyspaceName     = "keyspace"
		tableName        = "table"
		partitionKeyName = "pk_column"
		host1ID          = "host1"
		host2ID          = "host2"
		localDC          = "DC1"
		localRack        = "Rack1"
		nonLocalDC       = "DC2"
		nonlocalRack     = "Rack2"
		protoVersion     = 4
	)
	var (
		keyspaceMetadata = map[string]*KeyspaceMetadata{
			keyspaceName: {
				Tables: map[string]*TableMetadata{
					tableName: {
						PartitionKey: []*ColumnMetadata{
							{
								Name: partitionKeyName,
								Type: "int",
							},
						},
					},
				},
			},
		}

		store = func(val any) atomic.Value {
			av := atomic.Value{}
			av.Store(val)
			return av
		}
	)

	type fields struct {
		schemaDescriber *schemaDescriber
		connCfg         *ConnConfig
		pool            *policyConnPool
		policy          HostSelectionPolicy
		isClosed        bool
	}
	type args struct {
		keyspace              string
		table                 string
		primaryKeyColumnNames []string
		args                  []interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    ShardAwareRoutingInfo
		wantErr bool
	}{
		{
			name: "Test 1. empty keyspace",
			args: args{
				keyspace:              "",
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 2. empty table name",
			args: args{
				keyspace:              keyspaceName,
				table:                 "",
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 3. empty columns name",
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 4. empty values name",
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},

		{
			name: "Test 5. Not token aware policy",
			fields: fields{
				policy: new(dcAwareRR), // not token aware policy
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},

		{
			name: "Test 6.1. Can't get keyspace metadata",
			fields: fields{
				policy:   new(tokenAwareHostPolicy),
				isClosed: true, // closed session
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},

		{
			name: "Test 6.2. Can't get keyspace metadata",
			fields: fields{
				policy: new(tokenAwareHostPolicy),
				schemaDescriber: &schemaDescriber{
					cache: map[string]*KeyspaceMetadata{}, // no keyspace metadata
					session: &Session{
						useSystemSchema: false, // failed to get keyspace metadata
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 7. Can't get table metadata",
			fields: fields{
				policy: new(tokenAwareHostPolicy),
				schemaDescriber: &schemaDescriber{
					cache: map[string]*KeyspaceMetadata{
						keyspaceName: {
							Tables: map[string]*TableMetadata{}, // no table metadata
						},
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 8.1. Can't get token metadata",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: atomic.Value{}, // empty token metadata
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 8.2. Can't get token metadata",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: nil, // no token ring metadata
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 9. Missing partition key column",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{{
								token: scyllaCDCMinToken,
								host: &HostInfo{
									hostId: host1ID,
								},
							}},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{"not_pk_column"},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 10. Can't create routing key",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{{
								token: scyllaCDCMinToken,
								host: &HostInfo{
									hostId: host1ID,
								},
							}},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{math.MaxInt64}, // Marshal error (int64 to cassandra int)
			},
			wantErr: true,
		},
		{
			name: "Test 11. Not Murmur3Partitioner",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: orderedToken("")},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 12. Hosts Provider = TokenRing. Host is Down. No coon pool",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{{
								token: scyllaCDCMinToken,
								host: &HostInfo{
									hostId: host1ID,
									state:  NodeDown,
								},
							}},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 13. Hosts Provider = TokenRing. Host is Up. Empty coon pool",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{{
								token: scyllaCDCMinToken,
								host: &HostInfo{
									hostId: host1ID,
									state:  NodeUp,
								},
							}},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
				pool: &policyConnPool{
					hostConnPools: map[string]*hostConnPool{}, // no hosts
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 14. Hosts Provider = TokenRing. Host is Up. Not scylla conn picke",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{{
								token: scyllaCDCMinToken,
								host: &HostInfo{
									hostId: host1ID,
									state:  NodeUp,
								},
							}},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
				pool: &policyConnPool{
					hostConnPools: map[string]*hostConnPool{
						host1ID: {
							connPicker: newDefaultConnPicker(1), // not scylla conn picker
						},
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: true,
		},
		{
			name: "Test 15. Hosts Provider = TokenRing. Host is Up. OK",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{{
								token: scyllaCDCMinToken,
								host: &HostInfo{
									hostId: host1ID,
									state:  NodeUp,
								},
							}},
						},
					}),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
				pool: &policyConnPool{
					hostConnPools: map[string]*hostConnPool{
						host1ID: {
							connPicker: &scyllaConnPicker{
								nrShards:  1,
								msbIgnore: 1,
							},
						},
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: false,
			want: ShardAwareRoutingInfo{
				RoutingKey: []byte{0, 0, 0, 1},
				Host: &HostInfo{
					hostId: host1ID,
					state:  NodeUp,
				},
				Shard: 0,
			},
		},
		{
			name: "Test 16. Hosts Provider = replicas. DCAware. OK",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					fallback: DCAwareRoundRobinPolicy(localDC),
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{
								{
									token: scyllaCDCMinToken,
									host: &HostInfo{
										hostId: host1ID,
										state:  NodeUp,
									},
								},
								{
									token: scyllaCDCMinToken,
									host: &HostInfo{
										hostId: host2ID,
										state:  NodeDown,
									},
								},
							},
						},
						replicas: map[string]tokenRingReplicas{
							keyspaceName: {
								{
									token: scyllaCDCMinToken,
									hosts: []*HostInfo{
										{
											hostId:     host1ID,
											state:      NodeUp,
											dataCenter: nonLocalDC, // up but not in local DC
										},
										{
											hostId:     host2ID,
											state:      NodeDown,
											dataCenter: localDC, // in local DC but not ready
										},
									},
								},
							},
						},
					},
					),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
				pool: &policyConnPool{
					hostConnPools: map[string]*hostConnPool{
						host1ID: {
							connPicker: &scyllaConnPicker{
								nrShards:  1,
								msbIgnore: 1,
							},
						},
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: false,
			want: ShardAwareRoutingInfo{
				RoutingKey: []byte{0, 0, 0, 1},
				Host: &HostInfo{
					hostId:     host1ID,
					state:      NodeUp,
					dataCenter: nonLocalDC,
				},
				Shard: 0,
			},
		},
		{
			name: "Test 17. Hosts Provider = replicas. DCAware. OK",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					fallback: RackAwareRoundRobinPolicy(localDC, localRack),
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{
								{
									token: scyllaCDCMinToken,
									host: &HostInfo{
										hostId: host1ID,
										state:  NodeUp,
									},
								},
								{
									token: scyllaCDCMinToken,
									host: &HostInfo{
										hostId: host2ID,
										state:  NodeDown,
									},
								},
							},
						},
						replicas: map[string]tokenRingReplicas{
							keyspaceName: {
								{
									token: scyllaCDCMinToken,
									hosts: []*HostInfo{
										{
											hostId:     host1ID,
											state:      NodeUp,
											dataCenter: localDC,
											rack:       nonlocalRack, // in local DC and  not local rack but host is UP
										},
										{
											hostId:     host2ID,
											state:      NodeDown,
											dataCenter: localDC,
											rack:       localRack, // in local DC and local rack but not ready
										},
									},
								},
							},
						},
					},
					),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
				pool: &policyConnPool{
					hostConnPools: map[string]*hostConnPool{
						host1ID: {
							connPicker: &scyllaConnPicker{
								nrShards:  1,
								msbIgnore: 1,
							},
						},
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: false,
			want: ShardAwareRoutingInfo{
				RoutingKey: []byte{0, 0, 0, 1},
				Host: &HostInfo{
					hostId:     host1ID,
					state:      NodeUp,
					dataCenter: localDC,
					rack:       nonlocalRack,
				},
				Shard: 0,
			},
		},
		{
			name: "Test 18. Hosts Provider = replicas. RoundRobin. OK",
			fields: fields{
				policy: &tokenAwareHostPolicy{
					fallback: RoundRobinHostPolicy(),
					metadata: store(&clusterMeta{
						tokenRing: &tokenRing{
							partitioner: partitionerMock{t: scyllaCDCMinToken},
							tokens: []hostToken{
								{
									token: scyllaCDCMinToken,
									host: &HostInfo{
										hostId: host1ID,
										state:  NodeUp,
									},
								},
								{
									token: scyllaCDCMinToken,
									host: &HostInfo{
										hostId: host2ID,
										state:  NodeDown,
									},
								},
							},
						},
						replicas: map[string]tokenRingReplicas{
							keyspaceName: {
								{
									token: scyllaCDCMinToken,
									hosts: []*HostInfo{
										{
											hostId: host1ID,
											state:  NodeUp,
										},
										{
											hostId: host2ID,
											state:  NodeDown,
										},
									},
								},
							},
						},
					},
					),
				},
				connCfg: &ConnConfig{
					ProtoVersion: protoVersion, // no panic in marshal
				},
				schemaDescriber: &schemaDescriber{
					cache: keyspaceMetadata,
				},
				pool: &policyConnPool{
					hostConnPools: map[string]*hostConnPool{
						host1ID: {
							connPicker: &scyllaConnPicker{
								nrShards:  1,
								msbIgnore: 1,
							},
						},
					},
				},
			},
			args: args{
				keyspace:              keyspaceName,
				table:                 tableName,
				primaryKeyColumnNames: []string{partitionKeyName},
				args:                  []interface{}{1},
			},
			wantErr: false,
			want: ShardAwareRoutingInfo{
				RoutingKey: []byte{0, 0, 0, 1},
				Host: &HostInfo{
					hostId: host1ID,
					state:  NodeUp,
				},
				Shard: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Session{
				schemaDescriber: tt.fields.schemaDescriber,
				connCfg:         tt.fields.connCfg,
				pool:            tt.fields.pool,
				policy:          tt.fields.policy,
				isClosed:        tt.fields.isClosed,
				logger:          Logger,
			}
			s.cfg.Keyspace = tt.args.keyspace

			got, err := s.GetShardAwareRoutingInfo(tt.args.table, tt.args.primaryKeyColumnNames, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Session.GetShardAwareRoutingInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Session.GetShardAwareRoutingInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}
