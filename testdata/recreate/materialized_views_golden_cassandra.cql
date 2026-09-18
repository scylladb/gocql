CREATE KEYSPACE gocqlx_mv WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '2'} AND durable_writes = true;
CREATE TABLE gocqlx_mv.mv_buildings (
    name text,
    built int,
    city text,
    meters int,
    PRIMARY KEY (name)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND paxos_grace_seconds = 864000
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};
CREATE MATERIALIZED VIEW gocqlx_mv.mv_building_by_city AS
    SELECT city, name, built, meters
    FROM gocqlx_mv.mv_buildings
    WHERE city IS NOT null
    PRIMARY KEY (city, name)
    WITH CLUSTERING ORDER BY (name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND paxos_grace_seconds = 864000
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};
CREATE MATERIALIZED VIEW gocqlx_mv.mv_building_by_city2 AS
    SELECT city, name, meters
    FROM gocqlx_mv.mv_buildings
    WHERE city IS NOT null AND name IS NOT null
    PRIMARY KEY (city, name)
    WITH CLUSTERING ORDER BY (name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND speculative_retry = '99.0PERCENTILE'
    AND paxos_grace_seconds = 864000
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};
