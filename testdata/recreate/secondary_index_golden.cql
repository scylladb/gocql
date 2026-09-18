CREATE KEYSPACE gocqlx_sec_idx WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1'} AND durable_writes = true;
CREATE TABLE gocqlx_sec_idx.menus (
    location text,
    name text,
    dish_type text,
    price float,
    PRIMARY KEY (location, name)
) WITH CLUSTERING ORDER BY (name ASC)
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
CREATE INDEX menus_name_idx ON gocqlx_sec_idx.menus((location), name);
