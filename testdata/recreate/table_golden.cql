CREATE KEYSPACE gocqlx_table WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '2'} AND durable_writes = true;
CREATE TABLE gocqlx_table.loads (
    machine inet,
    cpu int,
    mtime timeuuid,
    load float,
    PRIMARY KEY ((machine, cpu), mtime)
) WITH CLUSTERING ORDER BY (mtime DESC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '14', 'compaction_window_unit': 'DAYS'}
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
CREATE TABLE gocqlx_table.monkeyspecies (
    species text,
    average_size int,
    common_name text,
    population varint,
    PRIMARY KEY (species)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = 'Important biological records'
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
CREATE TABLE gocqlx_table.timeline (
    userid uuid,
    posted_month int,
    posted_time uuid,
    body text,
    posted_by text,
    PRIMARY KEY (userid, posted_month, posted_time)
) WITH CLUSTERING ORDER BY (posted_month ASC, posted_time ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND comment = ''
    AND compaction = {'class': 'LeveledCompactionStrategy'}
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
CREATE TABLE gocqlx_table.users_picture (
    userid uuid,
    pictureid uuid,
    posted_by text,
    body text STATIC,
    PRIMARY KEY (userid, pictureid, posted_by)
) WITH CLUSTERING ORDER BY (pictureid ASC, posted_by ASC)
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
