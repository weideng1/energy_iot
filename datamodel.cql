
CREATE KEYSPACE metrics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE metrics.daily_rollups (
    device_id text,
    metric_day timestamp,
    metric_avg decimal,
    metric_max decimal,
    metric_min decimal,
    metric_name text,
    PRIMARY KEY (device_id, metric_day)
) WITH CLUSTERING ORDER BY (metric_day DESC)
    AND bloom_filter_fp_chance = 0.01
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 315360000
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE metrics.raw_metrics (
    device_id text,
    metric_time timestamp,
    metric_name text,
    metric_value decimal,
    PRIMARY KEY (device_id, metric_time)
) WITH CLUSTERING ORDER BY (metric_time DESC)
    AND bloom_filter_fp_chance = 0.01
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 2592000
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
