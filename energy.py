#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import logging
import time
from uuid import UUID
import random
from faker import Factory
import datetime
from time import sleep
import sys

log = logging.getLogger()
log.setLevel('INFO')
fake=Factory.create()
class Config(object):
    cassandra_hosts = sys.argv[2]

def generate_sensor_data():
    time = datetime.datetime.now()
    epochtime = int(time.strftime("%s"))
    start_time = epochtime - 86400
    results = []
    for device in range(1,int(sys.argv[1])+1):
        start_time = epochtime - 86400
        for j in range(1,97):
            results.append([str(device), datetime.datetime.fromtimestamp(start_time), 'KWH', random.uniform(0.1, 1.9)])
            start_time = start_time + 900
    return results

class SimpleClient(object):


    #Instantiate a session object to be used to connect to the database.
    session = None

    #Method to connect to the cluster and print connection info to the console
    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)

    #Close the connection
    def close(self):
        self.session.cluster.shutdown()
        log.info('Connection closed.')

    #Create the schema. This will drop the existing schema when the application is run.

    def create_schema(self):
        self.session.execute("""DROP KEYSPACE IF EXISTS metrics;""")
        self.session.execute("""CREATE KEYSPACE metrics WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;""")
        self.session.execute("""
            CREATE TABLE metrics.raw_metrics (
                device_id text,
                metric_time timestamp,
                metric_name text,
                metric_value float,
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
        """)

        self.session.execute("""
            CREATE TABLE metrics.daily_rollups (
                device_id text,
                metric_day timestamp,
                metric_avg float,
                metric_max float,
                metric_min float,
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
        """)

        log.info('Games keyspace and schema created.')

class BoundStatementsClient(SimpleClient):
    def prepare_statements(self):
        self.insert_metrics = self.session.prepare(
        """
            INSERT INTO metrics.raw_metrics
            (device_id,metric_time,metric_name,metric_value)
            VALUES (?,?,?,?);
        """)

    def load_seed_data(self):
        sensors = generate_sensor_data()
        #load coupon data
        for row in sensors:
            self.session.execute_async(self.insert_metrics,
                [row[0],row[1],row[2],row[3]]
            )

'''
    #load actual data like clips and likes of pds and coupons. in this example just households.
    def run_clips(self):
        #set up kafka producer
        kafka_client = KafkaClient(hosts=Config.kafka_host)
        kafka_topic = kafka_client.topics[Config.kafka_topics]
        kafka_producer = kafka_topic.get_producer()
        for i in range(0,100000):
            for j in range(0,100):
                row_zip = str(random.randint(90000,90099))
                row_offer_id = str(random.randint(1000,1099))
                #create time strings for c* insert, kafka and bucketing
                update_time = datetime.datetime.now()
                time_string = update_time.strftime("%Y-%m-%d %H:%M:%S")
                epoch_time_string = str((int(update_time.strftime("%s")) * 1000))
                #create one hour bucket
                bucket = str(int(update_time.strftime("%s"))/3600)
                self.session.execute_async(self.insert_coupon_clip,
                    [row_zip,row_offer_id,True,update_time]
                )
                self.session.execute_async(self.insert_coupon_event,
                    [row_zip,row_offer_id,bucket,update_time]
                )
                kafka_producer.produce([row_zip+','+row_offer_id+','+epoch_time_string])
            time.sleep(1)
'''

def main():
    logging.basicConfig()
    client = BoundStatementsClient()
    client.connect([Config.cassandra_hosts])
    client.create_schema()
    time.sleep(1)
    client.prepare_statements()
    client.load_seed_data()
    client.close()

if __name__ == "__main__":
    main()
