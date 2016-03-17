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

log = logging.getLogger()
log.setLevel('INFO')
fake=Factory.create()
class Config(object):
    cassandra_hosts = '127.0.0.1'

def generate_games_data():
    results = []
    for i in range(1,1000):
        for j in range(1,20):
            for k in range(1,5):
                results.append([str(i), str(j), random.randint(1,100), fake.date_time_between(start_date="-2y", end_date="now"), fake.company()])
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
        self.session.execute("""DROP KEYSPACE IF EXISTS games;""")
        self.session.execute("""CREATE KEYSPACE games WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};""")
        self.session.execute("""
            CREATE TABLE games.scores (
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY(user_id,game_id,last_updated)
            );
        """)

        self.session.execute("""
            CREATE TABLE games.scores_by_date_d (
                dummykey text,
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY(dummykey,last_updated)
            )
            WITH CLUSTERING ORDER BY (last_updated DESC);
        """)

        self.session.execute("""
            CREATE TABLE games.scores_by_date_a (
                dummykey text,
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY(dummykey,last_updated)
            )
            WITH CLUSTERING ORDER BY (last_updated ASC);
        """)

        self.session.execute("""
            CREATE TABLE games.scores_by_usergame_a (
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY((game_id,user_id),high_score)
            )
            WITH CLUSTERING ORDER BY (high_score ASC);
        """)

        self.session.execute("""
            CREATE TABLE games.scores_by_usergame_d (
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY((game_id,user_id),high_score)
            )
            WITH CLUSTERING ORDER BY (high_score DESC);
        """)

        self.session.execute("""
            CREATE TABLE games.scores_by_game_a (
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY(game_id,high_score)
            )
            WITH CLUSTERING ORDER BY (high_score ASC);
        """)

        self.session.execute("""
            CREATE TABLE games.scores_by_game_d (
                user_id text,
                game_id text,
                high_score int,
                last_updated timestamp,
                company text,
                PRIMARY KEY(game_id,high_score)
            )
            WITH CLUSTERING ORDER BY (high_score DESC);
        """)

        log.info('Games keyspace and schema created.')


class BoundStatementsClient(SimpleClient):
    def prepare_statements(self):
        self.insert_scores = self.session.prepare(
        """
            INSERT INTO games.scores
            (user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?);
        """)

        self.insert_scores_by_date_d = self.session.prepare(
        """
            INSERT INTO games.scores_by_date_d
            (dummykey,user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?,?);
        """)

        self.insert_scores_by_date_a = self.session.prepare(
        """
            INSERT INTO games.scores_by_date_a
            (dummykey,user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?,?);
        """)

        self.insert_scores_by_usergame_a = self.session.prepare(
        """
            INSERT INTO games.scores_by_usergame_a
            (user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?);
        """)

        self.insert_scores_by_usergame_d = self.session.prepare(
        """
            INSERT INTO games.scores_by_usergame_d
            (user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?);
        """)

        self.insert_scores_by_game_a = self.session.prepare(
        """
            INSERT INTO games.scores_by_game_a
            (user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?);
        """)

        self.insert_scores_by_game_d = self.session.prepare(
        """
            INSERT INTO games.scores_by_game_d
            (user_id,game_id,high_score,last_updated,company)
            VALUES (?,?,?,?,?);
        """)


    def load_seed_data(self):
        games = generate_games_data()
        #load coupon data
        for row in games:
            self.session.execute_async(self.insert_scores,
                [row[0],row[1],row[2],row[3],row[4]]
            )
            self.session.execute_async(self.insert_scores_by_date_d,
                ['1',row[0],row[1],row[2],row[3],row[4]]
            )

            self.session.execute_async(self.insert_scores_by_date_a,
                ['1',row[0],row[1],row[2],row[3],row[4]]
            )

            self.session.execute_async(self.insert_scores_by_usergame_a,
                [row[0],row[1],row[2],row[3],row[4]]
            )

            self.session.execute_async(self.insert_scores_by_usergame_d,
                [row[0],row[1],row[2],row[3],row[4]]
            )

            self.session.execute_async(self.insert_scores_by_game_a,
                [row[0],row[1],row[2],row[3],row[4]]
            )

            self.session.execute_async(self.insert_scores_by_game_d,
                [row[0],row[1],row[2],row[3],row[4]]
            )
            sleep(.0001)

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
