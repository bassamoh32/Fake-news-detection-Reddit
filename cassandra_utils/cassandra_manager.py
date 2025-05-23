import sys, os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from cassandra_utils.schema import CassandraSchema

class CassandraManager:
    def __init__(self, host: str, keyspace: str, table: str, username: str, password: str):
        self.host = host
        self.keyspace = keyspace
        self.table = table
        self.username = username
        self.password = password
        self.cluster = None
        self.session = None

    def connect(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )
            self.cluster = Cluster(
                [self.host],
                auth_provider=auth_provider,
                protocol_version=4,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
            )
            self.session = self.cluster.connect()

            self.session.execute(CassandraSchema.CREATE_KEYSPACE.format(self.keyspace))
            self.session.set_keyspace(self.keyspace)

            self.session.execute(CassandraSchema.CREATE_TABLE.format(self.table))
            logger.info(f"Successfully created table {self.table} in keyspace {self.keyspace}")
            return self.session
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    def get_session(self):
        if not self.session:
            raise RuntimeError("No active session. Did you call connect()?")
        return self.session

    def insert_data(self, post_id: str, title: str, subreddit: str, text: str, post_time: datetime):
        if not self.session:
            raise Exception("No active session. Call connect() first.")
        
        try:
            post_date = post_time.date()
            self.session.execute(
                CassandraSchema.get_insert_query(self.table),
                (post_id, title, subreddit, text, post_date, post_time)
            )
            logger.info(f"Data inserted into table {self.table}")
        except Exception as e:
            logger.error(f"Error inserting data into table {self.table}: {e}")
            raise

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")