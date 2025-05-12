from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)
from utils.schema import CassandraSchema

class CassandraManager:
    def __init__(self, host: str, keyspace: str, table: str):
        self.host = host
        self.keyspace = keyspace
        self.table = table
        self.cluster = None
        self.session = None

    def connect(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username="admin",
                password="admin"
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
    
    def insert_data(self, title: str, subreddit: str, text: str):
        if not self.session:
            raise Exception("No active session. Call connect() first.")
            
        try: 
            self.session.execute(
                CassandraSchema.get_insert_query(self.table), 
                (uuid.uuid1(), title, subreddit, text, datetime.now())
            )
            logger.info(f"Data inserted into table {self.table}")
        except Exception as e:
            logger.error(f"Error inserting data into table {self.table}: {e}")
            raise
    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")