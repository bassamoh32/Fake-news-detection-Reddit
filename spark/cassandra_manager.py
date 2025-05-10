from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging

logger = logging.getLogger(__name__)

class CassandraManager:
    def __init__(self, hosts=['cassandra'], port=9042, username='admin', password='admin'):
        self.cluster = None
        self.session = None
        self.hosts = hosts
        self.port = port
        self.username = username
        self.password = password

    def connect(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )
            self.cluster = Cluster(
                self.hosts,
                port=self.port,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
            logger.info("Connected to Cassandra successfully")
            return self.session
        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}")
            raise

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()