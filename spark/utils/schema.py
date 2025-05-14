class CassandraSchema:
    CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {0}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
    """

    CREATE_TABLE = """
        CREATE TABLE IF NOT EXISTS {0} (
            post_id TEXT,
            title TEXT,
            subreddit TEXT,
            text TEXT,
            post_date DATE,
            post_time TIMESTAMP,
            prob_fake FLOAT,
            prediction TEXT,
            last_checked TIMESTAMP,
            PRIMARY KEY ((post_date), post_time, post_id)
        );
    """

    @staticmethod
    def get_insert_query(table: str):
        return f"""
            INSERT INTO {table} (
                post_id, title, subreddit, text,
                post_date, post_time,
                prob_fake, prediction, last_checked
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
