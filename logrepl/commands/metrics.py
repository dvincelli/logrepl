from prometheus_client import start_http_server, Gauge, Counter
import time
from loguru import logger
from logrepl.db import source_db


# To get the replication lag in seconds, we'll need to enable track_commit_timestamp on the source database first
REPLICATION_LAG = Gauge('replication_lag', 'Replication lag in bytes', ['host', 'client', 'state', 'database', 'application_name'])
CONNECTION_ERRORS = Counter('connection_errors', 'Connection errors', ['host', 'database', 'application_name', 'error'])
POLL_INTERVAL = 10


def query_replication_lag(config):
    with source_db(config) as conn:
        return get_replication_lag(conn, config["target"]["subscription"])


def get_replication_lag(conn, application_name):
    with conn.cursor() as cur:
        logger.debug("Querying replication lag")
        query = '''
        SELECT
            application_name,
            client_addr,
            state,
            pg_current_wal_lsn() - replay_lsn AS lag_bytes
        FROM
            pg_stat_replication
        WHERE
            application_name = %s;
        '''
        cur.execute(query, (application_name,))
        row = cur.fetchone()
        logger.debug(f"Replication lag: {row}")
        return row


def metrics_server(config):
    start_http_server(8000)
    while True:
        try:
            application_name, client_addr, state, lag_bytes = query_replication_lag(config)
            REPLICATION_LAG.labels(**{
                'host': config['source']['host'],
                'client': client_addr,
                'state': state,
                'database': config['source']['dbname'],
                'application_name': application_name
            }).set(lag_bytes)
        except Exception as e:
            logger.exception("Error querying replication lag")
            CONNECTION_ERRORS.labels(**{
                'host': config['source']['host'],
                'database': config['source']['dbname'],
                'application_name': config['target']['subscription'],
                'error': str(e)
            }).inc()
        time.sleep(POLL_INTERVAL)
