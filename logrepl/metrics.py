from prometheus_client import start_http_server, Gauge, Counter
import time
from loguru import logger


REPLICATION_LAG = Gauge('replication_lag', 'Replication lag in seconds', ['host', 'client', 'state', 'database', 'application_name'])
CONNECTION_ERRORS = Counter('connection_errors', 'Connection errors', ['host', 'database', 'application_name'])
POLL_INTERVAL = 10


def query_replication_lag(source_db, config):
    with source_db(config) as conn:
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
            cur.execute(query, (config["target"]["subscription"],))
            row = cur.fetchone()
            logger.debug(f"Replication lag: {row}")
            return row


def main_loop(source_db, config):
    start_http_server(8000)
    while True:
        try:
            application_name, client_addr, state, lag_bytes = query_replication_lag(source_db, config)
            REPLICATION_LAG.labels(**{
                'host': config['source']['host'],
                'client': client_addr,
                'state': state,
                'database': config['source']['dbname'],
                'application_name': application_name
            }).set(lag_bytes)
            time.sleep(POLL_INTERVAL)
        except Exception as e:
            logger.exception("Error querying replication lag")
            CONNECTION_ERRORS.labels(**{
                'host': config['source']['host'],
                'database': config['source']['dbname'],
                'application_name': config['target']['subscription'],
                'error': str(e)
            }).inc()
