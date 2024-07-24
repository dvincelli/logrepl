from prometheus_client import start_http_server, Gauge
import time
import psycopg


REPLICATION_LAG = Gauge('replication_lag', 'Replication lag in seconds')


def query_replication_lag(db_config):
    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_last_wal_receive_lsn()')
            receive_lsn = cur.fetchone()[0]
            cur.execute('SELECT pg_last_wal_replay_lsn()')
            replay_lsn = cur.fetchone()[0]
            delay = receive_lsn - replay_lsn
            return delay


def main_loop(db_config):
    start_http_server(8000)
    while True:
        lag = query_replication_lag(db_config)
        REPLICATION_LAG.set(lag, labels={'host': db_config['host'], database: db_config['database']})
        time.sleep(10)

