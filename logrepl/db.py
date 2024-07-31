import psycopg
from psycopg import sql  # noqa
from loguru import logger
import contextlib


@contextlib.contextmanager
def connect_db(db, user, password, host, port, sslmode="require"):
    logger.debug(f"Connecting to database {db} on {host}:{port} as {user}")
    with psycopg.connect(
        dbname=db, user=user, host=host, port=port, password=password, sslmode=sslmode
    ) as cxn:
        logger.debug(f"Connected to database {db} on {host}:{port} as {user}")
        yield cxn


@contextlib.contextmanager
def source_db(config, dbname=None):
    conf = config["source"]
    with connect_db(
        dbname or conf["dbname"],
        conf["username"],
        conf["password"],
        conf["host"],
        conf["port"],
        conf["sslmode"],
    ) as conn:
        yield conn


@contextlib.contextmanager
def target_db(config, dbname=None):
    conf = config["target"]
    with connect_db(
        dbname or conf["dbname"],
        conf["username"],
        conf["password"],
        conf["host"],
        conf["port"],
        conf["sslmode"]
    ) as conn:
        yield conn


def execute_sql(conn, query, args=None):
    args = args or []
    logger.debug(f"Executing query: {query} with args: {args}")
    with conn.cursor() as cur:
        cur.execute(query, args)
    conn.commit()
