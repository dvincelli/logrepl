from logrepl.db import execute_sql
from loguru import logger
from psycopg import sql


def drop_node(conn, node):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_node(node_name := %s)"),
        [node],
    )
    logger.info(f"Node {node} dropped")


def drop_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_replication_set(set_name := %s)"),
        [set_name],
    )
    logger.info(f"Replication set {set_name} dropped")


def drop_subscription(conn, subscription):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_subscription(subscription_name := %s)"),
        [subscription],
    )
    logger.info(f"Subscription {subscription} dropped")
