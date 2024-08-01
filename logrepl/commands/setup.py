from logrepl.db import execute_sql
from psycopg import sql
from loguru import logger


def create_pglogical_extension(conn):
    logger.debug("Creating pglogical extension")
    execute_sql(conn, "CREATE EXTENSION IF NOT EXISTS pglogical")


def create_node(conn, node, dsn):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.create_node(node_name := %s, dsn := %s)"),
        [node, dsn],
    )
    logger.info(f"Node {node} created")


def create_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.create_replication_set(set_name := %s)"),
        [set_name],
    )
    logger.info(f"Replication set {set_name} created")


def add_all_tables_to_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.replication_set_add_all_tables(%s, ARRAY['public'])"),
        [set_name],
    )
    logger.info(f"All tables added to replication set {set_name}")


def add_all_sequences_to_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL(
            "SELECT pglogical.replication_set_add_all_sequences(%s, ARRAY['public'])"
        ),
        [set_name],
    )
    logger.info(f"All sequences added to replication set {set_name}")


def create_replication_user(conn, user, password):
    role = sql.Identifier(user)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = {}").format(
                sql.Literal(user)
            )
        )
        exists = cur.fetchone()
        if not exists:
            cur.execute(
                sql.SQL("CREATE ROLE {} WITH REPLICATION LOGIN PASSWORD {}").format(
                    role, sql.Literal(password)
                ),
            )
            logger.info(f"Replication user {user} created")

        # if cloudsqlsuperuser permission group exists, grant it:
        cur.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = 'cloudsqlsuperuser'"))
        exists = cur.fetchone()
        if cur.rowcount > 0:
            execute_sql(conn, sql.SQL("GRANT cloudsqlsuperuser TO {}").format(role))

        # For alloydb grant alloydbsuperuser instead.
        cur.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = 'alloydbsuperuser'"))
        if cur.rowcount > 0:
            execute_sql(conn, sql.SQL("GRANT alloydbsuperuser TO {}").format(role))

        cur.execute(sql.SQL("GRANT ALL ON SCHEMA pglogical TO {}").format(role))
        cur.execute(
            sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA pglogical TO {}").format(role)
        )
        cur.execute(
            sql.SQL("GRANT SELECT ON ALL SEQUENCES IN SCHEMA pglogical TO {}").format(
                role
            )
        )
        cur.execute(
            sql.SQL("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pglogical TO {}").format(
                role
            )
        )

        cur.execute(sql.SQL("GRANT ALL ON SCHEMA public TO {}").format(role))
        cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
        cur.execute(
            sql.SQL(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {}"
            ).format(role)
        )

        logger.info(f"Replication user {user} granted permissions")
