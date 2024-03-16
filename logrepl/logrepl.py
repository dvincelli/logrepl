import subprocess
import psycopg2
from psycopg2 import sql
import argparse
import configparser
import io
import contextlib


def run_subprocess(command, env=None):
    subprocess.run(command, shell=True, check=True, env=env)


@contextlib.contextmanager
def connect_db(db, user, password, host, port):
    with psycopg2.connect(dbname=db, user=user, host=host, port=port, password=password) as cxn:
        yield cxn


@contextlib.contextmanager
def source_db(config):
    conf = config["source"]
    with connect_db(
        conf["dbname"], conf["username"], conf["password"], conf["host"], conf["port"]
    ) as conn:
        yield conn


@contextlib.contextmanager
def target_db(config):
    conf = config["target"]
    with connect_db(
        conf["dbname"], conf["username"], conf["password"], conf["host"], conf["port"]
    ) as conn:
        yield conn


def execute_sql(conn, query):
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def dump_schema(db, user, password, host, port, schema, file):
    command = f"pg_dump -h {host} -p {port} -U {user} -s {db} -n {schema} > {file}"
    run_subprocess(command, env={"PGPASSWORD": password})


def restore_schema(db, user, password, host, port, file):
    command = f"psql -h {host} -p {port} -U {user} -d {db} -f {file}"
    run_subprocess(command, env={"PGPASSWORD": password})


def create_pglogical_extension(conn):
    execute_sql(conn, "CREATE EXTENSION IF NOT EXISTS pglogical")


def create_node(conn, node, host, port, db, user, password):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.create_node(node_name := %s, dsn := %s)"),
        [node, f"host={host} port={port} dbname={db} user={user} password={password}"],
    )


def create_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.create_replication_set(set_name := %s)"),
        [set_name],
    )


def add_all_tables_to_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.replication_set_add_all_tables(%s, ARRAY['public'])"),
        [set_name],
    )


def create_subscription(conn, subscription, host, port, db, user, password, set_name):
    execute_sql(
        conn,
        sql.SQL(
            "SELECT pglogical.create_subscription(subscription_name := %s, provider_dsn := %s, replication_sets := ARRAY[%s])"
        ),
        [subscription, f"host={host} port={port} dbname={db} user={user} password={password}", set_name],
    )


def start_subscription(conn, subscription):
    execute_sql(
        conn,
        sql.SQL(
            "SELECT pglogical.alter_subscription_set_enabled(subscription_name := %s, enabled := true)"
        ),
        [subscription],
    )


def stop_subscription(conn, subscription):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_subscription(subscription_name := %s)"),
        [subscription],
    )


def subscription_status(conn, subscription):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT * FROM pglogical.show_subscription_status(%s)"), [subscription]
        )
        for record in cur.fetchall():
            print(record)


def wait_for_subscription(conn, subscription):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT * FROM pglogical.show_subscription_status(%s)"), [subscription]
        )
        for record in cur.fetchall():
            print(record)

# -- commands --



def status(config):
    with target_db(config) as conn:
        subscription_status(conn, config["target"]["subscription"])


def setup(config):
    source_db = config["source"]["dbname"]
    source_user = config["source"]["username"]
    source_password = config["source"]["password"]
    source_host = config["source"]["host"]
    source_port = config["source"]["port"]
    source_node = config["source"]["node"]

    target_db = config["target"]["dbname"]
    target_user = config["target"]["username"]
    target_password = config["target"]["password"]
    target_host = config["target"]["host"]
    target_port = config["target"]["port"]
    target_node = config["target"]["node"]
    target_subscription = config["target"]["subscription"]

    # Ensure the pglogical extension is installed on both databases
    with source_db(source_db, source_user, source_host, source_port) as conn:
        create_pglogical_extension(conn)

    with target_db(target_db, target_user, target_host, target_port) as conn:
        create_pglogical_extension(conn)

    # Dump the schema from the source database
    schema_file = "/tmp/schema.sql"
    dump_schema(source_db, source_user, source_password, source_host, source_port, "public", schema_file)
    # Restore the schema to the target database
    restore_schema(target_db, target_user, target_password, target_host, target_port, schema_file)

    # create the provider nodes on the source databases
    with source_db(source_db, source_user, source_host, source_port) as conn:
        create_node(conn, source_node, source_host, source_port, source_db, source_user, source_password)

    set_name = "default"
    # Add all tables in public schema to the default replication set on the source databases
    with source_db(source_db, source_user, source_host, source_port) as conn:
        create_replication_set(conn, set_name)
        add_all_tables_to_replication_set(conn, set_name)

    # Create the subscriber on the target database.
    with target_db(target_db, target_user, target_host, target_port) as conn:
        create_node(conn, target_node, target_host, target_port, target_db, target_user, target_password)
        create_subscription(
            conn, target_subscription, source_host, source_port, source_db, source_user, set_name
        )

    # Check the status of the subscription
    with target_db(target_db, target_user, target_host, target_port) as conn:
        subscription_status(conn, target_subscription)


def start(config):
    with target_db(config) as conn:
        start_subscription(conn, config["target"]["subscription"])
        subscription_status(conn, config["target"]["subscription"])


def stop(config):
    with target_db(config) as conn:
        stop_subscription(conn, config["target"]["subscription"])
        subscription_status(conn, config["target"]["subscription"])


def teardown(config):
    with target_db(config) as conn:
        stop_subscription(conn, config["target"]["subscription"])
        subscription_status(conn, config["target"]["subscription"])

        execute_sql(
            conn,
            sql.SQL("SELECT pglogical.drop_node(node_name := %s)"),
            [config["node"]["name"]],
        )


def verify_config(config):
    print("Verifying the configuration")

    print("Verifying the source database")
    with source_db(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result != (1,):
                raise SystemExit("Source database is not accessible")

    print("Verifying the target database")
    with target_db(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result != (1,):
                raise SystemExit("Target database is not accessible")


def wait(config):
    with target_db(config) as conn:
        wait_for_subscription(conn, config["target"]["subscription"])


def dump_config(config):
    out = io.StringIO()
    config.write(out)
    print(out.getvalue())


def argparser():
    parser = argparse.ArgumentParser(
        description="Replicate a PostgreSQL database using pglogical"
    )

    parser.add_argument(
        "--config", "-c", required=True, help="Path to the configuration file"
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    subparsers.add_parser("config", help="Print the configuation")
    subparsers.add_parser("setup", help="Setup the replication")
    subparsers.add_parser("start", help="Start the replication")
    subparsers.add_parser(
        "status", help="Show the status of the replication"
    )
    subparsers.add_parser("stop", help="Stop the replication")
    subparsers.add_parser("teardown", help="Teardown the replication")
    subparsers.add_parser("verify", help="Verify the configuration")
    subparsers.add_parser("wait", help="Wait for the replication to catch up")

    return parser


def main():
    parser = argparser()
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    if args.command is None:
        parser.print_help()
    elif args.command == "status":
        status(config)
    elif args.command == "setup":
        setup(config)
    elif args.command == "start":
        start(config)
    elif args.command == "stop":
        stop(config)
    elif args.command == "teardown":
        teardown(config)
    elif args.command == "config":
        dump_config(config)
    elif args.command == "verify":
        verify_config(config)
    elif args.command == "wait":
        wait(config)
    else:
        print("Unknown command")
        parser.print_help()
