import psycopg
import argparse
from pprint import pprint
from .config import load_config_from_ini, load_config_from_env
import io
from loguru import logger
from psycopg import sql
from .db import source_db, target_db, execute_sql, source_dsn, target_dsn
from .commands.metrics import metrics_server
from .commands.verify import verify_config
from .commands.schema import dump_schema, restore_schema
from .commands.client import handle_client
from .commands.setup import (
    create_pglogical_extension,
    create_node,
    create_replication_set,
    create_replication_user,
    add_all_tables_to_replication_set,
    add_all_sequences_to_replication_set,
)
from .commands.pgbench import init_pgbench
from .commands.status import subscription_status
from .commands.teardown import drop_node, drop_replication_set, drop_subscription


# -- commands --
def status(config):
    with target_db(config) as conn:
        subscription_status(conn, config["target"]["subscription"])


def setup(config):
    create_extension(config)  # on both source and target
    create_schema(config)  # from source to target
    create_provider_node(config)  # on source database

    # Add all tables in public schema to the default replication set on the source databases
    init_replication_set(config)
    create_subscriber(config)
    create_subscription(config)

    status(config)


def get_sequence_names(config, schema="public"):
    with source_db(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = %s"
                ),
                [schema],
            )
            result = cur.fetchall()
            return [row[0] for row in result]


def synchronize_sequences(config):
    schema = "public"
    sequences = get_sequence_names(config, schema)
    with source_db(config) as conn:
        with conn.cursor() as cur:
            for sequence in sequences:
                cur.execute(
                    "SELECT pglogical.synchronize_sequence(%s)",
                    [f"{schema}.{sequence}"],
                )


def create_subscriber(config):
    with target_db(config) as conn:
        try:
            create_node(conn, config["target"]["node"], target_dsn(config))
        except (
            psycopg.errors.InternalError_
        ) as e:  # node mlflow_staging_subscriber already exists
            if "already exists" in str(e):
                logger.warning(e)
            else:
                raise e


def setup_replication_user(config):
    with target_db(config) as conn:
        create_replication_user(
            conn,
            config["target"]["replication_username"],
            config["target"]["replication_password"],
        )


def create_subscription(config):
    subscription = config["target"]["subscription"]
    set_name = config["source"]["replication_set"]
    dsn = source_dsn(config)
    with target_db(config) as conn:
        execute_sql(
            conn,
            sql.SQL(
                "SELECT pglogical.create_subscription(subscription_name := %s, provider_dsn := %s, replication_sets := ARRAY[%s])"
            ),
            [subscription, dsn, set_name],
        )
        logger.info(
            f"Subscription {subscription} to replication set {set_name} created"
        )


def init_replication_set(config):
    set_name = config["source"]["replication_set"]
    with source_db(config) as conn:
        create_replication_set(conn, set_name)
        add_all_tables_to_replication_set(conn, set_name)
        add_all_sequences_to_replication_set(conn, set_name)


def create_provider_node(config):
    with source_db(config) as conn:
        create_node(conn, config["source"]["node"], source_dsn(config))


def drop_provider_node(config):
    with source_db(config) as conn:
        drop_node(conn, config["source"]["node"])


def create_schema(config):
    dump_schema(config)
    restore_schema(config)


def teardown_database(config):
    with target_db(config, dbname="template1") as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP DATABASE {}").format(
                    sql.Identifier(config["target"]["dbname"])
                ),
            )


def teardown_replication_set(config):
    with source_db(config) as conn:
        drop_replication_set(conn, config["source"]["replication_set"])


def teardown_subscription(config):
    with target_db(config) as conn:
        drop_subscription(conn, config["target"]["subscription"])


def teardown_subscriber(config):
    with target_db(config) as conn:
        drop_node(conn, config["target"]["node"])


def teardown_provider(config):
    with source_db(config) as conn:
        drop_node(conn, config["source"]["node"])


def create_extension(config):
    with source_db(config) as conn:
        create_pglogical_extension(conn)

    with target_db(config) as conn:
        create_pglogical_extension(conn)


def stop(config):
    with target_db(config) as conn:
        drop_subscription(conn, config["target"]["subscription"])


def teardown_all(config):
    teardown_subscription(config)
    teardown_subscriber(config)

    teardown_replication_set(config)
    teardown_provider(config)


def dump_config(config):
    out = io.StringIO()
    if isinstance(config, dict):
        pprint(config, stream=out)
    else:
        config.write(out)
    logger.info(out.getvalue())


def argparser():
    parser = argparse.ArgumentParser(
        description="Replicate a PostgreSQL database using pglogical"
    )

    parser.add_argument(
        "--config", "-c", required=False, help="Path to the configuration file"
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    subparsers.add_parser("config", help="Print the configuation")
    subparsers.add_parser("dump", help="Dump the schema")
    subparsers.add_parser("load", help="Restore the schema")

    setup_subparser = subparsers.add_parser("setup", help="Setup the replication")
    setup_subparsers = setup_subparser.add_subparsers(dest="setup_command")
    setup_subparsers.add_parser("all", help="Setup the replication")
    setup_subparsers.add_parser("extension", help="Create the pglogical extension")
    setup_subparsers.add_parser(
        "pgbench", help="Initialize a pgbench database in the source, for testing."
    )

    schema_parser = setup_subparsers.add_parser("schema", help="Create the schema")
    schema_parser.add_argument(
        "--file",
        "-f",
        required=False,
        help="Path to the schema file",
        default="/tmp/schema.sql",
    )
    schema_parser.add_argument(
        "--dump",
        "-d",
        required=False,
        action="store_true",
        help="Dump the schema from source database",
    )
    schema_parser.add_argument(
        "--load",
        "-l",
        required=False,
        action="store_true",
        help="Load the schema on target database",
    )
    setup_subparsers.add_parser("provider", help="Create the provider node")
    setup_subparsers.add_parser("replication_set", help="Create the replication set")
    setup_subparsers.add_parser("subscriber", help="Create the subscriber")
    setup_subparsers.add_parser("subscription", help="Create the subscriber")
    setup_subparsers.add_parser("replication_user", help="Create the replication user")
    setup_subparsers.add_parser("sequences", help="Copy sequence values")

    teardown_subparser = subparsers.add_parser(
        "teardown", help="Teardown the replication"
    )
    teardown_subparsers = teardown_subparser.add_subparsers(dest="teardown_command")

    teardown_subparsers.add_parser(
        "schema", help="Drop the schema on the target database"
    )
    teardown_subparsers.add_parser("provider", help="Drop the provider node")
    teardown_subparsers.add_parser("replication_set", help="Drop the replication set")
    teardown_subparsers.add_parser("subscriber", help="Drop the subscriber")
    teardown_subparsers.add_parser("subscription", help="Drop the subscription")
    teardown_subparsers.add_parser("all", help="Teardown the replication")

    subparsers.add_parser("status", help="Show the status of the replication")
    subparsers.add_parser("stop", help="Stop the replication")
    subparsers.add_parser("verify", help="Verify the configuration")
    client = subparsers.add_parser("client", help="Connect with psql")
    client.add_argument(
        "--database", "-d", required=False, help="Database: source or target"
    )
    subparsers.add_parser("metrics", help="Start the prometheus metrics server")

    return parser


def handle_setup(config, args):
    if args.setup_command == "all":
        setup(config)
    elif args.setup_command == "extension":
        create_extension(config)
    elif args.setup_command == "schema":
        if args.dump:
            dump_schema(config, args.file)
        elif args.load:
            restore_schema(config, args.file)
    elif args.setup_command == "provider":
        create_provider_node(config)
    elif args.setup_command == "replication_set":
        init_replication_set(config)
    elif args.setup_command == "subscriber":
        create_subscriber(config)
    elif args.setup_command == "subscription":
        create_subscription(config)
    elif args.setup_command == "replication_user":
        setup_replication_user(config)
    elif args.setup_command == "sequences":
        synchronize_sequences(config)
    elif args.setup_command == "pgbench":
        init_pgbench(config)


def handle_teardown(config, args):
    if args.teardown_command == "all":
        teardown_all(config)
    elif args.teardown_command == "schema":
        teardown_database(config)
    elif args.teardown_command == "provider":
        teardown_provider(config)
    elif args.teardown_command == "replication_set":
        teardown_replication_set(config)
    elif args.teardown_command == "subscriber":
        teardown_subscriber(config)
    elif args.teardown_command == "subscription":
        teardown_subscription(config)


def main():
    parser = argparser()
    args = parser.parse_args()

    if args.config:
        config = load_config_from_ini(args.config)
    else:
        config = load_config_from_env()

    if args.command is None:
        parser.print_help()
    elif args.command == "dump":
        dump_schema(config)
    elif args.command == "load":
        restore_schema(config)
    elif args.command == "status":
        status(config)
    elif args.command == "setup":
        handle_setup(config, args)
    elif args.command == "stop":
        stop(config)
    elif args.command == "teardown":
        handle_teardown(config, args)
    elif args.command == "config":
        dump_config(config)
    elif args.command == "verify":
        verify_config(config)
    elif args.command == "client":
        handle_client(config, args)
    elif args.command == "metrics":
        metrics_server(config)
    else:
        print("Unknown command")
        parser.print_help()
