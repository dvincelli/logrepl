import subprocess
import psycopg
from psycopg import sql
import argparse
import configparser
import io
import os
import contextlib
from loguru import logger


def run_subprocess(command, env=None):
    logger.debug(f"Running command: {command}")
    subprocess.run(command, shell=True, check=True, env=env)


@contextlib.contextmanager
def connect_db(db, user, password, host, port):
    logger.debug(f"Connecting to database {db} on {host}:{port} as {user}")
    with psycopg.connect(
        dbname=db, user=user, host=host, port=port, password=password, sslmode="require"
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
    ) as conn:
        yield conn


def execute_sql(conn, query, args=None):
    args = args or []
    logger.debug(f"Executing query: {query} with args: {args}")
    with conn.cursor() as cur:
        cur.execute(query, args)
    conn.commit()


def dump_schema(config, file="/tmp/schema.sql"):
    logger.debug(f"Dumping schema to {file}")

    dbname = config["source"]["dbname"]
    user = config["source"]["username"]
    password = config["source"]["password"]
    host = config["source"]["host"]
    port = config["source"]["port"]
    sslmode = config["source"].get("sslmode", "require")
    schema = "public"

    command = (
        f"pg_dump -h {host} -p {port} -U {user} -s {dbname} -n {schema} -x -O > {file}"
    )
    run_subprocess(command, env={"PGPASSWORD": password, "PGSSLMODE": sslmode})


def restore_schema(config, file="/tmp/schema.sql"):
    logger.debug(f"Restoring schema from {file}")

    create_database(config)

    dbname = config["target"]["dbname"]
    user = config["target"]["username"]
    password = config["target"]["password"]
    host = config["target"]["host"]
    port = config["target"]["port"]
    sslmode = config["source"].get("sslmode", "require")

    command = f"psql -h {host} -p {port} -U {user} -d {dbname} -f {file}"
    run_subprocess(command, env={"PGPASSWORD": password, "PGSSLMODE": sslmode})


def create_pglogical_extension(conn):
    logger.debug("Creating pglogical extension")
    execute_sql(conn, "CREATE EXTENSION IF NOT EXISTS pglogical")


def source_dsn(config):
    source = config["source"]
    sslmode = config["source"].get("sslmode", "require")
    return f"host={source['host']} port={source['port']} dbname={source['dbname']} user={source['username']} password={source['password']} sslmode={sslmode}"


def target_dsn(config, connect_as_replication_user=True):
    target = config["target"]
    sslmode = config["target"].get("sslmode", "require")
    if connect_as_replication_user:
        return f"host={target['host']} port={target['port']} dbname={target['dbname']} user={target['replication_username']} password={target['replication_password']} sslmode={sslmode}"
    else:
        return f"host={target['host']} port={target['port']} dbname={target['dbname']} user={target['username']} password={target['password']} sslmode={sslmode}"


def create_node(conn, node, dsn):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.create_node(node_name := %s, dsn := %s)"),
        [node, dsn],
    )
    logger.info(f"Node {node} created")


def drop_node(conn, node):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_node(node_name := %s)"),
        [node],
    )
    logger.info(f"Node {node} dropped")


def create_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.create_replication_set(set_name := %s)"),
        [set_name],
    )
    logger.info(f"Replication set {set_name} created")


def drop_replication_set(conn, set_name):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_replication_set(set_name := %s)"),
        [set_name],
    )
    logger.info(f"Replication set {set_name} dropped")


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
        sql.SQL("SELECT pglogical.replication_set_add_all_sequences(%s, ARRAY['public'])"),
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

        # TODO: GCP only, make this conditional
        # if cloudsqlsuperuser permission group exists, grant it:
        # cur.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = 'cloudsqlsuperuser'"))

        execute_sql(conn, sql.SQL("GRANT cloudsqlsuperuser TO {}").format(role))

        (cur.execute(sql.SQL("GRANT ALL ON SCHEMA pglogical TO {}").format(role)),)
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


def drop_subscription(conn, subscription):
    execute_sql(
        conn,
        sql.SQL("SELECT pglogical.drop_subscription(subscription_name := %s)"),
        [subscription],
    )
    logger.info(f"Subscription {subscription} dropped")


def subscription_status(conn, subscription):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT status FROM pglogical.show_subscription_status(%s)"),
            [subscription],
        )
        result = cur.fetchone()
        status = result[0]
        logger.info(f"Subscription status: {status}")


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


def copy_sequence_values(config):
    values = {}
    with source_db(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema = 'public'
                """
            )
            result = cur.fetchall()
            for row in result:
                name = row[0]
                cur.execute("SELECT last_value FROM {}".format(name))
                last_value = cur.fetchone()[0]
                values[name] = last_value

    with target_db(config) as conn:
        for name, value in values.items():
            with conn.cursor() as cur:
                cur.execute("SELECT setval(%s, %s, true)", (name, value))


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


def create_database(config):
    dbname = config["target"]["dbname"]
    with target_db(config, dbname="template1") as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)),
            )


def stop(config):
    with target_db(config) as conn:
        drop_subscription(conn, config["target"]["subscription"])


def teardown_all(config):
    teardown_subscription(config)
    teardown_subscriber(config)

    teardown_replication_set(config)
    teardown_provider(config)


def verify_config(config):
    logger.info("Verifying the configuration")

    verified = True

    with source_db(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result != (1,):
                raise SystemExit("Source database is not accessible")
    logger.info("Source database is accessible")

    with target_db(config, dbname="template1") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            if result != (1,):
                raise SystemExit("Target database is not accessible")
    logger.info("Target database is accessible")

    # very the following settings on both source and target databases:
    # wal_level = 'logical'
    # shared_preload_libraries = 'pglogical'
    # max_worker_processes = 10   # one per database needed on provider node
    # one per node needed on subscriber node
    logger.info("Verifying the source database settings:")
    with source_db(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            result = cur.fetchone()
            wal_level = result[0]
            logger.info(f"wal_level: {wal_level}")
            if wal_level != "logical":
                logger.error("wal_level is not set to 'logical' on source database")
                verified = False

            cur.execute("SHOW shared_preload_libraries")
            result = cur.fetchone()
            shared_preload_libraries = result[0].split(",")
            logger.info(
                f"shared_preload_libraries: {','.join(shared_preload_libraries)}"
            )
            if "pglogical" not in shared_preload_libraries:
                logger.error(
                    "shared_preload_libraries does not include 'pglogical' on source database"
                )
                verified = False

            cur.execute("SHOW max_worker_processes")
            result = cur.fetchone()
            max_worker_processes = int(result[0])
            logger.info(f"max_worker_processes: {max_worker_processes}")
            if max_worker_processes < 10:
                logger.error("max_worker_processes is less than 10 on source database")
                verified = False

            cur.execute("SHOW max_replication_slots")
            result = cur.fetchone()
            max_replication_slots = int(result[0])
            logger.info(f"max_replication_slots: {max_replication_slots}")
            if max_replication_slots < 10:
                logger.error("max_replication_slots is less than 10 on source database")
                verified = False

            cur.execute("SHOW max_wal_senders")
            result = cur.fetchone()
            max_wal_senders = int(result[0])
            logger.info(f"max_wal_senders: {max_wal_senders}")
            if max_wal_senders < 10:
                logger.error("max_wal_senders is less than 10 on source database")
                verified = False

            # PGLogical does not support replication between databases with different encoding.
            # We recommend using UTF-8 encoding in all replicated databases.
            cur.execute("SHOW server_encoding")
            result = cur.fetchone()
            server_encoding = result[0]
            logger.info(f"server_encoding: {server_encoding}")
            if server_encoding != "UTF8":
                logger.error("server_encoding is not set to 'UTF8' on source database")
                verified = False

    logger.info("Verifying the target database settings:")
    with target_db(config, dbname="template1") as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            result = cur.fetchone()
            wal_level = result[0]
            logger.info(f"wal_level: {wal_level}")
            if wal_level != "logical":
                logger.error("wal_level is not set to 'logical' on target database")
                verified = False

            cur.execute("SHOW shared_preload_libraries")
            result = cur.fetchone()
            shared_preload_libraries = result[0].split(",")
            logger.info(
                f"shared_preload_libraries: {','.join(shared_preload_libraries)}"
            )
            if "pglogical" not in shared_preload_libraries:
                logger.error(
                    "shared_preload_libraries does not include 'pglogical' on target database"
                )
                verified = False

            cur.execute("SHOW max_worker_processes")
            result = cur.fetchone()
            max_worker_processes = int(result[0])
            logger.info(f"max_worker_processes: {max_worker_processes}")
            if max_worker_processes < 10:
                logger.error("max_worker_processes is less than 10 on target database")
                raise SystemExit(
                    "max_worker_processes is less than 10 on target database"
                )

            cur.execute("SHOW server_encoding")
            result = cur.fetchone()
            server_encoding = result[0]
            logger.info(f"server_encoding: {server_encoding}")
            if server_encoding != "UTF8":
                logger.error("server_encoding is not set to 'UTF8' on target database")
                verified = False

    if verified:
        logger.info("Configuration verification passed")
    else:
        logger.error("Configuration verification failed")

    return verified


def handle_client(config, args):
    env = os.environ.copy()

    db = args.database
    host = config[db]["host"]
    port = config[db]["port"]
    user = config[db]["username"]
    dbname = config[db]["dbname"]
    password = config[db]["password"]
    sslmode = config[db].get("sslmode", "require")

    args = ["psql", "-h", host, "-p", port, "-U", user, "-d", dbname]

    env["PGPASSWORD"] = password
    env["PGSSLMODE"] = sslmode

    os.execve("/usr/bin/psql", args, env)


def dump_config(config):
    out = io.StringIO()
    config.write(out)
    logger.info(out.getvalue())


def argparser():
    parser = argparse.ArgumentParser(
        description="Replicate a PostgreSQL database using pglogical"
    )

    parser.add_argument(
        "--config", "-c", required=True, help="Path to the configuration file"
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    subparsers.add_parser("config", help="Print the configuation")
    subparsers.add_parser("dump", help="Dump the schema")
    subparsers.add_parser("load", help="Restore the schema")

    setup_subparser = subparsers.add_parser("setup", help="Setup the replication")
    setup_subparsers = setup_subparser.add_subparsers(dest="setup_command")
    setup_subparsers.add_parser("all", help="Setup the replication")
    setup_subparsers.add_parser("extension", help="Create the pglogical extension")

    schema_parser = setup_subparsers.add_parser("schema", help="Create the schema")
    schema_parser.add_argument(
        "--file",
        "-f",
        required=False,
        help="Path to the schema file",
        default="/tmp/schema.sql",
    )
    schema_parser.add_argument(
        "--dump", "-d", required=False, help="Dump the schema from source database"
    )
    schema_parser.add_argument(
        "--load", "-l", required=False, help="Load the schema on target database"
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
        copy_sequence_values(config)


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

    config = configparser.ConfigParser()
    config.read(args.config)

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
    else:
        print("Unknown command")
        parser.print_help()
