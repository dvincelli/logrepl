import subprocess
import psycopg
from psycopg import sql
import argparse
import configparser
import io
import contextlib
from loguru import logger


def run_subprocess(command, env=None):
    logger.debug(f"Running command: {command}")
    subprocess.run(command, shell=True, check=True, env=env)


@contextlib.contextmanager
def connect_db(db, user, password, host, port):
    logger.debug(f"Connecting to database {db} on {host}:{port} as {user}")
    with psycopg.connect(dbname=db, user=user, host=host, port=port, password=password, sslmode="require") as cxn:
        logger.debug(f"Connected to database {db} on {host}:{port} as {user}")
        yield cxn


@contextlib.contextmanager
def source_db(config, dbname=None):
    conf = config["source"]
    with connect_db(
        dbname or conf["dbname"], conf["username"], conf["password"], conf["host"], conf["port"]
    ) as conn:
        yield conn


@contextlib.contextmanager
def target_db(config, dbname=None):
    conf = config["target"]
    with connect_db(
        dbname or conf["dbname"], conf["username"], conf["password"], conf["host"], conf["port"]
    ) as conn:
        yield conn


def execute_sql(conn, query, args=None):
    logger.debug(f"Executing query: {query} with args: {args}")
    args = args or []
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
    schema = 'public'

    command = f"pg_dump -h {host} -p {port} -U {user} -s {dbname} -n {schema} -x -O > {file}"
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


def target_dsn(config):
    target = config["target"]
    sslmode = config["target"].get("sslmode", "require")
    return f"host={target['host']} port={target['port']} dbname={target['dbname']} user={target['username']} password={target['password']} sslmode={sslmode}"


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


def create_subscription(conn, subscription, dsn, set_name):
    execute_sql(
        conn,
        sql.SQL(
            "SELECT pglogical.create_subscription(subscription_name := %s, provider_dsn := %s, replication_sets := ARRAY[%s])"
        ),
        [subscription, dsn, set_name],
    )


def drop_subscription(conn, subscription):
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
    create_extension(config) # on both source and target
    create_schema(config) # from source to target
    create_provider_node(config)  # on source database

    # Add all tables in public schema to the default replication set on the source databases
    init_replication_set(config)
    create_subscriber(config)

    status(config)


def create_subscriber(config):
    set_name = config["source"]["replication_set"]
    with target_db(config) as conn:
        try:
            create_node(conn, config["target"]["node"], target_dsn(config))
        except psycopg.errors.InternalError_ as e: # node mlflow_staging_subscriber already exists
            print(e)

    with target_db(config) as conn:
        create_subscription(
            conn, config["target"]["subscription"], source_dsn(config), set_name
        )


def init_replication_set(config):
    set_name = config["source"]["replication_set"]
    with source_db(config) as conn:
        create_replication_set(conn, set_name)
        add_all_tables_to_replication_set(conn, set_name)


def create_provider_node(config):
    # create the provider nodes on the source databases
    with source_db(config) as conn:
        create_node(conn, config["source"]["node"], source_dsn(config))


def create_schema(config):
    dump_schema(config)
    restore_schema(config)


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
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(dbname)), 
            )


def stop(config):
    with target_db(config) as conn:
        drop_subscription(conn, config["target"]["subscription"])
        subscription_status(conn, config["target"]["subscription"])


def teardown(config):
    with target_db(config) as conn:
        drop_subscription(conn, config["target"]["subscription"])
        subscription_status(conn, config["target"]["subscription"])

        execute_sql(
            conn,
            sql.SQL("SELECT pglogical.drop_node(node_name := %s)"),
            [config["node"]["name"]],
        )


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
            shared_preload_libraries = result[0].split(',')
            logger.info(f"shared_preload_libraries: {','.join(shared_preload_libraries)}")
            if 'pglogical' not in shared_preload_libraries:
                logger.error("shared_preload_libraries does not include 'pglogical' on source database")
                verified = False
    
            cur.execute("SHOW max_worker_processes")
            result = cur.fetchone()
            max_worker_processes = int(result[0])
            logger.info("max_worker_processes: {max_worker_processes}")
            if max_worker_processes < 10:
                logger.error("max_worker_processes is less than 10 on source database")
                verified = False

            cur.execute("SHOW max_replication_slots")
            result = cur.fetchone()
            max_replication_slots = int(result[0])
            logger.info("max_replication_slots: {max_replication_slots}")
            if max_replication_slots < 10:
                logger.error("max_replication_slots is less than 10 on source database")
                verified = False

            cur.execute("SHOW max_wal_senders")
            result = cur.fetchone()
            max_wal_senders = int(result[0])
            logger.info("max_wal_senders: {max_wal_senders}")
            if max_wal_senders < 10:
                logger.error("max_wal_senders is less than 10 on source database")
                verified = False

    logger.info("Verifying the target database settings:")
    with target_db(config) as conn:
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
            shared_preload_libraries = result[0].split(',')
            logger.info(f"shared_preload_libraries: {','.join(shared_preload_libraries)}")
            if 'pglogical' not in shared_preload_libraries:
                logger.error("shared_preload_libraries does not include 'pglogical' on target database")
                verified = False

            cur.execute("SHOW max_worker_processes")
            result = cur.fetchone()
            max_worker_processes = int(result[0])
            logger.info("max_worker_processes: {max_worker_processes}")
            if max_worker_processes < 10:
                logger.error("max_worker_processes is less than 10 on target database")
                raise SystemExit("max_worker_processes is less than 10 on target database")    

    if verified:
        logger.info("Configuration verification passed")
    else:
        logger.error("Configuration verification failed")

    return verified

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
    subparsers.add_parser("dump", help="Dump the schema")
    subparsers.add_parser("load", help="Restore the schema")

    setup_subparser = subparsers.add_parser("setup", help="Setup the replication")
    setup_subparsers = setup_subparser.add_subparsers(dest="setup_command")
    setup_subparsers.add_parser("all", help="Setup the replication")
    setup_subparsers.add_parser("extension", help="Create the pglogical extension")

    schema_parser = setup_subparsers.add_parser("schema", help="Create the schema")
    schema_parser.add_argument(
        "--file", "-f", required=False, help="Path to the schema file", default="/tmp/schema.sql"
    )
    schema_parser.add_argument(
        "--dump", "-d", required=False, help="Dump the schema from source database"
    )
    schema_parser.add_argument(
        "--load", "-l", required=False, help="Load the schema on target database"
    )
    setup_subparsers.add_parser("node", help="Create the provider node")
    setup_subparsers.add_parser("replication_set", help="Create the replication set")
    setup_subparsers.add_parser("subscriber", help="Create the subscriber")

    subparsers.add_parser(
        "status", help="Show the status of the replication"
    )
    subparsers.add_parser("stop", help="Stop the replication")
    subparsers.add_parser("teardown", help="Teardown the replication")
    subparsers.add_parser("verify", help="Verify the configuration")
    subparsers.add_parser("wait", help="Wait for the replication to catch up")

    return parser


def setup_command(config, args):
    if args.setup_command == "all":
        setup(config)
    elif args.setup_command == "extension":
        create_extension(config)
    elif args.setup_command == "schema":
        if args.dump:
            dump_schema(config, args.file)
        elif args.load:
            restore_schema(config, args.file)
    elif args.setup_command == "node":
        create_provider_node(config)
    elif args.setup_command == "replication_set":
        init_replication_set(config)
    elif args.setup_command == "subscriber":
        create_subscriber(config)


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
        setup_command(config, args)
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
