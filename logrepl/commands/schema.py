import subprocess
import os
from loguru import logger
from logrepl.db import target_db
from psycopg import sql


def create_database(config):
    dbname = config["target"]["dbname"]
    with target_db(config, dbname="template1") as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE IF NOT EXISTS {}").format(sql.Identifier(dbname)),
            )


def run_subprocess(command, env=None):
    logger.debug(f"Running command: {command}")
    subprocess.run(command, shell=True, check=True, env=env)


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

    env = os.environ.copy()
    env["PGPASSWORD"] = password
    env["PGSSLMODE"] = sslmode

    run_subprocess(command, env=env)


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
    env = os.environ.copy()
    env["PGPASSWORD"] = password
    env["PGSSLMODE"] = sslmode

    run_subprocess(command, env=env)
