import os
import subprocess


def init_pgbench(config):
    db = "source"

    host = config[db]["host"]
    port = config[db]["port"]
    user = config[db]["username"]
    dbname = config[db]["dbname"]
    password = config[db]["password"]
    sslmode = config[db].get("sslmode", "require")

    env = os.environ.copy()

    env["PGPASSWORD"] = password
    env["PGSSLMODE"] = sslmode
    env["PGHOST"] = host
    env["PGPORT"] = port
    env["PGUSER"] = user
    env["PGDATABASE"] = dbname

    subprocess.run("pgbench -i", shell=True, env=env)
