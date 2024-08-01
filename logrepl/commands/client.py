import os


def handle_client(config, args):
    db = args.database

    host = config[db]["host"]
    port = config[db]["port"]
    user = config[db]["username"]
    dbname = config[db]["dbname"]
    password = config[db]["password"]
    sslmode = config[db].get("sslmode", "require")

    args = ["psql", "-h", host, "-p", port, "-U", user, "-d", dbname]

    env = os.environ.copy()
    env["PGPASSWORD"] = password
    env["PGSSLMODE"] = sslmode

    psql = os.popen("which psql").read().strip()

    os.execve(psql, args, env)
