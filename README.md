# logrepl

Sets up pglogical on a source and target database. This tool makes it easy to setup the pglogical extension to migrate a PostgreSQL database. This tool exists to quickly setup a source to target replication for the purposes of migrating
between cloud providers.

This software comes with no warranties implied or expressed.

## Requirements

- Python 3.12
- pdm
- psycopg 3
- loguru

## Installation

1. Clone this github repository
2. Use pdm to create a virtualenv and install dependencies: `pdm install`

## Usage

### Configuration

Create a ini configuration file with two sections: source and target. For example:

```
[source]
username=postgres
password=hunter2
host=192.168.1.1
port=5432
dbname=example
node=exmple_provider
replication_set=example
sslmode=require

[target]
username=postgres
password=secret
host=192.168.1.2
port=5432
dbname=example
node=example_subscriber
subscription=example_subscription
sslmode=require
replication_username=replicator
replication_password=superduper
```

Every key in this example is required.

## Basic verification

Run the verify command. This command will check for connectivity from the logrepl tool to the two databases. It will also verify the basic database settings meet the minimum requirements advised by pglogical.

```
pdm run python -m logrel -c example.ini verify
```

## Setup

If the tests pass, you are ready to replicate source to target. Use the `setup` subcommand.

### Schema

The schema subcommand command will dump the _source_ database schema and restore it on the _target_:

```
pdm run python -m logrepl -c example.ini setup schema --dump
```

Before loading the schema, you can edit it, it is dumped under `/tmp/schema.sql` by default. The utility does _not_ dump extensions, users, grants or owners. Those need to be addressed seperately. For one of my migration it was necessary to add a "CREATE EXTENSION..." manually. The rationale for this decision is that the environments I migrate databases to and from are different cloud providers who pre-install different users and extensions which are incompatible.

```
pdm run python -m logrepl -c example.ini setup schema --load
```

Pay attention to errors during this step. Drop and repeat as many times as necessary to address errors. The schema load command does not stop on error.

### Provider and Replication Set

Create the provider node and replication on the _source_ database:

The `replication_set` will contain all tables and sequences.

```
pdm run python -m logrepl -c example.ini setup provider
pdm run python -m logrepl -c example.ini setup replication_set

```

### Replication user

Create the replication user on the target database. This command will also grant
the user appropriate permissions to get the job done. It currently grants a Google CloudSQL
specific permission. This will be made conditional soon. As the tool is tested in more environments,
we hope to support more cloud providers.


```
pdm run python -m logrepl -c example.ini setup replication_user
```

### Subscriber and Subscription

Create the subscriber and subscription on the target databse:

```
pdm run python -m logrepl -c example.ini setup subscriber
pdm run python -m logrepl -c example.ini setup subscription
```


### Status

Verify the status of the subscription on the target database.


```
pdm run python -m logrepl -c example.ini status
```

If you see status down, the replication setup failed. Inspect the logs on the source and target databases and try again.

You can drop the provider, `replication_set`, subscriber and subscription with the teardown command:

```
pdm run python -m logrepl -c example.ini teardown subscription
pdm run python -m logrepl -c example.ini teardown subscriber
pdm run python -m logrepl -c example.ini teardown replication_set
pdm run python -m logrepl -c example.ini teardown provider
```

### Client

For miscellaneous administrative tasks, you can open a psql prompt on the source or target using the client subcommand:


```
pdm run python -m logrepl -c example.ini client [--source,--target]
```

### Tips

If you are migrating to Google CloudSQL, create your target at the Enterprise tier (not Enterprise Plus) because the Enterprise Plus tier does not have an `Outgoing IP address`. You will need to allow connections from this IP address on your source database. In addition, set the following flags on your database:


| database flag               | setting | comment |
|-----------------------------|---------|---------|
| `max_replication_slots`     | 10      |         |
| `max_worker_processes`      | 10      |         |
| `max_wal_senders`           | 10      |         | 
| `cloudsql.enable_pglogical` | `on`    | Adds pglogical to `shared_preload_libraries` |
| `cloudsql.logical_decoding` | `on`    | Sets `wal_level = logical` |


# Future

- Some refactoring
- Split DBA user from replication user on the source database
- Use `synchronize_structure` flag, may avoid needing to dump/restore the objects
- User grant and owner management commands
- Comparison commands
- Test on major cloud providers, handle, document non obvious setup.
- docker-compose and integration tests

