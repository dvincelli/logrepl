from logrepl.db import source_db, target_db
from loguru import logger


def verify_config(config):
    logger.info("Verifying the configuration")

    verification_passed = True

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
                verification_passed = False

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
                verification_passed = False

            cur.execute("SHOW max_worker_processes")
            result = cur.fetchone()
            max_worker_processes = int(result[0])
            logger.info(f"max_worker_processes: {max_worker_processes}")
            if max_worker_processes < 10:
                logger.error("max_worker_processes is less than 10 on source database")
                verification_passed = False

            cur.execute("SHOW max_replication_slots")
            result = cur.fetchone()
            max_replication_slots = int(result[0])
            logger.info(f"max_replication_slots: {max_replication_slots}")
            if max_replication_slots < 10:
                logger.error("max_replication_slots is less than 10 on source database")
                verification_passed = False

            cur.execute("SHOW max_wal_senders")
            result = cur.fetchone()
            max_wal_senders = int(result[0])
            logger.info(f"max_wal_senders: {max_wal_senders}")
            if max_wal_senders < 10:
                logger.error("max_wal_senders is less than 10 on source database")
                verification_passed = False

            # PGLogical does not support replication between databases with different encoding.
            # We recommend using UTF-8 encoding in all replicated databases.
            cur.execute("SHOW server_encoding")
            result = cur.fetchone()
            server_encoding = result[0]
            logger.info(f"server_encoding: {server_encoding}")
            if server_encoding != "UTF8":
                logger.error("server_encoding is not set to 'UTF8' on source database")
                verification_passed = False

    logger.info("Verifying the target database settings:")
    with target_db(config, dbname="template1") as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            result = cur.fetchone()
            wal_level = result[0]
            logger.info(f"wal_level: {wal_level}")
            if wal_level != "logical":
                logger.error("wal_level is not set to 'logical' on target database")
                verification_passed = False

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
                verification_passed = False

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
                verification_passed = False

    if verification_passed:
        logger.info("Configuration verification passed")
    else:
        logger.error("Configuration verification failed")

    return verification_passed
