from psycopg import sql
from loguru import logger


def subscription_status(conn, subscription):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT status FROM pglogical.show_subscription_status(%s)"),
            [subscription],
        )
        result = cur.fetchone()
        status = result[0]
        logger.info(f"Subscription status: {status}")
        return status
