import psycopg2
from config import Config

def get_connect():
    return psycopg2.connect(
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        database=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
    )

class DB_ORDERS:

    ## functions for orders table
    def get_user_id_random(cur):
        cur.execute(
            """
            SELECT user_id
            FROM source.orders
            ORDER BY random()
            LIMIT 1;
            """
        )
        user_id = cur.fetchone()[0]
        if not user_id:
            raise RuntimeError("No user found in source.orders")
        return user_id

    def get_next_order_id(cur):
        cur.execute(
            """
            SELECT COALESCE(MAX(order_id) + 1, 0)
            FROM source.orders
            """
        )
        order_id = cur.fetchone()[0]
        return order_id

    def get_next_orderNumber_user(cur, user_id):
        cur.execute(
            """
            SELECT COALESCE(MAX(order_number) + 1, 0)
            FROM source.orders
            WHERE user_id = %s
            """,
            (user_id,),
        )
        order_number = cur.fetchone()[0]
        return order_number


## functions for order products prior table
