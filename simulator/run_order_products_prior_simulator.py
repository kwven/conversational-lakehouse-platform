import psycopg2
from config import Config
from db import get_connect,DB_ORD_PRODUCTS_PRIOR
import signal 
import random
import time

RUNNING = True
def handle_stop(_signum, _frame):
    global RUNNING
    RUNNING = False
    print("\nStopping the simulator...")

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def insert_into_order_products_prior(cur,order_id,product_id):
    add_to_cart_order = DB_ORD_PRODUCTS_PRIOR.get_add_to_cart_order(cur,order_id)
    reordered = random.randint(0,1)
    cur.execute(
        """
        INSERT INTO source.order_products_prior (
        order_id,
        product_id,
        add_to_cart_order,
        reordered)
        VALUES (%s, %s, %s, %s)
        """,
        (order_id,product_id,add_to_cart_order,reordered),
        )
    return order_id

def main():
    conn = get_connect()
    try:
        with conn.cursor() as cur:
            while RUNNING:
                try: 
                    order_id = DB_ORD_PRODUCTS_PRIOR.get_order_id_random(cur)
                    product_id = DB_ORD_PRODUCTS_PRIOR.get_product_id_random(cur)
                    insert_into_order_products_prior(cur)
                    conn.commit()
                    sleep_seconds = random.randint(
                            Config.MIN_SLEEP_SECONDS,
                            Config.MAX_SLEEP_SECONDS,)
                    time.sleep(sleep_seconds)
                    print(f"insert order {order_id} product {product_id}")
                except Exception as e:
                    conn.rollback()
                    print(f"order {order_id} product {product_id} failed: {e}")
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
    finally:
        conn.close()
        print("Simulator stopped")

if __name__ == "__main__":
    main()