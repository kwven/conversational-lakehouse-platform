import psycopg2
from config import Config
from db import get_connect,DB_PRODUCTS
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

def insert_into_products(cur,product_id,product_name):
    aisle_id = DB_PRODUCTS.get_aisle_id(cur)
    department_id = DB_PRODUCTS.get_department_id(cur)
    cur.execute(
        """
        INSERT INTO source.products (
        product_id,
        product_name,
        aisle_id,
        department_id)
        VALUES (%s, %s, %s, %s)
        """,
        (product_id,product_name,aisle_id,department_id),
        )
    return product_id

def main():
    conn = get_connect()
    number = 1
    try:
        with conn.cursor() as cur:
            while RUNNING:
                try:
                    product_id = DB_PRODUCTS.get_product_id_random(cur)
                    product_name = f"product test {number}"
                    insert_into_products(cur,product_id,product_name)
                    conn.commit()
                    sleep_seconds = random.randint(
                            Config.MIN_SLEEP_SECONDS,
                            Config.MAX_SLEEP_SECONDS,)
                    time.sleep(sleep_seconds)
                    print(f"insert product {product_id} test {number}")
                    number +=1
                except Exception as e:
                    conn.rollback()
                    print(f"product {product_id} failed: {e}")
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
    finally:
        conn.close()
        print("Simulator stopped")

if __name__ == "__main__":
    main()