import psycopg2

from config import Config
from db import DB_ORDERS, get_connect
import random
from typing import Optional
import time
import signal

RUNNING = True


# usefull function so we can handle stoping simulator without let any insert or commit give a exception and avoid problems in pipline 

def handle_stop(_signum, _frame):
    global RUNNING
    RUNNING = False
    print("\nStopping simulator...")

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def insert_order(cur, user_id: int) -> int:
    order_dow = random.randint(0, 6)
    order_hour_of_day = random.randint(6, 22)
    days_since_prior_order: Optional[int] = random.randint(1, 30)
    SIMULATED_EVAL_SET = Config.SIMULATED_EVAL_SET
    next_order = DB_ORDERS.get_next_order_id(cur)
    next_orderNumber = DB_ORDERS.get_next_orderNumber_user(cur, user_id)
    cur.execute(
        """
        INSERT INTO source.orders (
            order_id,
            user_id,
            eval_set,
            order_number,
            order_dow,
            order_hour_of_day,
            days_since_prior_order
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            next_order,
            user_id,
            SIMULATED_EVAL_SET,
            next_orderNumber,
            order_dow,
            order_hour_of_day,
            days_since_prior_order,
        ),
    )
    return next_order


def main():
    conn = get_connect()
    user_id = None
    next_order = None
    sleep_seconds = 0

    try:
        with conn.cursor() as cur:
            while RUNNING:
                try:
                    user_id = DB_ORDERS.get_user_id_random(cur)
                    next_order = insert_order(cur, user_id)
                    conn.commit()
                    print(f"insert order {next_order} for user {user_id}")
                    sleep_seconds = random.randint(
                        Config.MIN_SLEEP_SECONDS,
                        Config.MAX_SLEEP_SECONDS,
                    )
                    time.sleep(sleep_seconds)
                except Exception as e:
                    conn.rollback()
                    print(f"insert order failed for user {user_id}: {e}")
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
    finally:
        conn.close()
        print("Simulator stopped")

if __name__ == "__main__":
    main()

