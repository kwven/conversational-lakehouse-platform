import psycopg2
from config import Config
from db import get_connect,DB_AISLES
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

def insert_into_aisles(cur,aisle_id,number):
    aisle = f"aisle_test_{number}"
    cur.execute(
        """
        INSERT INTO source.aisles (
        aisle_id,
        aisle)
        VALUES (%s, %s)
        """,
        (aisle_id,aisle),
        )
    return aisle_id

def main():
    conn = get_connect()
    number = 1
    try:
        with conn.cursor() as cur:
            while RUNNING:
                try:
                    aisle_id = DB_AISLES.get_aisle_id(cur)
                    insert_into_aisles(cur,aisle_id,number)
                    conn.commit()
                    sleep_seconds = random.randint(
                            Config.MIN_SLEEP_SECONDS,
                            Config.MAX_SLEEP_SECONDS,)
                    time.sleep(sleep_seconds)
                    print(f"insert aisle {aisle_id} with test number {number}")
                    number +=1
                except Exception as e:
                    conn.rollback()
                    print(f"aisle {aisle_id} failed: {e}")
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
    finally:
        conn.close()
        print("Simulator stopped")

if __name__ == "__main__":
    main()