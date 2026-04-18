import psycopg2
from config import Config
from db import get_connect,DB_DEPARTMENTS
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

def insert_into_departments(cur,department_id,number):
    department = f"department_test_{number}"
    cur.execute(
        """
        INSERT INTO source.departments (
        department_id,
        department)
        VALUES (%s, %s)
        """,
        (department_id,department),
        )
    return department_id

def main():
    conn = get_connect()
    number = 1
    try:
        with conn.cursor() as cur:
            while RUNNING:
                try:
                    department_id = DB_DEPARTMENTS.get_department_id(cur)
                    insert_into_departments(cur,department_id,number)
                    conn.commit()
                    sleep_seconds = random.randint(
                            Config.MIN_SLEEP_SECONDS,
                            Config.MAX_SLEEP_SECONDS,)
                    time.sleep(sleep_seconds)
                    print(f"insert department {department_id} with test number {number}")
                    number +=1
                except Exception as e:
                    conn.rollback()
                    print(f"department {department_id} failed: {e}")
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
    finally:
        conn.close()
        print("Simulator stopped")

if __name__ == "__main__":
    main()