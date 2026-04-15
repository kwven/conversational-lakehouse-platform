import os 
import psycopg2
from psycopg2 import sql
import pandas as pd
from psycopg2.extras import execute_batch
from pathlib import Path
import sys

DB_CONFIG = {
        'host': os.getenv('POSTGRES_HOST','localhost'),
        'port': os.getenv('POSTGRES_PORT','5432'),
        'database': os.getenv('POSTGRES_DB','instacart'),
        'user': os.getenv('POSTGRES_USER','postgres'),
        'password': os.getenv('PASSWORD','postgres'),
}

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("connected to instacart successfuly")
        return conn
    except Exception as e:
        print(f"Error connecting to instacart: {e}")
    sys.exit(1)

def create_tables(schema_path, conn):
    cursor = conn.cursor()
    #schema_path = Path('/infra/postgres/init/01_schema.sql')
    try:
        cursor.execute(schema_path.read_text())
    except Exception as e:
        conn.rollback()
        print(f"Error creating tables: {e}")
    cursor.close()

def load_data(file_path,table_name,conn):
    cursor = conn.cursor()
    #file_path = Path('/data/instacart/aisles.csv')
    copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)"
    truncate_sql = f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"
    try:
        cursor.execute(truncate_sql)
        with open(file_path , 'r',encoding = 'utf-8') as f:
            #reader = csv.reader(f,delimiter = ',')
            #next(reader)
            cursor.copy_expert(copy_sql, f)
        conn.commit()
        print(f"Data loaded into {table_name} successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data into {table_name} : {e}")
    finally:
        cursor.close()

if __name__ == '__main__':
    conn = get_db_connection()
    create_tables(Path('infra/postgres/init/01_schema.sql'), conn)
    file_path = {
        'source.aisles': 'data/instacart/aisles.csv',
        'source.departments': 'data/instacart/departments.csv',
        'source.orders': 'data/instacart/orders.csv',
        'source.products': 'data/instacart/products.csv',
        'source.order_products_prior': 'data/instacart/order_products__prior.csv',
    }
    for table_name , file_path in file_path.items():
        load_data(file_path,table_name,conn)
    conn.commit()


