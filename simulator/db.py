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

 ## functions for orders table
class DB_ORDERS:
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
class DB_ORD_PRODUCTS_PRIOR:
    def get_order_id_random(cur):
        cur.execute(
            """
            SELECT order_id
            FROM source.orders
            ORDER BY random()
            LIMIT 1;
            """
        )
        order_id = cur.fetchone()[0]
        if not order_id:
            raise RuntimeError("No order found in source.orders")
        return order_id
    def get_product_id_random(cur):
        cur.execute(
            """
            SELECT product_id
            FROM source.products
            ORDER BY random()
            LIMIT 1;
            """
        )
        product_id = cur.fetchone()[0]
        if not product_id:
            raise RuntimeError("No product found in source.products")
        return product_id
    def get_add_to_cart_order(cur,order_id:int):

        cur.execute(
            """
            SELECT COALESCE(count(*) + 1, 0)
            FROM source.order_products_prior
            WHERE order_id = %s
            """,(order_id,),
        )
        add_to_cart_order = cur.fetchone()[0]
        return add_to_cart_order

class DB_AISLES:
    def get_aisle_id(cur):
        cur.execute(
            """
            SELECT max(aisle_id) + 1 
            FROM source.aisles;
            """
        )
        return cur.fetchone()[0]
class DB_PRODUCTS:
    def get_product_id_random(cur):
        cur.execute(
            """
            SELECT max(product_id) + 1 
            FROM source.products;
            """
        )
        return cur.fetchone()[0]
    def get_aisle_id(cur):
        cur.execute(
            """
            SELECT aisle_id
            FROM source.aisles
            ORDER BY random()
            LIMIT 1;
            """
        )
        return cur.fetchone()[0]
    def get_department_id(cur):
        cur.execute(
            """
            SELECT department_id
            FROM source.departments
            ORDER BY random()
            LIMIT 1;
            """
        )
        return cur.fetchone()[0]
    
class DB_DEPARTMENTS:
    def get_department_id(cur):
        cur.execute(
            """
            SELECT MAX(department_id) + 1 
            FROM source.departments
            """
        )
        return cur.fetchone()[0]
