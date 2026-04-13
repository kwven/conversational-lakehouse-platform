CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.aisles(
    aisle_id INT PRIMARY KEY,
    aisle TEXT
);

CREATE TABLE IF NOT EXISTS source.departments(
    department_id INT PRIMARY KEY,
    department TEXT
);

CREATE TABLE IF NOT EXISTS source.products(
    product_id BIGINT PRIMARY KEY,
    product_name TEXT,
    aisle_id INT,
    department_id INT
);
CREATE TABLE IF NOT EXISTS source.orders(
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    eval_set TEXT,
    order_number INT,
    order_dow INT,
    order_hour_of_day INT,
    days_since_prior_order NUMERIC
);
CREATE TABLE IF NOT EXISTS source.order_products_prior(
    order_id BIGINT,
    product_id BIGINT,
    add_to_cart_order BIGINT,
    reordered INT,
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id
    ON source.orders(user_id);

CREATE INDEX IF NOT EXISTS idx_order_products_prior_product_id
    ON source.order_products_prior(product_id);

CREATE INDEX IF NOT EXISTS idx_products_aisle_id
    ON source.products(aisle_id);

CREATE INDEX IF NOT EXISTS idx_products_department_id
    ON source.products(department_id);