DROP PUBLICATION IF EXISTS dbz_publication;

CREATE PUBLICATION dbz_publication
FOR TABLE
    source.orders,
    source.order_products_prior,
    source.products,
    source.aisles,
    source.departments;