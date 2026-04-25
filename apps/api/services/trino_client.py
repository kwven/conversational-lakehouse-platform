from trino.dbapi import connect
import os
from typing import Any


from apps.api.config import (
    TRINO_CATALOG,
    TRINO_HOST,
    TRINO_PORT,
    TRINO_SCHEMA,
    TRINO_USER,
)

def get_trino_client():
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
    )

def stream_query_results(query:str,params:list[Any] = None):
    conn = get_trino_client()
    curr = conn.cursor()
    try:
        curr.execute(query,params)
        columns = [column[0] for column in curr.description]
        for row in curr:
            yield dict(zip(columns, row))
    finally:
        curr.close()
        conn.close()
