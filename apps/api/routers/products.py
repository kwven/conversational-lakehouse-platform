from fastapi import APIRouter, Query

from apps.api.services.trino_client import stream_query_results
router = APIRouter(prefix="/products",tags = ["products"])

@router.get("/top")
def get_top_products(limit:int = Query(default=10,ge=1,le=50),department:str | None = Query(None)):
    params = []
    sql = f"""
        SELECT
            product_id,
            product_name,
            aisle,
            department,
            total_purchases,
            distinct_orders,
            unique_users,
            total_reorders,
            reorder_rate_pct,
            product_rank
        FROM gold.top_products_clean
        WHERE 1=1
    """ 
    if department:
        sql += f"WHERE department = ?"
        params.append(department)
    if limit:
        sql += f"ORDER BY product_rank ASC LIMIT ?"
        params.append(limit)
    result = list(stream_query_results(sql, params))
    return {
        "metric": "top_products_clean",
        "limit": limit,
        "data": result, 
    }
