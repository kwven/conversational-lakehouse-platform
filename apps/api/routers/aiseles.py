from fastapi import APIRouter, Query
from apps.api.services.trino_client import stream_query_results
router = APIRouter(prefix="/aisles",tags = ["aisles"])

router.get("/aisle")
def get_aisle(id:int):
    params = []
    sql = f"""
            SELECT 
                aisle_id,
                aisle,
                department,
                total_items_sold,
                distinct_orders,
                unique_customers,
                distinct_products
            FROM gold.top_aisles_clean
            WHERE 1=1
    """
    sql += f"AND aisle_id = ?"
    params.append(id)
    result = list(stream_query_results(sql, params))
    return {
        "metric": "aisle",
        "id": id,
        "data": result[0],
    }

router.get("/top")
def get_aisle_details(limit:int = Query(default=10,ge=1,le=50)):
    params= []
    sql = f"""
        SELECT
            aisle_id,
            aisle,
            department,
            total_items_sold,
            distinct_orders,
            unique_customers,
            distinct_products
        FROM gold.top_aisles_clean
        WHERE 1=1
    """

    if limit:
        sql += f"ORDER BY aisle_rank ASC LIMIT ?"
        params.append(limit)
    result = list(stream_query_results(sql, params))
    return {
        "metric": "top_aisles_clean",
        "limit": limit,
        "data": result, 
    }

router.get("/by-department")
def get_aisles_by_department(limit:int = Query(default=10,ge=1,le=50),department:str | None = Query(None)):
    params= []
    sql = f"""
        SELECT
            aisle_id,
            aisle,
            department,
            total_items_sold,
            distinct_orders,
            unique_customers,
            distinct_products,
        FROM gold.top_aisles_clean
        WHERE 1=1
    """
    if department:
        sql += f"WHERE department = ?"
        params.append(department)
    if limit:
        sql += f"ORDER BY aisle_rank ASC LIMIT ?"
        params.append(limit)
    result = list(stream_query_results(sql, params))
    return {
        "metric": "top_aisles_clean",
        "limit": limit,
        "data": result, 
    }
