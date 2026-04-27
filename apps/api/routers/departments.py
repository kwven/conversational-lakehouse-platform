from fastapi import APIRouter, Query
from apps.api.services.trino_client import stream_query_results
router = APIRouter(prefix="/departments",tags = ["departments"])

router.get("/department")
def get_department(id:int):
    params = []
    sql = f"""
            SELECT 
                department_id,
                department,
                total_items,
                total_reorders,
                reorder_rate_pct,
                distinct_orders,
                distinct_customers,
                department_rank
            FROM gold.departments_clean
            WHERE 1=1
            """
    sql += f"AND department_id = ?"
    params.append(id)
    result = list(stream_query_results(sql, params))
    return {
        "metric": "department",
        "id": id,
        "data": result[0],
    }

router.get("/reorder-rate")
def get_reorder_rate_by_department(limit:int = Query(default = 10,ge=1,le=50)):
    params = []
    sql = f"""
        SELECT 
            department_id,
            department,
            total_items,
            total_reorders,
            reorder_rate_pct,
            distinct_orders,
            distinct_customers,
            department_rank
        FROM gold.reorder_rate_by_department_clean
        WHERE 1=1
    """
    if limit:
        sql+=f"ORDER BY reorder_rate_pct LIMIT ?"
        params.append(limit)
    result = list(stream_query_results(sql, params))
    return{
        "metric": "department_reorder_rate",
        "limit": limit,
        "data": result,
    }
