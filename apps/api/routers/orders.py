from fastapi import APIRouter, Query
from apps.api.services.trino_client import stream_query_results


router = APIRouter(prefix="/orders",tags = ["orders"])

@router.get("/demand/by-time")
def get_demand_by_time(dow:int = Query(default = 0),hour:int = Query(default = 0)):
    params = []
    sql = f"""SELECT
        order_dow,
        order_hour_of_day,
        total_orders,
        active_customer,
        avg_days_since_previous_order
        FROM gold.order_demand_by_time
        WHERE 1=1
    """

    if dow:
        sql += f"AND order_dow = ?" 
        params.append(dow)
    if hour:
        sql += f"AND order_hour_of_day = ?" 
        params.append(hour)
        
    result = list(stream_query_results(sql, params))
    return{
        "metric": "order_demand_by_time",
        "dow": dow,
        "hour": hour,
        "data": result,
    }
@router.get("/demand/peak-hours")
def get_peak_hours(limit:int = Query(default = 10,ge=1,le=50)):
    params = []
    sql = f"""SELECT
        order_dow,
        order_hour_of_day,
        total_orders,
        active_customer,
        avg_days_since_previous_order
        FROM gold.order_demand_by_time
        WHERE 1=1
    """
    if limit:
        sql+=f"ORDER BY order_dow, order_hour_of_day LIMIT ?"
        params.append(limit)
    result = list(stream_query_results(sql, params))
    return{
        "metric": "order_demand_peak_hours",
        "limit": limit,
        "data": result,
    }
