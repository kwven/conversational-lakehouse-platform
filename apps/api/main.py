from fastapi import FastAPI

from apps.api.routers import health, products, aisles, departments, orders

app = FastAPI(
    title="Conversational Lakehouse API",
    description="Analytics API for Gold tables served through Trino.",
    version="0.1.0",
)

app.include_router(health.router)
app.include_router(products.router)
app.include_router(aisles.router)
app.include_router(departments.router)
app.include_router(orders.router)



@app.get("/")
def root():
    return {
        "name": "Conversational Lakehouse API",
        "version": "0.1.0",
        "docs": "/docs",
    }