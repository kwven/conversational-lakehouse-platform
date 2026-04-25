import os

TRINO_HOST = os.getenv("TRINO_HOST","trino")
TRINO_PORT = int(os.getenv("TRINO_PORT","8080"))
TRINO_USER = os.getenv("TRINO_USER","api")
TRINO_CATALOG = os.getenv("TRINO_CATALOG","delta")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA","gold")
