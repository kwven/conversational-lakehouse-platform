import os 
class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "instacart")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
    SIMULATED_EVAL_SET = os.getenv("SIMULATED_EVAL_SET", "simulated")
    MIN_SLEEP_SECONDS = int(os.getenv("MIN_SLEEP_SECONDS", "3"))
    MAX_SLEEP_SECONDS = int(os.getenv("MAX_SLEEP_SECONDS", "8"))