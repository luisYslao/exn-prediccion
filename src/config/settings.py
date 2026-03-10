import os
from dotenv import load_dotenv, find_dotenv


# Load variables from the .env file if it exists
_dotenv_path = find_dotenv()
if _dotenv_path:
    load_dotenv(_dotenv_path)

class Settings:
    def __init__(self):
        self.ENVIRONMENT = os.getenv("ENVIRONMENT") or "development"
        self.LOG_LEVEL = os.getenv("LOG_LEVEL") or "INFO"
        self.JDBC_URL = os.getenv("JDBC_URL")
        self.JDBC_USER = os.getenv("JDBC_USER")
        self.JDBC_PASSWORD = os.getenv("JDBC_PASSWORD")
        self.START_YEAR = int(os.getenv("START_YEAR"))
        self.END_YEAR = int(os.getenv("END_YEAR"))
        self.DOUBLE_FROM_YEAR = int(os.getenv("DOUBLE_FROM_YEAR"))
        self.EXECUTION_DAY = os.getenv("EXECUTION_DAY")
        self.LAG_EXECUTION = int(os.getenv("LAG_EXECUTION"))
        self.QUEUE_NAME = os.getenv("QUEUE_NAME")
        self.QUEUE_CN = os.getenv("QUEUE_CN")


# Do not run this file directly.
if __name__ != "__main__":
    settings = Settings()
else:
    raise RuntimeError("This file is not meant to be executed directly.")
