import os
from dotenv import load_dotenv


load_dotenv()

# "production" in your deployment environment
APP_ENV: str = os.getenv("APP_ENV", "development")

# Directory to store log files
LOG_DIR: str = "logs"
