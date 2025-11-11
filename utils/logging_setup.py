import logging
from rich.logging import RichHandler
import os
from datetime import datetime

def setup_logging():
    """Configures the root logger for file and console output."""
    
    DETAILED_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s"
    
    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{LOG_DIR}/app_run_{timestamp}.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) 

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    file_handler = logging.FileHandler(file_name, mode='w')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(DETAILED_FORMAT))
    root_logger.addHandler(file_handler)

    console_handler = RichHandler(rich_tracebacks=True, tracebacks_show_locals=True, markup=True)
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)