from datetime import datetime
import logging
from pathlib import Path
import sys
from loguru import logger
from .root_config import LOG_DIR


def setup_logging():
    """
    Logging Configuration.

    1. Console
    2. File: JSON-serialized
       - Rotates every 500MB.
       - Compresses old logs.
       - Retains logs for 50 days.

    3. Interception: Captures standard python logging and routes to Loguru.
    """

    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

    # session_id = uuid.uuid4().hex
    # session_file = f"{LOG_DIR}/app_{session_id}.json"

    now = datetime.now()
    timestamp = (
        now.strftime("%Y%m%d_%I%M%S")
        + f"{int(now.microsecond / 1000):03d}"
        + now.strftime("%p").lower()
    )

    session_file = f"{LOG_DIR}/app_session_{timestamp}.json"

    logger.remove()

    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="DEBUG",
        colorize=True,
        enqueue=True,
        diagnose=True,
        backtrace=True,
    )

    logger.add(
        session_file,
        rotation="500 MB",
        retention="50 days",
        compression="zip",
        serialize=True,
        level="DEBUG",
        enqueue=True,
        backtrace=False,
        diagnose=False,
    )

    class InterceptHandler(logging.Handler):
        def emit(self, record):
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno
            frame, depth = logging.currentframe(), 2

            while (
                frame is not None
                and getattr(frame, "f_code", None) is not None
                and frame.f_code.co_filename == logging.__file__
            ):
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    intercept_handler = InterceptHandler()

    logging.basicConfig(handlers=[intercept_handler], level=0, force=True)

    for lib in ["uvicorn", "uvicorn.access", "fastapi", "httpx"]:
        _logger = logging.getLogger(lib)
        _logger.handlers = [intercept_handler]
        _logger.propagate = False

    logger.info(f"Session logging initialized. File: {session_file}")
