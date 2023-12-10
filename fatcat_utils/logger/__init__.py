import os
from .mylogger import generate_logger

logger = generate_logger(os.getenv("FATCAT_LOGGER_NAME", "FatCatLogger"))
