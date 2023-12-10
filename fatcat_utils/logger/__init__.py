import os
from .mylogger import generate_logger

logger = generate_logger(os.getenv("LOGGER_NAME", "FatCatLogger"))
