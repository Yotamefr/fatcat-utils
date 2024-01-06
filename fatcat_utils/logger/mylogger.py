import os
import logging
from elasticsearch_logging_handler import ElasticHandler
from .formatter import Formatter
from .elastic_config import ELASTIC_CONFIG


def generate_logger(logger_name: str) -> logging.Logger:
    """Generates a logger

    :param logger_name: The name for the logger
    :type logger_name: str
    :return: The logger
    :rtype: logging.Logger
    """
    if logger_name in logging.Logger.manager.loggerDict:
        return logging.getLogger(logger_name)
    
    level = logging.DEBUG if os.getenv(
        "FATCAT_LOG_DEBUG", "0") == "1" else logging.INFO
    logging.basicConfig(level=level)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)

    file_handler = logging.FileHandler(
        filename=f"{logger_name.lower().replace('logger', '')}.log")
    file_handler.setFormatter(Formatter())

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(Formatter())

    if ELASTIC_CONFIG["host"] is not None:
        url = ELASTIC_CONFIG['host']

        if ELASTIC_CONFIG["password"] is not None and ELASTIC_CONFIG["username"] is not None:
            url = f"{ELASTIC_CONFIG['username']}:{ELASTIC_CONFIG['password']}@{url}"
        
        elastic_handler = ElasticHandler(
            host=url, index=ELASTIC_CONFIG["index_name"], flush_period=ELASTIC_CONFIG["flush_period"], batch_size=ELASTIC_CONFIG["batch_size"])

        logger.addHandler(elastic_handler)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger
