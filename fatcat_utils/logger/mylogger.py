import os
import logging
from elasticsearch_logging_handler import ElasticHandler
from elasticsearch_logging_handler._sending_handler import ElasticSendingHandler
from elasticsearch_logging_handler._queue_handler import ObjectQueueHandler
from .formatter import Formatter
from .elastic_config import ELASTIC_CONFIG


def generate_logger(logger_name: str) -> logging.Logger:
    """Generates a logger

    :param logger_name: The name for the logger
    :type logger_name: str
    :return: The logger
    :rtype: logging.Logger
    """
    level = logging.DEBUG if os.getenv(
        "FATCAT_LOG_DEBUG", "0") == "1" else logging.INFO
    logging.basicConfig(level=level)

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    # for handler in logging.root.handlers:
    #     logging.root.removeHandler(handler)

    file_handler = logging.FileHandler(
        filename=f"{logger_name.lower().replace('logger', '')}.log")
    file_handler.setFormatter(Formatter())

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(Formatter())

    if ELASTIC_CONFIG["host"] is not None:
        def prepare_hook(self, record: logging.LogRecord):
            """A hook for the prepare function in the elastic logger queue handler

            :param record: The log record
            :type record: logging.LogRecord
            :return: The same as the original function, with slight modifications
            :rtype: The same as the original function
            """
            record.actual_exc_info = record.exc_info
            record.actual_exc_text = record.exc_text
            return self.prepare_old(record)

        def __prepare_action_hook(self, record: logging.LogRecord) -> dict:
            """A hook for the __prepare_action function in the elastic logger sending handler

            :param record: The log record
            :type record: logging.LogRecord
            :return: The action with slight modifications from the original version
            :rtype: dict
            """
            action = self.__prepare_action_old(record)
            action["logger"] = logger_name
            action["func"] = record.funcName
            action["exc_info"] = logging.Formatter().formatException(
                record.actual_exc_info) if record.actual_exc_info is not None else None
            action["exc_text"] = record.actual_exc_text

            return action

        url = ELASTIC_CONFIG['host']
        if ELASTIC_CONFIG["password"] is not None and ELASTIC_CONFIG["username"] is not None:
            url = f"{ELASTIC_CONFIG['username']}:{ELASTIC_CONFIG['password']}@{url}"
        elastic_handler = ElasticHandler(
            host=url, index=ELASTIC_CONFIG["index_name"], flush_period=ELASTIC_CONFIG["flush_period"], batch_size=ELASTIC_CONFIG["batch_size"])

        # Le hooks:
        queue_handler = elastic_handler._queue_handler
        setattr(queue_handler, "prepare_old", queue_handler.prepare)
        # pylint: disable-next=no-value-for-parameter
        setattr(queue_handler, "prepare", prepare_hook.__get__(
            queue_handler, ObjectQueueHandler))  

        handler = elastic_handler._queue_listener.handlers[0]
        setattr(handler, "__prepare_action_old",
                handler._ElasticSendingHandler__prepare_action)
        # pylint: disable-next=no-value-for-parameter
        setattr(handler, "_ElasticSendingHandler__prepare_action", __prepare_action_hook.__get__(
            handler, ElasticSendingHandler))
        logger.addHandler(elastic_handler)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger
