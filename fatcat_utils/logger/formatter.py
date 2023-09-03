import logging


class Formatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if record.levelno == logging.INFO:
            log_format = "[+] [%(asctime)s] [%(levelname)s] %(name)s (%(funcName)s) - %(message)s"
        elif record.levelno == logging.DEBUG:
            log_format = "[*] [%(asctime)s] [%(levelname)s] %(name)s (%(funcName)s) - %(message)s"
        elif record.levelno in (logging.WARNING, logging.WARN):
            log_format = "[?] [%(asctime)s] [%(levelname)s] %(name)s (%(funcName)s) - %(message)s"
        elif record.levelno == logging.ERROR:
            log_format = "[-] [%(asctime)s] [%(levelname)s] %(name)s (%(funcName)s) - %(message)s"
        elif record.levelno == logging.CRITICAL:
            log_format = "[!] [%(asctime)s] [%(levelname)s] %(name)s (%(funcName)s) - %(message)s"
        else:
            return super().format(record)
        
        return logging.Formatter(log_format).format(record)
