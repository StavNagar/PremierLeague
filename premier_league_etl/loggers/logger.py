import os
import logging
from loggers.logger_formatter import get_formatter

def get_logger(name: str, log_file: str="app.log"):
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        os.makedirs("logs", exist_ok=True)

        file_handler = logging.FileHandler(os.path.join("logs", log_file))
        file_handler.setFormatter(get_formatter())

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(get_formatter())

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger