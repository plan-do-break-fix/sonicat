

from apps.Helpers import Config

from datetime import datetime
import logging

LOOKUP = {
    "debug": logging.DEBUG,
    "info": logging.INFO
}

def initialize_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger(config.app_name)
    logger.setLevel(LOOKUP[config.log_level])
    date_str = datetime.now().strftime('%Y-%m-%d')
    log_path = f"{config.logging}/{date_str}-{config.app_name}.log"
    fh = logging.FileHandler(log_path)
    FORMAT = "%(asctime)s | %(name)10s | %(levelname)8s | %(message)s"
    formatter = logging.Formatter(FORMAT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
