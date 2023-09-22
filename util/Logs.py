

from apps.ConfiguredApp import Config

from datetime import datetime
import logging

LOOKUP = {
    "debug": logging.DEBUG,
    "info": logging.INFO
}

def initialize_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger(config.name)
    logger.setLevel(LOOKUP[config.log_level])
    date_str = datetime.now().strftime('%Y-%m-%d')
    log_path = f"{config.log}/{date_str}-{config.name}.log"
    fh = logging.FileHandler(log_path)
    FORMAT = "%(asctime)s | %(name)10s | %(levelname)8s | %(message)s"
    formatter = logging.Formatter(FORMAT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


class StdOutLogger:

    def debug(self, msg):
        print(f"Debug: {msg}")

    def info(self, msg):
        print(f"Info: {msg}")

    def warning(self, msg):
        print(f"Warning: {msg}")

    def error(self, msg):
        print(f"Error: {msg}")



