

import logging



def initialize_logging(name: str, log_path: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(log_path)
    FORMAT = "%(asctime)s | %(name)10s | %(levelname)8s | %(message)s"
    formatter = logging.Formatter(FORMAT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
