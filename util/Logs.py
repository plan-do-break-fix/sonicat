

from apps.ConfiguredApp import Config
from typing import Dict, List

from datetime import datetime
import logging


LOOKUP = {
    "debug": logging.DEBUG,
    "info": logging.INFO
}

def new_initialize_logging(facility, level, path) -> logging.Logger:
    logger = logging.getLogger(facility)
    logger.setLevel(LOOKUP[level])
    date_str = datetime.now().strftime('%Y-%m-%d')
    log_path = f"{path}/{date_str}-{facility}.log"
    fh = logging.FileHandler(log_path)
    FORMAT = "%(asctime)s | %(name)10s | %(levelname)8s | %(message)s"
    formatter = logging.Formatter(FORMAT)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


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


class Utility:

    @staticmethod
    def remove_duplicate_lines(lines: List[str]) -> List[str]:
        output = [lines[0]]
        for _line in lines:
            if not _line == output[-1]:
                output.append(_line)
        return output

    @staticmethod
    def log_date_overflow(log_name: str, lines: List[str]) -> bool:
        log_date = log_name[:10]
        return all([_l.startswith(log_date) for _l in lines])

    @staticmethod
    def split_lines_by_date(log_name: str, lines: List[str]) -> Dict[str, List[str]]:
        output = {}
        for _l in lines:
            if _l[:10] not in output.keys():
                output[_l[:10]] = []
            output[_l[:10]].append(_l)
        return output

    @staticmethod
    def insert_log_lines(log_name: str, lines: List[str]):
        pass
    
    
    @staticmethod
    def archive_old_logs(logdir: str):
        pass



