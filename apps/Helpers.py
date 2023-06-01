
from dataclasses import dataclass



@dataclass
class Config:
    app_name: str
    root: str
    data: str
    logging: str
    log_level: str
    covers: str
    intake: str