from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class DBAccessInfo:
    uri: str
    database: str
    collection: str


MONGO_DRIVER: Final[str] = "mongodb"


MONGO_HOST: Final[str] = ""
MONGO_PORT: Final[str] = ""
MONGO_USER: Final[str] = ""
MONGO_PASSWORD: Final[str] = ""
