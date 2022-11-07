from enum import Enum, unique


@unique
class ETLStep(Enum):
    SOURCING = "sourcing"
    LOG_0 = "log_0"
    LOG_1 = "log_1"
