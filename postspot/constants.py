from enum import Enum


class Environment(Enum):
    PRODUCTION = "production"
    DEVELOPMENT = "development"
    LOCAL = "local"
    BUILD = "build"


class AccountStatus(Enum):
    OPEN = 0
    CLOSED = 1
    SUSPENDED = 2
