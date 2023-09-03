import os
from typing import TypedDict, Optional


class IElasticConfig(TypedDict):
    host: Optional[str]
    username: Optional[str]
    password: Optional[str]
    index_name: Optional[str]
    flush_period: float
    batch_size: int


ELASTIC_CONFIG: IElasticConfig = {
    "host": os.getenv("ELASTIC_HOST_LOGGER"),
    "username": os.getenv("ELASTIC_USER_LOGGER"),
    "password": os.getenv("ELASTIC_PASSWORD_LOGGER"),
    "index_name": os.getenv("ELASTIC_INDEX_LOGGER"),
    "flush_period": float(os.getenv("ELASTIC_FLUSH_PERIOD_LOGGER", "1")),
    "batch_size": int(os.getenv("ELASTIC_BATCH_SIZE_LOGGER", "1_000"))
}
