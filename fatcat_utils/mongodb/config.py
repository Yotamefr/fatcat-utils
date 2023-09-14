import os
from typing import Optional
from typing_extensions import TypedDict


class MongoConfig(TypedDict):
    hostname: str
    port: int
    username: Optional[str]
    password: Optional[str]
    database_name: str
    queues_collection: str
    schemas_collection: str
    failed_collection: str
    worker_failed_collection: str


MONGO_CONFIG: MongoConfig = {
    "hostname": os.getenv("FATCAT_MONGO_HOSTNAME", "localhost"),
    "port": int(os.getenv("FATCAT_MONGO_PORT", "27017")),
    "username": os.getenv("FATCAT_MONGO_USERNAME", None),
    "password": os.getenv("FATCAT_MONGO_PASSWORD", None),
    "database_name": os.getenv("FATCAT_MONGO_DATABASE", "FatCat"),
    "queues_collection": os.getenv("FATCAT_MONGO_QUEUES_COLLECTION", "FatCatQueues"),
    "schemas_collection": os.getenv("FATCAT_MONGO_SCHEMAS_COLLECTION", "FatCatSchemas"),
    "failed_collection": os.getenv("FATCAT_MONGO_FAILED_COLLECTION", "FatCatFailed"),
    "worker_failed_collection": os.getenv("FATCAT_MONGO_WORKER_FAILED_COLLECTION", "FatCatWorkerFailed")
}
