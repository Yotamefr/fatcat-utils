import os
from typing import TypedDict


class RabbitConfig(TypedDict):
    hostname: str
    port: int
    username: str
    password: str
    incoming_queue: str
    failed_queue: str


RABBIT_CONFIG: RabbitConfig = {
    "hostname": os.getenv("FATCAT_RABBIT_HOSTNAME", "localhost"),
    "port": int(os.getenv("FATCAT_RABBIT_PORT", "5672")),
    "username": os.getenv("FATCAT_RABBIT_USERNAME", "guest"),
    "password": os.getenv("FATCAT_RABBIT_PASSWORD", "guest"),
    "incoming_queue": os.getenv("FATCAT_RABBIT_INCOMING_QUEUE", "incoming"),
    "failed_queue": os.getenv("FATCAT_RABBIT_FAILED_QUEUE", "failed")
}
