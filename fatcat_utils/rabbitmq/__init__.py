from .consumer import Consumer, AckTime, MessageStatus
from .publisher import Publisher
from .config import RABBIT_CONFIG


class RabbitMQHandler(Consumer, Publisher):
    pass
