import json
from typing import Any
import aio_pika
from fatcat_utils.logger import logger
from .base_rabbit import BaseRabbitMQ


class Publisher(BaseRabbitMQ):
    async def send_to_queue(self, message: Any, queue_name: str):
        """Sends a message to the queue

        :param message: The message we wanna send
        :type message: Any
        :param queue_name: The queue we wanna send the message to
        :type queue_name: str
        :raises TypeError: Raises if `queue_name` is not a string
        """
        logger.debug(f"Sending a message to queue {queue_name}")
        if not isinstance(queue_name, str):
            logger.error("Queue name should be a string")
            raise TypeError("`queue_name` object should be a string.")
        
        channel = await self._server.channel()
        await channel.declare_queue(queue_name)
        logger.info(f"Sending message to queue {queue_name}")
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message, indent=2).encode("utf-8") if isinstance(message, dict) else message if isinstance(message, bytes) else str(message).encode("utf-8")),
            routing_key=queue_name,
        )
        logger.info(f"Message sent to queue {queue_name}")
