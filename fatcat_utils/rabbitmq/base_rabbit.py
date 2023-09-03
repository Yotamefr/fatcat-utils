import aio_pika
from .exceptions import NotConnected
from typing import Any
from .config import RABBIT_CONFIG
from fatcat_utils.logger import logger



class BaseRabbitMQ:
    shared_instance: Any = None

    def __init__(self):
        self.host: str = RABBIT_CONFIG["hostname"]
        self.port: int = RABBIT_CONFIG["port"]
        self.username: str = RABBIT_CONFIG["username"]
        self.password: str = RABBIT_CONFIG["password"]
        self._server: aio_pika.connection.Connection = None

    def __new__(cls):
        if cls.shared_instance is None:
            cls.shared_instance = object.__new__(cls)
        return cls.shared_instance
    
    async def connect(self):
        """Connects to the RabbitMQ server
        """
        logger.debug("Connecting to the Rabbit server")
        self._server = await aio_pika.connect(
            host=self.host, 
            port=self.port, 
            login=self.username, 
            password=self.password
        )
        await self._server.connect()
    
    async def close(self):
        """Closes the RabbitMQ connection

        :raises NotConnected: Raises if already not connected
        """
        logger.debug("Closing the Rabbit connection")
        if self._server.is_closed:
            raise NotConnected(f"Not connected to server {self.host}")
        await self._server.close()
    

    async def send_to_queue(self, message: Any, queue_name: str):
        """Not implemented. Here just so I can use it in `Consumer`

        :param message: The message we want to send
        :type message: Any
        :param queue_name: The queue we want to send the message to
        :type queue_name: str
        :return: Not Implemented
        :rtype: NotImplemented
        """
        return NotImplemented

    