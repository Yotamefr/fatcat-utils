import logging
import asyncio
import os
from .rabbitmq import RabbitMQHandler
from .mongodb import DatabaseHandler
from .logger import generate_logger


class FatCat:
    """This is the FatCat class.

    When calling the run method, the code will run as long as the
    rabbitmq listeners are active.
    """
    rabbit: RabbitMQHandler = RabbitMQHandler()
    mongo: DatabaseHandler = DatabaseHandler()
    logger: logging.Logger

    def __init__(self, logger_name: str=None):
        if logger_name is not None:
            self.logger = generate_logger(logger_name)
        else:
            self.logger = generate_logger("FatCatLogger")
    
    def run(self):
        """The run function.

        Call this function to start the worker.
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.start())
        loop.call_later(5, self.check_listeners_status)
        loop.run_forever()

    async def start(self):
        """THe start function.
        
        Sets up the mongo and connectes to the rabbit
        """
        self.logger.info("Starting up the Rabbit handler")
        await self.rabbit.connect()
        self.logger.info("Rabbit handler was been connected")
        self.logger.info("Setting up the Mongo environment")
        await self.mongo.setup()
        self.logger.info("Mongo has been set up")

    def check_listeners_status(self):
        """Checks if there are any active listeners. If not, kills the program.
        """
        loop = asyncio.get_event_loop()
        if len(self.rabbit._background_listeners) == 0:
            loop.call_later(0.1, self.rabbit.close)  # Hacky hack
            loop.stop()
        else:
            loop.call_later(5, self.check_listeners_status)
