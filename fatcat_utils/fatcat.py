import logging
import asyncio
import inspect
from typing import List, Coroutine, Literal
from .plugin import FatCatPlugin, PluginListener
from .rabbitmq import RabbitMQHandler, ListenerGroup, AckTime
from .rabbitmq.group import Listener
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
    _listeners: List[PluginListener]

    def __init__(self, logger_name: str=None):
        if logger_name is not None:
            self.logger = generate_logger(logger_name)
        else:
            self.logger = generate_logger("FatCatLogger")
        
        self._listeners = []
    
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

        self.logger.info("Calling all Plugin Listeners")
        for listener in self._listeners:
            await listener()

    def check_listeners_status(self):
        """Checks if there are any active listeners. If not, kills the program.
        """
        loop = asyncio.get_event_loop()
        if len(self.rabbit._background_listeners) == 0:
            loop.call_later(0.1, self.rabbit.close)  # Hacky hack
            loop.stop()
        else:
            loop.call_later(5, self.check_listeners_status)

    def add_listener_group(self, group: ListenerGroup):
        for listener in group._add_listeners():
            self.rabbit.subscribe_listener(listener)
    
    @staticmethod
    def create_listener(queue: str, ack_time: AckTime = AckTime.Manual):
        def inner(func: Coroutine):
            if isinstance(func, Listener):
                # TODO
                raise Exception()
            if not asyncio.iscoroutinefunction(func):
                # TODO
                raise Exception()
            
            sig = inspect.signature(func)
            if len(sig.parameters) == 0 or \
                (len(sig.parameters) == 1 and list(sig.parameters.keys())[0] == "self"):
                # TODO
                raise Exception()
            
            return Listener(queue, func, ack_time)
        return inner
    
    def add_plugin(self, plugin_manager: FatCatPlugin):
        for plugin in plugin_manager._add_listeners():
            self._listeners.append(plugin)
    
    @staticmethod
    def create_plugin_listener(on: Literal["ready"]):
        def inner(func: Coroutine):
            if isinstance(func, PluginListener):
                # TODO
                raise Exception()
            if not asyncio.iscoroutinefunction(func):
                # TODO
                raise Exception()
            
            return PluginListener(func, on)
        return inner
