import asyncio
import json
from typing import Callable, Dict, List, Tuple
import aio_pika
from fatcat_utils.logger import logger
from .base_rabbit import BaseRabbitMQ
from .config import RABBIT_CONFIG
from .exceptions import AlreadyExists
from .group import Listener
from .enums import MessageStatus, AckTime


class Consumer(BaseRabbitMQ):
    _listeners: Dict[str, List[Tuple[Callable, AckTime, list, dict]]]
    _background_listeners: Dict[str, asyncio.Task]

    def __init__(self):
        super().__init__()
        self._listeners = dict()
        self._background_listeners = dict()
    
    def subscribe_listener(self, listener: Listener):
        logger.debug("Adding a queue to the listeners list")
        if not isinstance(listener, Listener):
            # TODO
            raise Exception()
        
        self._listeners[listener.queue_name] = self._listeners.get(listener.queue_name, [listener])
        if self._listeners[listener.queue_name] == [listener]:
            if self._server is not None:
                self._spawn_new_listener(listener.queue_name)

    def subscribe(self, queue_name: str, *, ack: AckTime = AckTime.Manual) -> Callable:
        """Adds a new listener to the queue

        :param queue_name: The queue to listen to
        :type queue_name: str
        :raises TypeError: Raises if `queue_name` is not a string
        :return: The function we decorated
        :rtype: Callable
        """
        logger.debug(f"Subscribing to queue {queue_name}")
        if not isinstance(queue_name, str):
            logger.error("Got a queue name that is not a string")
            raise TypeError("`queue_name` object should be a string.")

        def decorator(func: Callable):
            listener = Listener(queue_name, func, ack)
            self.subscribe_listener(listener)
            return listener

        return decorator

    async def connect(self):
        """Connects to the RabbitMQ server and spawns all listeners
        """
        await super().connect()
        self._spwan_all_listeners()

    async def close(self):
        """Closes connection to the RabbitMQ server and kills all listeners
        """
        await super().close()
        self._kill_all_listeners()

    def _kill_all_listeners(self):
        """Kills all the listeners. Should only be used when the code exited!
        """
        logger.debug("Killing all listeners")
        for background_listener in self._background_listeners.values():
            logger.info(f"Killing {background_listener.get_name()}")
            background_listener.cancel()

    def _spwan_all_listeners(self):
        """Spawns all the listeners
        """
        logger.debug("Spawns all listeners")

        to_delete = list()
        for queue_name in self._listeners:
            if len(self._listeners[queue_name]) == 0:
                logger.info(
                    f"There are no listeners for {queue_name}. Adding key to deletion list.")
                to_delete.append(queue_name)
                continue
            self._spawn_new_listener(queue_name)

        for queue_name in to_delete:
            logger.info(f"Deleting {queue_name} from the listeners dictionary")
            del self._listeners[queue_name]

    def _spawn_new_listener(self, queue_name: str):
        """Spawns a new listener

        :param queue_name: The queue we wanna listen to
        :type queue_name: str
        :raises TypeError: Raises if `queue_name` is not a string
        :raises AlreadyExists: Raises if there is already a listener
        """
        logger.debug(f"Spawns a new listener for {queue_name}")

        if not isinstance(queue_name, str):
            logger.error(f"Queue name {queue_name} should be a string")
            raise TypeError("`queue_name` object should be a string.")

        if self._background_listeners.get(queue_name) is not None:
            logger.error(f"Listener for queue {queue_name} already exists")
            raise AlreadyExists(
                f"Listener for queue `{queue_name}` already exists.")

        logger.debug("Creating a new async task for the listener")
        self._background_listeners[queue_name] = asyncio.create_task(
            self._listener_worker(queue_name))
        logger.debug("Changing the task name")
        self._background_listeners[queue_name].set_name(
            f"rabbitmq-listener-{queue_name}")
        logger.debug("Adds a done callback")
        self._background_listeners[queue_name].add_done_callback(
            self._listener_closed)

    async def _listener_worker(self, queue_name: str):
        """Listens to the queue

        :param queue_name: The name of the queue we wanna listen to
        :type queue_name: str
        :raises TypeError: Raises if `queue_name` is not a string
        """
        logger.debug(f"Spawned a new listener worker for {queue_name}")
        if not isinstance(queue_name, str):
            logger.error("Queue name should be a string")
            raise TypeError("`queue_name` object should be a string.")

        channel = await self._server.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue(queue_name, durable=True)

        while True:
            try:
                logger.debug("Attempting to get a new message")
                message = await queue.get()
            except aio_pika.exceptions.QueueEmpty:
                logger.debug("Qeueue is currently empty. Trying again.")
                continue

            if len(self._listeners[queue_name]) == 0:
                logger.info("No active listeners. Killing listener worker.")
                break

            for listener in self._listeners[queue_name]:
                if not isinstance(listener, Listener):
                    logger.info(
                        "I don't know how I got here. What is this handler? I'll keep listening though.")
                    continue

                if listener.ack_time == AckTime.Start:
                    logger.info(
                        "I was asked to ack in advanced (for some reason. Not judging though). Acknowledging the message")
                    await message.ack()
                try:
                    logger.info("Calling the listener handler")
                    status = await listener(message)
                except Exception as e:
                    logger.error(
                        "Failed to execute handler. Sending it to the failed queue.", exc_info=e)
                    body = json.loads(message.body)
                    body["FATCAT_QUEUE"] = message.routing_key
                    await self.send_to_queue(body, RABBIT_CONFIG["failed_queue"])
                    try:
                        logger.debug("Trying to reject the message")
                        await message.reject()
                    except:
                        logger.debug(
                            "Failed to reject the message. Was probably acknowledged earlier")
                    else:
                        status = MessageStatus.Rejected
                else:
                    if status == MessageStatus.Rejected:
                        logger.info(
                            "Message was rejected. Sending it to the failed queue")
                        body = json.loads(message.body)
                        body["FATCAT_QUEUE"] = message.routing_key
                        await self.send_to_queue(body, RABBIT_CONFIG["failed_queue"])
                        continue
                    if listener.ack_time == AckTime.End:
                        logger.info(
                            "I was asked to ack after the handler finished. Acknowledging the message.")
                        await message.ack()

    def _listener_closed(self, task: asyncio.Task):
        """Triggered when the listened closes connection

        :param task: The listener task
        :type task: asyncio.Task
        """
        # Listener name
        logger.debug("A listener got closed")
        key = task.get_name()[len("rabbitmq-listener-"):]
        logger.info(f"Listener for {key} was closed")

        exception = task.exception()

        # If it's just a timeout we don't want to leave the listener
        # Timeouts only occurred to me during testing (due to breakpoints)
        # But I left it here in case it happens to you on production :)
        if isinstance(exception, asyncio.exceptions.TimeoutError):
            logger.info(
                f"Listener {key} was closed due to a timeout. Spawning the listener again.")
            del self._background_listeners[key]
            self._spawn_new_listener(key)
            return

        if exception is not None:
            logger.error(
                f"Listener {key} was closed due to an unexpected error. Killing listener", exc_info=exception)
        else:
            logger.info(f"Listener {key} was probably closed deliberately.")

        del self._background_listeners[key]
