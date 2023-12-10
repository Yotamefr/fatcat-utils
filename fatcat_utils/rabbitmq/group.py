import asyncio
import inspect
from typing import Any, Coroutine, Generator
from .enums import AckTime


class ListenerGroup:
    def _add_listeners(self) -> Generator["Listener", None, None]:
        for _, method in inspect.getmembers(self, predicate=lambda x: isinstance(x, Listener)):
            yield method
            method._instance = self


class Listener:
    def __init__(self, queue_name: str, func: Coroutine, ack_time: AckTime = AckTime.Manual, instance: ListenerGroup = None):
        if instance is not None and not isinstance(instance, ListenerGroup):
            # TODO
            raise Exception()
        if not asyncio.iscoroutinefunction(func):
            # TODO
            raise Exception()
        if not isinstance(queue_name, str):
            # TODO
            raise Exception()
        if not isinstance(ack_time, AckTime):
            # TODO
            raise Exception()
        
        self._queue_name = queue_name,
        self._ack_time = ack_time
        self._func = func
        self._instance = instance
    
    @property
    def group(self):
        return self._instance.__class__
    
    @property
    def group_instance(self):
        return self._instance
    
    @property
    def queue_name(self):
        return self._queue_name
    
    @property
    def ack_time(self):
        return self._ack_time
    
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._instance is not None:
            return await self._func(self._instance, *args, **kwargs)
        else:
            return await self._func(*args, **kwargs)
