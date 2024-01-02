import inspect
import asyncio
from typing import Generator, Literal, Any, Coroutine


class FatCatPlugin:
    def _add_listeners(self) -> Generator["PluginListener", None, None]:
        for _, method in inspect.getmembers(self, predicate=lambda x: isinstance(x, PluginListener)):
            yield method
            method._instance = self


class PluginListener:
    def __init__(self, func: Coroutine, on: Literal["ready"], instance: FatCatPlugin = None):
        if instance is not None and not isinstance(instance, FatCatPlugin):
            # TODO
            raise Exception()
        if not asyncio.iscoroutinefunction(func):
            # TODO
            raise Exception()
        if not isinstance(on, str) and on not in ["ready"]:
            # TODO
            raise Exception()
        
        self._on = on
        self._func = func
        self._instance = instance
    
    @property
    def group(self):
        return self._instance.__class__
    
    @property
    def group_instance(self):
        return self._instance
    
    @group_instance.setter
    def group_instance(self, group_instance: FatCatPlugin):
        if not isinstance(isinstance, FatCatPlugin):
            # TODO
            raise Exception()
        self._instance = group_instance
    
    @property
    def on(self):
        return self.on
    
    @on.setter
    def on(self, on: Literal["ready"]):
        if not isinstance(on, str) and on not in ["ready"]:
            # TODO
            raise Exception()
        self._on = on
    
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._instance is not None:
            return await self._func(self._instance, *args, **kwargs)
        else:
            return await self._func(*args, **kwargs)