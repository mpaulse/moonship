#  Copyright (c) 2024, Marlon Paulse
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#
#  1. Redistributions of source code must retain the above copyright notice, this
#     list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#  OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import abc
import ast
import asyncio
import io
import uuid

from enum import Enum
from moonship.core import *
from typing import Awaitable, Callable, Optional, Union

__all__ = [
    "MessageBus",
    "MessageResult",
    "SharedCache",
    "SharedCacheBulkOp",
    "SharedCacheDataAccessor"
]


class SharedCacheBulkOp(abc.ABC):

    @abc.abstractmethod
    def list_push_head(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def list_push_tail(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def list_pop_head(self, storage_key: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def list_pop_tail(self, storage_key: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def list_remove(self, storage_key: str, element: str, count: int = None) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def set_add(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def set_remove(self, storage_key: str, element: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def map_put(self, storage_key: str, entries: dict[str, str]) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def delete(self, storage_key: str) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    def expire(self, storage_key: str, time_msec: int) -> "SharedCacheBulkOp":
        pass

    @abc.abstractmethod
    async def execute(self) -> None:
        pass


class SharedCache(abc.ABC):

    def __init__(self, config: Config) -> None:
        self.config = config

    @abc.abstractmethod
    async def open(self) -> None:
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    @abc.abstractmethod
    async def list_push_head(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    async def list_push_tail(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    async def list_pop_head(self, storage_key: str) -> str:
        pass

    @abc.abstractmethod
    async def list_pop_tail(self, storage_key: str) -> str:
        pass

    @abc.abstractmethod
    async def list_remove(self, storage_key: str, element: str, count: int = None) -> None:
        pass

    @abc.abstractmethod
    async def list_get_head(self, storage_key: str) -> str:
        pass

    @abc.abstractmethod
    async def list_get_tail(self, storage_key: str) -> str:
        pass

    @abc.abstractmethod
    async def list_get_elements(self, storage_key: str) -> list[str]:
        pass

    @abc.abstractmethod
    async def set_add(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    async def set_remove(self, storage_key: str, element: str) -> None:
        pass

    @abc.abstractmethod
    async def set_get_elements(self, storage_key: str) -> set[str]:
        pass

    @abc.abstractmethod
    async def map_put(self, storage_key: str, entries: dict[str, str], append=True) -> None:
        pass

    @abc.abstractmethod
    async def map_get(self, storage_key: str, key: str) -> str:
        pass

    @abc.abstractmethod
    async def map_get_entries(self, storage_key: str) -> dict[str, str]:
        pass

    @abc.abstractmethod
    async def delete(self, storage_key: str) -> None:
        pass

    @abc.abstractmethod
    async def expire(self, storage_key: str, time_msec: int) -> None:
        pass

    @abc.abstractmethod
    def start_bulk(self, transaction=True) -> SharedCacheBulkOp:
        pass


class SharedCacheDataAccessor:

    def __init__(self, shared_cache: SharedCache) -> None:
        self.shared_cache = shared_cache

    async def open(self) -> None:
        await self.shared_cache.open()

    async def close(self) -> None:
        await self.shared_cache.close()

    async def add_strategy(self, name, config: Optional[Config], engine: str, engine_id: str) -> None:
        if engine_id is None:
            engine_id = await self.get_engine_id(engine)
        b = self.shared_cache.start_bulk()
        self._add_strategy(name, config, engine, engine_id, b)
        await b.execute()

    def _add_strategy(self, name, config: Optional[Config], engine: str, engine_id: str, op: SharedCacheBulkOp) -> None:
        config = self._to_cache_map_entries(config.dict) if config is not None else {}
        op \
            .set_add(f"moonship:{engine}:{engine_id}:strategies", name) \
            .map_put(f"moonship:{engine}:{engine_id}:strategy:{name}", {"active": "false"})  \
            .map_put(f"moonship:{engine}:{engine_id}:strategy:{name}:config", config)

    async def remove_strategy(self, name: str, engine: str, engine_id: str = None) -> None:
        if engine_id is None:
            engine_id = await self.get_engine_id(engine)
        b = self.shared_cache.start_bulk()
        self._remove_strategy(name, engine, engine_id, b)
        await b.execute()

    def _remove_strategy(self, name: str, engine: str, engine_id: str, op: SharedCacheBulkOp):
        op \
            .delete(f"moonship:{engine}:{engine_id}:strategy:{name}") \
            .delete(f"moonship:{engine}:{engine_id}:strategy:{name}:config")

    async def get_strategy(self, name: str, engine: str, engine_id: str = None) -> Optional[dict[str, any]]:
        if engine_id is None:
            engine_id = await self.get_engine_id(engine)
        key = f"moonship:{engine}:{engine_id}:strategy:{name}"
        strategy: dict[str, any] = self._from_cache_map_entries(await self.shared_cache.map_get_entries(key))
        if len(strategy) == 0:
            return None
        strategy["name"] = name
        strategy["engine"] = engine
        strategy["config"] = self._from_cache_map_entries(await self.shared_cache.map_get_entries(f"{key}:config"))
        return strategy

    async def get_strategies(self, criteria: dict[str, str] = None) -> list[dict[str, any]]:
        if criteria is None:
            criteria = {}
        strategies = []
        for engine in set(await self.shared_cache.list_get_elements("moonship:engines")):
            engine_id = await self.get_engine_id(engine)
            for name in await self.shared_cache.set_get_elements(f"moonship:{engine}:{engine_id}:strategies"):
                strategy = await self.get_strategy(name, engine, engine_id)
                if strategy is not None:
                    match = True
                    for param, value in criteria.items():
                        if value.lower() == "true":
                            value = "True"
                        elif value.lower() == "false":
                            value = "False"
                        if param not in strategy or str(strategy.get(param)) != value:
                            match = False
                            break
                    if match:
                        strategies.append(strategy)
        return strategies

    async def update_strategy(self, name: str, data: dict[str, any], engine: str, engine_id: str = None) -> None:
        if engine_id is None:
            engine_id = await self.get_engine_id(engine)
        await self.shared_cache.map_put(
            f"moonship:{engine}:{engine_id}:strategy:{name}",
            self._to_cache_map_entries(data))

    async def add_engine(self, name: str, id: str, strategies_config: dict[str, Optional[Config]]) -> None:
        await self.shared_cache.open()
        b = self.shared_cache.start_bulk() \
            .list_push_tail("moonship:engines", name) \
            .list_push_tail(f"moonship:{name}:ids", id)
        for strategy_name, strategy_config in strategies_config.items():
            self._add_strategy(strategy_name, strategy_config, name, id, b)
        await b.execute()

    async def remove_engine(self, name: str, id: str, strategies: list[str]) -> None:
        b = self.shared_cache.start_bulk() \
            .list_remove("moonship:engines", name, count=1) \
            .list_remove(f"moonship:{name}:ids", id) \
            .delete(f"moonship:{name}:{id}:strategies")
        for strategy_name in strategies:
            self._remove_strategy(strategy_name, name, id, b)
        await b.execute()

    async def get_engine_id(self, engine: str) -> str:
        return await self.shared_cache.list_get_tail(f"moonship:{engine}:ids")

    def _to_cache_map_entries(self, object: dict[str, any], result: dict[str, str] = None, key_prefix="") -> dict[str, str]:
        if result is None:
            result = {}
        for key, value in object.items():
            if isinstance(value, dict):
                self._to_cache_map_entries(value, result, f"{key}.")
            elif isinstance(value, list):
                s = io.StringIO()
                print(*value, sep=",", end="", file=s)
                result[f"{key_prefix}{key}"] = f"[ {s.getvalue()} ]"
            else:
                result[f"{key_prefix}{key}"] = str(value)
        return result

    def _from_cache_map_entries(self, entries: dict[str, str]) -> dict[str, any]:
        result = {}
        for key, value in entries.items():
            obj = result
            if "." in key:
                sub_keys = key.split(".")
                for i in range(0, len(sub_keys) - 1):
                    child = obj.get(sub_keys[i])
                    if child is None:
                        child = {}
                        obj[sub_keys[i]] = child
                    obj = child
                key = sub_keys[-1]
            if value.startswith("[ ") and value.endswith(" ]"):
                obj[key] = value[2:-2].split(",")
            else:
                try:
                    obj[key] = ast.literal_eval(value)
                except:
                    obj[key] = value
        return result


class MessageBus(abc.ABC):

    def __init__(self, config: Config) -> None:
        self.config = config
        self._recv_futures: dict[str, list[asyncio.Future]] = {}

    @abc.abstractmethod
    async def start(self) -> None:
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    @abc.abstractmethod
    async def subscribe(self, channel: str, handler: Callable[[dict, str], Awaitable[None]]) -> None:
        pass

    @abc.abstractmethod
    async def unsubscribe(self, channel: str, handler: Callable[[dict, str], Awaitable[None]] = None) -> None:
        pass

    @abc.abstractmethod
    async def publish(self, msg: dict, channel: str) -> None:
        pass

    async def publish_and_receive(
        self,
        msg: dict,
        send_channel: str,
        recv_channel: str,
        timeout_sec: int = 20,
        recv_count: int = 1
    ) -> Union[dict[str, any], list[dict[str, any]]]:
        await self.subscribe(recv_channel, self._receive_handler)
        if "id" not in msg:
            msg["id"] = uuid.uuid4().hex
        recv_msgs: list[dict[str, any]] = []
        recv_futures: list[asyncio.Future] = []
        for i in range(0, recv_count):
            recv_futures.append(asyncio.get_event_loop().create_future())
        self._recv_futures[msg["id"]] = recv_futures
        try:
            await self.publish(msg, send_channel)
            done, pending = await asyncio.wait(recv_futures, timeout=timeout_sec, return_when=asyncio.ALL_COMPLETED)
            if len(pending) > 0:
                for future in pending:
                    future.cancel()
                raise TimeoutError
            for future in done:
                recv_msgs.append(future.result())
        finally:
            del self._recv_futures[msg["id"]]
        return recv_msgs if len(recv_msgs) > 1 else recv_msgs[0]

    async def _receive_handler(self, msg: dict[str, any], channel: str) -> None:
        id = msg.get("id")
        if id is not None:
            recv_futures = self._recv_futures.get(id)
            if recv_futures is not None:
                for future in recv_futures:
                    if not future.done():
                        future.set_result(msg)


class MessageResult(Enum):
    SUCCESS = 0
    FAILED = 1
    MISSING_OR_INVALID_PARAMETER = 2
    UNSUPPORTED = 3
