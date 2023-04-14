#  Copyright (c) 2023, Marlon Paulse
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
import asyncio
import uuid

from enum import Enum
from moonship.core import *
from typing import Awaitable, Callable, Union

__all__ = [
    "MessageBus",
    "MessageResult",
    "SharedCache",
    "SharedCacheBulkOp"
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


class MessageBus(abc.ABC):

    def __init__(self, config: Config) -> None:
        self.config = config
        self._recv_futures: dict[str, list[asyncio.Future]] = {}
        pass

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
