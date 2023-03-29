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

import aiohttp.web
import aiohttp_session
import asyncio
import logging
import os
import redis.asyncio as aioredis

from moonship.core import *
from moonship.core.ipc import *
from typing import Awaitable, Callable, Optional, Union

__all__ = [
    "RedisMessageBus",
    "RedisSessionStore",
    "RedisSharedCache"
]

STORAGE_KEY_PREFIX = "moonship."

redis: Optional[aioredis.Redis] = None
redis_ref_count = 0

logger = logging.getLogger(__name__)


async def init_redis(config: Config) -> aioredis.Redis:
    global redis, redis_ref_count
    if redis is None:
        url = config.get("moonship.redis.url")
        if isinstance(url, str):
            if len(url) > 1 and url.startswith("$"):
                try:
                    url = os.environ[url[1:]]
                except KeyError:
                    raise StartUpException(f"No {url[1:]} environment variable set")
        else:
            raise StartUpException("Redis URL not configured")
        options = {
            "decode_responses": True
        }
        if url.startswith("rediss://"):
            ssl_verify_cert = config.get("moonship.redis.ssl_verify_cert", default=True)
            options["ssl_cert_reqs"] = "required" if ssl_verify_cert else None
            options["ssl_check_hostname"] = ssl_verify_cert
        redis = await aioredis.from_url(url, **options)
    redis_ref_count += 1
    return redis


async def close_redis() -> None:
    global redis, redis_ref_count
    redis_ref_count -= 1
    if redis_ref_count == 0:
        await redis.close()
        await redis.connection_pool.disconnect()


# Hack to fix the "Redis.close was never awaited" error at shutdown
aioredis.Redis.__del__ = lambda *args: None


class RedisSharedCacheBulkOp(SharedCacheBulkOp):

    def __init__(self, pipeline: aioredis.client.Pipeline):
        self.pipeline = pipeline

    def set_add(self, storage_key: str, element: str) -> SharedCacheBulkOp:
        self.pipeline.sadd(f"{STORAGE_KEY_PREFIX}{storage_key}", element)
        return self

    def set_remove(self, storage_key: str, element: str) -> SharedCacheBulkOp:
        self.pipeline.srem(f"{STORAGE_KEY_PREFIX}{storage_key}", element)
        return self

    def map_put(self, storage_key: str, entries: dict[str, str]) -> SharedCacheBulkOp:
        self.pipeline.hset(f"{STORAGE_KEY_PREFIX}{storage_key}", mapping=entries)
        return self

    def delete(self, storage_key: str) -> SharedCacheBulkOp:
        self.pipeline.delete(f"{STORAGE_KEY_PREFIX}{storage_key}")
        return self

    def expire(self, storage_key: str, time_msec: int) -> SharedCacheBulkOp:
        self.pipeline.pexpire(f"{STORAGE_KEY_PREFIX}{storage_key}", time_msec)
        return self

    async def execute(self) -> None:
        await self.pipeline.execute()


class RedisSharedCache(SharedCache):

    def __init__(self, config: Config) -> None:
        super().__init__(config)

    async def open(self) -> None:
        await init_redis(self.config)

    async def close(self) -> None:
        await close_redis()

    async def set_add(self, storage_key: str, element: str) -> None:
        await redis.sadd(f"{STORAGE_KEY_PREFIX}{storage_key}", element)

    async def set_remove(self, storage_key: str, element: str) -> None:
        await redis.srem(f"{STORAGE_KEY_PREFIX}{storage_key}", element)

    async def set_get_elements(self, storage_key: str) -> set[str]:
        return await redis.smembers(f"{STORAGE_KEY_PREFIX}{storage_key}")

    async def map_put(self, storage_key: str, entries: dict[str, str], append=True) -> None:
        storage_key = f"{STORAGE_KEY_PREFIX}{storage_key}"
        if not append:
            await self.start_bulk() \
                .delete(storage_key) \
                .map_put(storage_key, entries) \
                .execute()
        else:
            await redis.hset(storage_key, mapping=entries)

    async def map_get(self, storage_key: str, key: str) -> str:
        return await redis.hget(f"{STORAGE_KEY_PREFIX}{storage_key}", key)

    async def map_get_entries(self, storage_key: str) -> dict[str, str]:
        return await redis.hgetall(f"{STORAGE_KEY_PREFIX}{storage_key}")

    async def delete(self, storage_key: str) -> None:
        await redis.delete(f"{STORAGE_KEY_PREFIX}{storage_key}")

    async def expire(self, storage_key: str, time_msec: int) -> None:
        await redis.pexpire(f"{STORAGE_KEY_PREFIX}{storage_key}", time_msec)

    def start_bulk(self, transaction=True) -> SharedCacheBulkOp:
        return RedisSharedCacheBulkOp(redis.pipeline(transaction))


class RedisMessageBus(MessageBus):

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.channel_handlers = {}
        self.pubsub: Optional[aioredis.client.PubSub] = None
        self.listen_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await init_redis(self.config)
        self.pubsub = redis.pubsub(ignore_subscribe_messages=True)
        self.listen_task = asyncio.create_task(self._listen())

    async def close(self) -> None:
        await self.pubsub.close()
        if self.listen_task is not None:
            self.listen_task.cancel()
        await close_redis()

    async def subscribe(self, channel_name: str, handler: Callable[[any, str], Awaitable[None]]) -> None:
        self.channel_handlers[channel_name] = handler
        await self.pubsub.subscribe(channel_name)

    async def unsubscribe(self, channel_name: str) -> None:
        await self.pubsub.unsubscribe(channel_name)
        del self.channel_handlers[channel_name]

    async def publish(self, msg: any, channel_name: str) -> None:
        await redis.publish(channel_name, msg)
        logger.debug(f"Message published: [{channel_name}] {msg}")

    async def _listen(self) -> None:
        try:
            async for msg in self.pubsub.listen():
                logger.debug(f"Message received: [{msg['channel']}] {msg['data']}")
                handler = self.channel_handlers.get(msg["channel"])
                if handler is not None:
                    await handler(msg["data"], msg["channel"])
        except asyncio.CancelledError:
            pass


class RedisSessionStore(aiohttp_session.AbstractStorage):

    def __init__(self, config: Config):
        super().__init__(
            cookie_name="__Host-session_token",
            domain=None,
            max_age=None,
            httponly=True,
            path="/",
            secure=True)
        self.cookie_params["samesite"] = "Strict"
        self.shared_cache = RedisSharedCache(config)

    async def open(self) -> None:
        await self.shared_cache.open()

    async def close(self) -> None:
        await self.shared_cache.close()

    def load_cookie(self, request: aiohttp.web.Request) -> str:
        auth_header = request.headers.get("Authorization")
        if auth_header is not None:
            s = auth_header.split()
            if len(s) == 2 and s[0] == "Bearer":
                return s[1]
        return super().load_cookie(request)

    async def load_session(self, request: aiohttp.web.Request):
        session_id = self.load_cookie(request)
        if session_id is None:
            return aiohttp_session.Session(None, data=None, new=True, max_age=self.max_age)
        else:
            storage_key = self._storage_key(session_id)
            session_save_data = await self.shared_cache.map_get_entries(storage_key)
            if session_save_data is None or len(session_save_data) == 0:
                return aiohttp_session.Session(None, data=None, new=True, max_age=self.max_age)
            if self.max_age is not None:
                await self.shared_cache.expire(storage_key, self.max_age * 1000)
            session_data = {
                "created": int(session_save_data["created"]),
                "session": session_save_data
            }
            del session_save_data["created"]
            return aiohttp_session.Session(session_id, data=session_data, new=False, max_age=self.max_age)

    async def save_session(
            self,
            request: aiohttp.web.Request,
            response: aiohttp.web.Response,
            session: aiohttp_session.Session):
        if session.empty:
            self.save_cookie(response, None)
            await self.shared_cache.delete(f"session.{session.identity}")
        else:
            self.save_cookie(response, session.identity, max_age=session.max_age)
            session_data = self._get_session_data(session)
            session_save_data = session_data["session"]
            session_save_data["created"] = session_data["created"]
            storage_key = self._storage_key(session)
            b = self.shared_cache.start_bulk() \
                .delete(storage_key) \
                .map_put(storage_key, session_save_data)
            if session.max_age is not None:
                b.expire(storage_key, session.max_age * 1000)
            await b.execute()

    def _storage_key(self, session: Union[str, aiohttp_session.Session]) -> str:
        session_id = session if isinstance(session, str) else session.identity
        return f"session.{session_id}"
