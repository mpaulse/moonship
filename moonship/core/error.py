#  Copyright (c) 2025 Marlon Paulse
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

import aiohttp.typedefs
import asyncio
import enum
import functools
import inspect

from moonship.core.data import utc_timestamp_now_msec
from typing import Any, Callable, ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

__all__ = [
    "CircuitBreaker",
    "CircuitBreakerException",
    "ConfigException",
    "HttpResponseException",
    "MarketErrorCode",
    "MarketException",
    "ShutdownException",
    "StartUpException"
]


class HttpResponseException(aiohttp.ClientResponseError):

    def __init__(
            self,
            request_info: aiohttp.RequestInfo,
            history: tuple[aiohttp.ClientResponse, ...],
            status: int | None = None,
            reason: str = "",
            headers: aiohttp.typedefs.LooseHeaders | None = None,
            body: Any = None):
        super().__init__(
            request_info,
            history,
            status=status,
            message=str({"reason": reason, "body": body}),
            headers=headers)
        self.body = body


class MarketErrorCode(enum.Enum):
    UNKNOWN = 0
    INSUFFICIENT_FUNDS = 2001
    POST_ONLY_ORDER_CANCELLED = 2002
    NO_SUCH_ORDER = 2003
    INTERNAL_SERVER_ERROR = 2004
    CIRCUIT_BREAKER_TRIPPED = 3001


class MarketException(Exception):

    def __init__(self, message: str, market_name: str, error_code=MarketErrorCode.UNKNOWN):
        super().__init__(f"{market_name}: {message}")
        self.market_name = market_name
        self.error_code = error_code


class StartUpException(Exception):
    pass


class ConfigException(StartUpException):
    pass


class ShutdownException(Exception):
    pass


class CircuitBreakerException(Exception):
    pass


class CircuitBreakerState(enum.Enum):
    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


class CircuitBreaker:

    def __init__(
        self,
        exceptions: list[type[BaseException]] | None = None,
        market_error_codes: list[MarketErrorCode] | None = None,
        http_status_codes: list[int] | None = None,
        error_threshold = 3,
        backoff_period_msec = 30_000,
        half_open_call_limit = 1,
        synchronize_calls = False
    ) -> None:
        self._exceptions = tuple(exceptions) if isinstance(exceptions, list) else tuple()
        self._market_error_codes = set(market_error_codes if market_error_codes is not None else [])
        self._http_status_codes = set(http_status_codes if http_status_codes is not None else [])
        self._error_threshold = error_threshold
        self._backoff_period_msec = backoff_period_msec
        self._half_open_call_limit = half_open_call_limit
        self._synchronize_calls = synchronize_calls
        self._open_start_time_msec: int | None = None
        self._error_count = 0
        self._half_open_call_count = 0
        self._call_lock = asyncio.Lock()
        self._state = CircuitBreakerState.CLOSED

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("CircuitBreaker can only be used on async functions")
        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            if self._synchronize_calls:
                async with self._call_lock:
                    return await self._invoke(func, args, kwargs)
            else:
                return await self._invoke(func, args, kwargs)
        return wrapper

    async def _invoke(self, func: Callable[P, R], args: tuple[Any, ...], kwargs: dict[str, Any]) -> R:
        if self._state == CircuitBreakerState.OPEN:
            open_time_msec = utc_timestamp_now_msec() - self._open_start_time_msec
            if open_time_msec > self._backoff_period_msec:
                self._state = CircuitBreakerState.HALF_OPEN
                self._half_open_call_count = 1
            else:
                raise CircuitBreakerException(
                    f"Circuit breaker tripped. Recovery in {self._backoff_period_msec - open_time_msec} msec")
        elif self._state == CircuitBreakerState.HALF_OPEN:
            if self._half_open_call_count >= self._half_open_call_limit:
                raise CircuitBreakerException("Circuit breaker tripped, but half-open")
            self._half_open_call_count += 1
        try:
            result = await func(*args, **kwargs)
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._state = CircuitBreakerState.CLOSED
                self._half_open_call_count = 0
                self._error_count = 0
            return result
        except BaseException as e:
            if isinstance(e, self._exceptions) \
                    or (isinstance(e, HttpResponseException) and e.status in self._http_status_codes) \
                    or (isinstance(e, MarketException) and e.error_code in self._market_error_codes):
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.OPEN
                    self._open_start_time_msec = utc_timestamp_now_msec()
                elif self._state == CircuitBreakerState.CLOSED:
                    self._error_count += 1
                    if self._error_count >= self._error_threshold:
                        self._state = CircuitBreakerState.OPEN
                        self._open_start_time_msec = utc_timestamp_now_msec()
            raise
