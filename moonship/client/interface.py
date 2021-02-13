#  Copyright (c) 2021, Marlon Paulse
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

from ..data import *
from dataclasses import dataclass, field
from typing import Union

__all__ = [
    "MarketClient",
    "MarketFeedSubscriber",
    "MarketFeed",
    "MarketEvent",
    "TickerEvent",
    "OrderBookInitEvent",
    "OrderBookEntryAddedEvent",
    "OrderBookEntryRemovedEvent",
    "TradeEvent",
    "MarketStatusEvent",
    "MarketClientException"
]


class MarketClient(abc.ABC):

    @abc.abstractmethod
    def __init__(self, market_name: str, app_config: dict):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass

    @abc.abstractmethod
    async def get_tickers(self) -> list[Ticker]:
        pass

    @abc.abstractmethod
    async def get_ticker(self, symbol: str) -> Ticker:
        pass

    @abc.abstractmethod
    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        pass

    @abc.abstractmethod
    async def get_order(self, order_id: str) -> FullOrderDetails:
        pass

    @abc.abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        pass


@dataclass()
class MarketEvent:
    timestamp: Timestamp
    symbol: str


@dataclass()
class TickerEvent(MarketEvent):
    ticker: Ticker


@dataclass()
class OrderBookInitEvent(MarketEvent):
    status: MarketStatus
    orders: list[LimitOrder] = field(default_factory=list)


@dataclass()
class OrderBookEntryAddedEvent(MarketEvent):
    order: LimitOrder


@dataclass()
class OrderBookEntryRemovedEvent(MarketEvent):
    order_id: str


@dataclass()
class TradeEvent(MarketEvent):
    base_amount: Amount
    counter_amount: Amount
    maker_order_id: str
    taker_order_id: str


@dataclass()
class MarketStatusEvent(MarketEvent):
    status: MarketStatus


class MarketFeedSubscriber(abc.ABC):

    @abc.abstractmethod
    async def on_market_event(self, e: MarketEvent) -> None:
        pass


class MarketFeed(abc.ABC):
    subscribers: list[MarketFeedSubscriber] = []

    @abc.abstractmethod
    def __init__(self, symbol: str, market_name: str, app_config: dict):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass

    def subscribe(self, subscriber) -> None:
        self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber) -> None:
        self.subscribers.remove(subscriber)


class MarketClientException(Exception):
    pass
