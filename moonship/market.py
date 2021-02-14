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
import asyncio
import sortedcontainers

from .config import *
from .data import *
from .error import *
from dataclasses import dataclass, field
from typing import Union

__all__ = [
    "Market",
    "MarketClient",
    "MarketFeed",
    "MarketEvent",
    "MarketFeedSubscriber",
    "MarketStatusEvent",
    "OrderBookEntry",
    "OrderBookItemAddedEvent",
    "OrderBookItemRemovedEvent",
    "OrderBookInitEvent",
    "TickerEvent",
    "TradeEvent",
]


class MarketClient(abc.ABC):

    @abc.abstractmethod
    def __init__(self, market_name: str, app_config: Config):
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
class OrderBookItemAddedEvent(MarketEvent):
    order: LimitOrder


@dataclass()
class OrderBookItemRemovedEvent(MarketEvent):
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


class MarketFeedSubscriber:

    async def on_ticker(self, event: TickerEvent) -> None:
        pass

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        pass

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        pass

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        pass

    async def on_trade(self, event: TradeEvent) -> None:
        pass

    async def on_market_status_update(self, event: MarketStatusEvent) -> None:
        pass


class MarketFeed(abc.ABC):
    subscribers: list[MarketFeedSubscriber] = []

    @abc.abstractmethod
    def __init__(self, symbol: str, market_name: str, app_config: Config):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass

    def subscribe(self, subscriber) -> None:
        self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber) -> None:
        self.subscribers.remove(subscriber)

    def raise_event(self, event: MarketEvent) -> None:
        for sub in self.subscribers:
            func = None
            if isinstance(event, OrderBookInitEvent):
                func = sub.on_order_book_init(event)
            elif isinstance(event, OrderBookItemAddedEvent):
                func = sub.on_order_book_item_added(event)
            elif isinstance(event, OrderBookItemRemovedEvent):
                func = sub.on_order_book_item_removed(event)
            elif isinstance(event, TradeEvent):
                func = sub.on_trade(event)
            elif isinstance(event, MarketStatusEvent):
                func = sub.on_market_status_update(event)
            if func is not None:
                asyncio.create_task(func)


class OrderBookEntry:
    price: Amount
    volume: Amount
    orders: dict[str, LimitOrder]


class Market:

    def __init__(self, name: str, symbol: str, client: MarketClient, feed: MarketFeed):
        self._name = name
        self._symbol = symbol
        self._client = client
        self._feed = feed
        self._bids = sortedcontainers.SortedSet[OrderBookEntry]()
        self._asks = sortedcontainers.SortedSet[OrderBookEntry]()
        self._status = MarketStatus.ACTIVE
        self.subscribe_to_feed(MarketManager(self))

    @property
    def name(self) -> str:
        return self._name

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def status(self) -> MarketStatus:
        return self._status

    async def get_tickers(self) -> list[Ticker]:
        if self._status == MarketStatus.DISABLED:
            raise MarketException(f"{self._name} market closed")
        return await self._client.get_tickers()

    async def get_ticker(self, symbol: str) -> Ticker:
        if self._status == MarketStatus.DISABLED:
            raise MarketException(f"{self._name} market closed")
        return await self._client.get_ticker(symbol)

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        if self._status == MarketStatus.DISABLED:
            raise MarketException(f"{self._name} market closed")
        return await self._client.place_order(order)

    async def get_order(self, order_id: str) -> FullOrderDetails:
        if self._status == MarketStatus.DISABLED:
            raise MarketException(f"{self._name} market closed")
        return await self._client.get_order(order_id)

    async def cancel_order(self, order_id: str) -> bool:
        if self._status == MarketStatus.DISABLED:
            raise MarketException(f"{self._name} market closed")
        return await self._client.cancel_order(order_id)

    def subscribe_to_feed(self, subscriber) -> None:
        self._feed.subscribe(subscriber)

    def unsubscribe_from_feed(self, subscriber) -> None:
        self._feed.unsubscribe(subscriber)


class MarketManager(MarketFeedSubscriber):

    def __init__(self, market: Market) -> None:
        self.market = market

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        pass

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        pass

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        pass

    async def on_market_status_update(self, event: MarketStatusEvent) -> None:
        self.market._status = event.status
