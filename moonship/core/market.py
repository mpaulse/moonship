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
import collections
import inspect
import logging
import sortedcontainers

from dataclasses import dataclass, field
from datetime import timezone
from moonship.core import *
from typing import Iterator, Optional, Union

__all__ = [
    "Market",
    "MarketClient",
    "MarketEvent",
    "MarketSubscriber",
    "MarketStatusEvent",
    "OrderBookEntriesView",
    "OrderBookEntry",
    "OrderBookItemAddedEvent",
    "OrderBookItemRemovedEvent",
    "OrderBookInitEvent",
    "OrderStatusUpdateEvent",
    "TickerEvent",
    "TradeEvent",
]


@dataclass
class MarketEvent:
    timestamp: Timestamp = Timestamp.now(tz=timezone.utc)
    market_name: str = None
    symbol: str = None


@dataclass
class TickerEvent(MarketEvent):
    ticker: Ticker = None


@dataclass
class OrderBookInitEvent(MarketEvent):
    orders: list[LimitOrder] = field(default_factory=list)


@dataclass
class OrderBookItemAddedEvent(MarketEvent):
    order: LimitOrder = None


@dataclass
class OrderBookItemRemovedEvent(MarketEvent):
    order_id: str = None


@dataclass
class TradeEvent(MarketEvent):
    trade: Trade = None
    maker_order_id: str = None
    taker_order_id: str = None


@dataclass
class MarketStatusEvent(MarketEvent):
    status: MarketStatus = MarketStatus.OPEN


@dataclass
class OrderStatusUpdateEvent(MarketEvent):
    order: FullOrderDetails = None


class MarketSubscriber:

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

    async def on_order_status_update(self, event: OrderStatusUpdateEvent) -> None:
        pass


class MarketClient(abc.ABC):

    def __init__(self, market_name: str, app_config: Config) -> None:
        self.market: Optional["Market"] = None
        pass

    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def close(self):
        pass

    @abc.abstractmethod
    async def get_market_info(self, use_cached=True) -> MarketInfo:
        pass

    @abc.abstractmethod
    async def get_ticker(self) -> Ticker:
        pass

    @abc.abstractmethod
    async def get_recent_trades(self, limit: int) -> list[Trade]:
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


class OrderBookEntry:

    def __init__(self, order: LimitOrder) -> None:
        self._price = order.price
        self._orders = {order.id: order}

    def __contains__(self, order_id: str):
        return self._orders.__contains__(order_id)

    @property
    def price(self) -> Amount:
        return self._price

    @property
    def quantity(self) -> Amount:
        quantity = Amount(0)
        for order in self._orders.values():
            quantity += order.quantity
        return quantity


class OrderBook:

    def __init__(self):
        self.bids = sortedcontainers.SortedDict[Amount, OrderBookEntry]()
        self.asks = sortedcontainers.SortedDict[Amount, OrderBookEntry]()
        self.order_entry_index: dict[str, OrderBookEntry] = {}

    def add_order(self, order: LimitOrder) -> None:
        entries = self._get_entries_list(order)
        entry: OrderBookEntry = entries.get(order.price)
        if entry is None:
            entry = OrderBookEntry(order)
            entries[order.price] = entry
        else:
            entry._orders[order.id] = order
        self.order_entry_index[order.id] = entry

    def remove_order(self, order_id: str) -> None:
        entry = self.order_entry_index.get(order_id)
        if entry is not None:
            order = entry._orders.get(order_id)
            if order is not None:
                del entry._orders[order_id]
                if entry.quantity == 0:
                    entries = self._get_entries_list(order)
                    del entries[entry.price]
            del self.order_entry_index[order_id]

    def clear(self) -> None:
        self.order_entry_index.clear()
        self.bids.clear()
        self.asks.clear()

    def _get_entries_list(self, order) -> sortedcontainers.SortedDict[Amount, OrderBookEntry]:
        return self.bids if order.action == OrderAction.BUY else self.asks


class OrderBookEntriesView(collections.Mapping):

    def __init__(self, entries: sortedcontainers.SortedDict[Amount, OrderBookEntry]) -> None:
        self._entries = entries

    def __getitem__(self, amount: Amount) -> OrderBookEntry:
        return self._entries.get(amount)

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[Amount]:
        return iter(self._entries)

    def keys(self) -> sortedcontainers.SortedKeysView:
        return self._entries.keys()

    def values(self) -> sortedcontainers.SortedValuesView:
        return self._entries.values()

    def items(self) -> sortedcontainers.SortedItemsView:
        return self._entries.items()

    def index(self, price: Amount, start: int = None, stop: int = None) -> int:
        return self._entries.index(price, start, stop)


class Market:

    def __init__(self, name: str, symbol: str, client: MarketClient) -> None:
        self._name = name
        self._symbol = symbol
        self._client = client
        self._client.market = self
        self._base_asset = symbol[0:3] if len(symbol) == 6 else symbol
        self._base_asset_precision = 0
        self._base_asset_min_quantity = Amount(1)
        self._quote_asset = symbol[3:] if len(symbol) == 6 else None
        self._quote_asset_precision = 0
        self._current_price = Amount(0)
        self._status = MarketStatus.CLOSED
        self._order_book = OrderBook()
        self._recent_trades = sortedcontainers.SortedList(key=lambda t: t.timestamp)
        self._subscribers: list[MarketSubscriber] = []
        self._pending_order_ids: set[str] = set()
        self._logger = logging.getLogger(f"moonship.market.{name}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def base_asset(self) -> str:
        return self._base_asset

    @property
    def base_asset_precision(self) -> int:
        return self._base_asset_precision

    @property
    def base_asset_min_quantity(self) -> Amount:
        return self._base_asset_min_quantity

    @property
    def quote_asset(self) -> str:
        return self._quote_asset

    @property
    def quote_asset_precision(self) -> int:
        return self._quote_asset_precision

    @property
    def status(self) -> MarketStatus:
        return self._status

    @property
    def current_price(self) -> Amount:
        return self._current_price

    @property
    def bid_price(self) -> Amount:
        return self._order_book.bids.peekitem(-1)[0] if len(self._order_book.bids) > 0 else Amount(0)

    @property
    def ask_price(self) -> Amount:
        return self._order_book.asks.peekitem(0)[0] if len(self._order_book.asks) > 0 else Amount(0)

    @property
    def spread(self) -> Amount:
        return self.ask_price - self.bid_price

    @property
    def bids(self) -> OrderBookEntriesView:
        return OrderBookEntriesView(self._order_book.bids)

    @property
    def asks(self) -> OrderBookEntriesView:
        return OrderBookEntriesView(self._order_book.asks)

    @property
    def recent_trades(self) -> Iterator[Trade]:
        return reversed(self._recent_trades)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    async def get_ticker(self) -> Ticker:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_ticker()

    async def get_recent_trades(self, limit=100) -> list[Trade]:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_recent_trades(limit)

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        if self.logger.isEnabledFor(logging.INFO):
            log_msg = f"{order.action.name} {self.symbol} "
            if isinstance(order, MarketOrder):
                if order.is_base_quantity:
                    log_msg += f"{to_amount_str(order.quantity)} @ market"
                else:
                    log_msg += f"@ market for {to_amount_str(order.quantity)}"
            else:
                log_msg += f"{to_amount_str(order.quantity)} @ {to_amount_str(order.price)}"
            self._log(logging.INFO, log_msg)
        order_id = await self._client.place_order(order)
        self._log(logging.INFO, f"{order.action.name} order {order_id} {OrderStatus.PENDING.name}")
        self._pending_order_ids.add(order_id)
        return order_id

    async def get_order(self, order_id: str) -> FullOrderDetails:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_order(order_id)

    async def cancel_order(self, order_id: str) -> bool:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        self._log(logging.INFO, f"Cancel order {order_id}")
        success = await self._client.cancel_order(order_id)
        await self._update_order_status(order_id)
        return success

    async def _update_order_status(self, order_id: str) -> None:
        if order_id in self._pending_order_ids:
            order_details = await self.get_order(order_id)
            if order_details.status == OrderStatus.FILLED \
                    or order_details.status == OrderStatus.CANCELLED \
                    or order_details.status == OrderStatus.EXPIRED \
                    or order_details.status == OrderStatus.REJECTED:
                try:
                    self._pending_order_ids.remove(order_id)
                except KeyError:
                    pass
            self.raise_event(OrderStatusUpdateEvent(order=order_details))

    def subscribe(self, subscriber) -> None:
        self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber) -> None:
        self._subscribers.remove(subscriber)

    def raise_event(self, event: MarketEvent) -> None:
        event.market_name = self.name
        event.symbol = self.symbol
        if isinstance(event, OrderStatusUpdateEvent):
            if self.logger.isEnabledFor(logging.INFO):
                order = event.order
                msg = f"{order.action.name} order {order.id} {order.status.value}"
                if order.status == OrderStatus.PARTIALLY_FILLED:
                    msg += f" ({to_amount_str(order.quantity_filled)}"
                    if order.limit_quantity > 0:
                        msg += f" / {to_amount_str(order.limit_quantity)}"
                    msg += ")"
                self._log(logging.INFO, msg)
        for sub in self._subscribers:
            task = None
            if isinstance(event, OrderBookItemAddedEvent):
                task = sub.on_order_book_item_added(event)
            elif isinstance(event, OrderBookItemRemovedEvent):
                task = sub.on_order_book_item_removed(event)
            elif isinstance(event, TradeEvent):
                task = sub.on_trade(event)
            elif isinstance(event, TickerEvent):
                task = sub.on_ticker(event)
            elif isinstance(event, OrderBookInitEvent):
                task = sub.on_order_book_init(event)
            elif isinstance(event, MarketStatusEvent):
                task = sub.on_market_status_update(event)
            elif isinstance(event, OrderStatusUpdateEvent):
                task = sub.on_order_status_update(event)
            if task is not None:
                asyncio.create_task(task)

    def _log(self, level: int, message: str) -> None:
        if self.logger.isEnabledFor(level):
            frame_stack = inspect.stack()
            if len(frame_stack) > 2:
                caller = frame_stack[2].frame.f_locals.get("self")
                if caller is not None and hasattr(caller, "strategy_name"):
                    message = f"{getattr(caller, 'strategy_name')} - {message}"
            self.logger.log(level, message)
