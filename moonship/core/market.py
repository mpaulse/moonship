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

from __future__ import annotations

import abc
import asyncio
import inspect
import logging
import sortedcontainers

from dataclasses import dataclass, field
from datetime import timezone, timedelta
from moonship.core import *
from typing import Any, Callable, Iterator, Mapping

__all__ = [
    "CandleEvent",
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
class CandleEvent(MarketEvent):
    candle: Candle = None

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

    async def on_candle(self, event: CandleEvent) -> None:
        pass

    async def on_market_status_update(self, event: MarketStatusEvent) -> None:
        pass

    async def on_order_status_update(self, event: OrderStatusUpdateEvent) -> None:
        pass


class MarketClient(abc.ABC):

    def __init__(self, market_name: str, app_config: Config) -> None:
        self.market: Market | None = None
        self.logger: logging.Logger | None = None

    def _set_market(self, market: Market) -> None:
        self.market = market
        self.logger = self.market.logger.getChild("client")

    @abc.abstractmethod
    async def connect(self):
        pass

    @abc.abstractmethod
    async def close(self):
        pass

    @abc.abstractmethod
    async def get_market_info(self, use_cached: bool = True) -> MarketInfo:
        pass

    @abc.abstractmethod
    async def get_ticker(self) -> Ticker:
        pass

    @abc.abstractmethod
    async def get_recent_trades(self, limit: int) -> list[Trade]:
        pass

    @abc.abstractmethod
    async def get_trades(self, handler: Callable[[list[Trade]], None], from_time: Timestamp) -> None:
        pass

    @abc.abstractmethod
    async def get_candles(self, period: CandlePeriod, from_time: Timestamp = None) -> list[Candle]:
        pass

    @abc.abstractmethod
    async def place_order(self, order: MarketOrder | LimitOrder) -> str:
        pass

    @abc.abstractmethod
    async def get_order(self, order_id: str) -> FullOrderDetails:
        pass

    @abc.abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        pass

    @abc.abstractmethod
    async def get_open_orders(self) -> list[FullOrderDetails]:
        pass

    @abc.abstractmethod
    async def get_asset_balances(self) -> tuple[AssetBalance, AssetBalance]:
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

    def add(self, order: LimitOrder) -> None:
        entries = self._get_entries_list(order)
        entry: OrderBookEntry = entries.get(order.price)
        if entry is None:
            entry = OrderBookEntry(order)
            entries[order.price] = entry
        else:
            entry._orders[order.id] = order
        self.order_entry_index[order.id] = entry

    def remove(self, order_id: str, quantity: Amount | None = None) -> None:
        entry = self.order_entry_index.get(order_id)
        if entry is not None:
            order = entry._orders.get(order_id)
            if order is not None:
                if quantity is not None:
                    order.quantity -= quantity
                if quantity is None or order.quantity <= 0:
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


class OrderBookEntriesView(Mapping):

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

    def __init__(self, name: str, symbol: str, client: MarketClient, **kwargs: dict[str, Any]) -> None:
        self._name = name
        self._symbol = symbol
        self._client = client

        candle_period = kwargs.get("candle_period")
        try:
            self._default_candle_period = CandlePeriod(candle_period)
        except ValueError:
            raise ConfigException(f"Invalid candle period specified for {name} market: {candle_period}")

        self._base_asset = symbol[0:3] if len(symbol) == 6 else symbol
        self._base_asset_precision = 0
        self._base_asset_min_quantity = Amount(1)
        self._quote_asset = symbol[3:] if len(symbol) == 6 else None
        self._quote_asset_precision = 0
        self._current_price = Amount(0)
        self._status = MarketStatus.CLOSED
        self._order_book = OrderBook()
        self._recent_trades = sortedcontainers.SortedList(key=lambda t: t.timestamp)
        self._recent_candles = sortedcontainers.SortedList(key=lambda c: c.start_time)
        self._subscribers: list[MarketSubscriber] = []
        self._pending_orders: dict[str, FullOrderDetails] = {}
        self._logger = logging.getLogger(f"moonship.market.{name}")
        self._client._set_market(self)

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
    def recent_candles(self) -> Iterator[Candle]:
        return reversed(self._recent_candles)

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    async def get_ticker(self) -> Ticker:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_ticker()

    async def get_recent_trades(self, limit: int = 1000) -> list[Trade]:
        """Gets the most recent trades executed at the exchange.

        The results may be returned in any order (ascending, descending or unordered),
        depending on the exchange API and/or the MarketClient implementation. The caller
        is responsible for re-ordering the results, if required.

        The limit parameter is just a hint. The maximum limit imposed by the exchange API
        will apply if the given value is greater than the API's limit.
        """
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_recent_trades(limit)

    async def get_trades(
        self,
        handler: Callable[[list[Trade]], None],
        from_time: Timestamp = Timestamp.now(tz=timezone.utc) - timedelta(minutes=5)
    ) -> None:
        """Gets the trades executed at the exchange, starting from a particular timestamp
        up to the current time, and calls the given handler function to process the
        retrieved list, one chunk at a time.

        The results may be returned in any order (ascending, descending or unordered),
        depending on the exchange API and/or the MarketClient implementation. The handler
        function is responsible for re-ordering the results, if required.

        Duplicates may also be present in the list of results, depending on the MarketClient
        implementation (Trade objects with the same id). The handler function is expected
        to cater for this.
        """
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_trades(handler, from_time)

    async def get_candles(self, period: CandlePeriod = None, from_time: Timestamp = None) -> list[Candle]:
        """Gets candles for the specified period, starting from a particular timestamp
        up to either the current time or some maximum number limit imposed by the exchange
        API, whichever comes first.

        The results may be returned in any order (ascending, descending or unordered),
        depending on the exchange and/or the MarketClient implementation. The caller
        is responsible for re-ordering the results, if required.
        """
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        if period is None:
            period = self._default_candle_period
        return await self._client.get_candles(period, from_time)

    async def place_order(self, order: MarketOrder | LimitOrder) -> str:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        if isinstance(order, LimitOrder) and (order.post_only or order.time_in_force is None):
            order.time_in_force = TimeInForce.GOOD_TILL_CANCELLED
        if self.logger.isEnabledFor(logging.INFO):
            log_msg = f"{order.action.name} {self.symbol} "
            if isinstance(order, MarketOrder):
                if order.is_base_quantity:
                    log_msg += f"{to_amount_str(order.quantity)} @ market"
                else:
                    log_msg += f"@ market for {to_amount_str(order.quantity)}"
            else:
                log_msg += f"{to_amount_str(order.quantity)} @ {to_amount_str(order.price)}"
            if order.time_in_force is not None:
                log_msg += f" ({order.time_in_force.name.replace("_", " ")})"
            self._log(logging.INFO, log_msg)
        order_id = await self._client.place_order(order)
        self._log(logging.INFO, f"{order.action.name} order {order_id} {OrderStatus.PENDING.name}")
        self._pending_orders[order_id] = \
            FullOrderDetails(
                id=order.id,
                symbol=self.symbol,
                action=order.action,
                quantity=order.quantity if isinstance(order, LimitOrder) or order.is_base_quantity else Amount(0),
                quote_quantity=order.quantity if isinstance(order, MarketOrder) and not order.is_base_quantity else Amount(0),
                limit_price=order.price if isinstance(order, LimitOrder) else Amount(0),
                time_in_force=order.time_in_force)
        # In case the order executed too fast and the TradeEvent occurred before the
        # order was marked as pending (e.g. for market and non-post-only limit orders)
        asyncio.create_task(self._check_placed_order_status(order_id))
        return order_id

    async def _check_placed_order_status(self, order_id: str) -> None:
        if order_id in self._pending_orders:
            placed_order = await self.get_order(order_id)
            if order_id in self._pending_orders:
                self._remove_completed_pending_order(placed_order)
                self.raise_event(OrderStatusUpdateEvent(order=placed_order))

    async def get_order(self, order_id: str) -> FullOrderDetails:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        order = await self._client.get_order(order_id)
        if order.status == OrderStatus.CANCELLED:
            if order.quantity_filled > 0 or order.quote_quantity_filled > 0:
                if order.quantity_filled == order.quantity or order.quote_quantity_filled == order.quote_quantity:
                    order.status = OrderStatus.FILLED
                else:
                    order.status = OrderStatus.CANCELLED_AND_PARTIALLY_FILLED
        return order

    async def cancel_order(self, order_id: str) -> OrderStatus:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        self._log(logging.INFO, f"Cancel order {order_id}")
        await self._client.cancel_order(order_id)
        order = await self._handle_pending_order_update(order_id)
        if order is not None:
            return order.status
        order = await self.get_order(order_id)
        if order.status != OrderStatus.PENDING:
            self.raise_event(OrderStatusUpdateEvent(order=order))
        return order.status

    async def _handle_pending_order_update(self, order_id: str) -> FullOrderDetails | None:
        pending_order_details = self._pending_orders.get(order_id)
        if pending_order_details is not None:
            updated_order_details = await self.get_order(order_id)
            if pending_order_details.status in [OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED] \
                    and updated_order_details.status in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]:
                # If info received in TradeEvents are more up-to-date than the info returned by the get_order() call
                updated_order_details.status = pending_order_details.status
                updated_order_details.quantity_filled = pending_order_details.quantity_filled
                updated_order_details.quote_quantity_filled = pending_order_details.quote_quantity_filled
            if updated_order_details.quote_quantity == 0 and pending_order_details.quote_quantity > 0:
                # If the get_order() call does not provide the original quote quantity
                updated_order_details.quote_quantity = pending_order_details.quote_quantity
            if order_id in self._pending_orders:
                self._remove_completed_pending_order(updated_order_details)
                self.raise_event(OrderStatusUpdateEvent(order=updated_order_details))
            return updated_order_details
        return None

    def _remove_completed_pending_order(self, order: FullOrderDetails) -> None:
        if order.id in self._pending_orders and order.status in [
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.CANCELLED_AND_PARTIALLY_FILLED,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED
        ]:
            try:
                del self._pending_orders[order.id]
            except KeyError:
                pass

    async def get_open_orders(self) -> list[FullOrderDetails]:
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_open_orders()

    async def get_asset_balances(self) -> tuple[AssetBalance, AssetBalance]:
        """Gets a tuple consisting of the base asset and quote asset balance
        (in that order) for the account associated with this market.
        """
        if self._status == MarketStatus.CLOSED:
            raise MarketException(f"Market closed", self.name)
        return await self._client.get_asset_balances()

    def subscribe(self, subscriber) -> None:
        self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber) -> None:
        self._subscribers.remove(subscriber)

    def raise_event(self, event: MarketEvent) -> None:
        event.market_name = self.name
        event.symbol = self.symbol
        if isinstance(event, OrderStatusUpdateEvent):
            self._on_order_status_update_event(event)
        for sub in self._subscribers:
            task = None
            if isinstance(event, OrderBookItemAddedEvent):
                task = sub.on_order_book_item_added(event)
            elif isinstance(event, OrderBookItemRemovedEvent):
                task = sub.on_order_book_item_removed(event)
            elif isinstance(event, TradeEvent):
                task = sub.on_trade(event)
            elif isinstance(event, CandleEvent):
                task = sub.on_candle(event)
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

    def _on_order_status_update_event(self, event: OrderStatusUpdateEvent) -> None:
        order = event.order
        if self.logger.isEnabledFor(logging.INFO):
            msg = f"{order.action.name} order {order.id} {order.status.value}"
            if order.status == OrderStatus.PARTIALLY_FILLED \
                    or order.status == OrderStatus.CANCELLED_AND_PARTIALLY_FILLED:
                msg += f" ({self._get_partially_filed_amount_str(order)})"
            level = logging.WARNING if order.status == OrderStatus.CANCELLED_AND_PARTIALLY_FILLED else logging.INFO
            self._log(level, msg)

    def _get_partially_filed_amount_str(self, order: FullOrderDetails) -> str:
        s = f"{to_amount_str(order.quantity_filled)}"
        if order.quantity > 0:
            s += f" / {to_amount_str(order.quantity)}"
        return s

    def _log(self, level: int, message: str) -> None:
        if self.logger.isEnabledFor(level):
            frame_stack = inspect.stack()
            if len(frame_stack) > 2:
                caller = frame_stack[2].frame.f_locals.get("self")
                if caller is not None and hasattr(caller, "strategy_name"):
                    message = f"{getattr(caller, 'strategy_name')} - {message}"
            self.logger.log(level, message)
