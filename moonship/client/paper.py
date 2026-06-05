#  Copyright (c) 2026 Marlon Paulse
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

import asyncio
import importlib
import uuid

from datetime import timezone
from moonship.core import *
from typing import Callable, Iterator


class PaperTradingClient(MarketClient, MarketSubscriber):

    def __init__(self, market_name: str, app_config: Config):
        super().__init__(market_name, app_config)
        real_client = app_config.get(f"moonship.markets.{market_name}.real_client")
        if not isinstance(real_client, str):
            raise ConfigException("Real market client not configured")
        module_name, class_name = real_client.rsplit(".", 1)
        module = importlib.import_module(module_name)
        real_client_class = getattr(module, class_name)
        if real_client_class is None or not issubclass(real_client_class, MarketClient):
            raise ConfigException(f"Invalid configuration: moonship.markets.{market_name}.real_client")
        self._real_client = real_client_class(market_name, app_config)
        self._orders: dict[str, FullOrderDetails] = {}

    async def connect(self) -> None:
        self._real_client._set_market(self.market)
        self.market.subscribe(self)
        await self._real_client.connect()

    async def close(self) -> None:
        await self._real_client.close()

    async def get_market_info(self, use_cached: bool = True) -> MarketInfo:
        return await self._real_client.get_market_info(use_cached)

    async def get_ticker(self) -> Ticker:
        return await self._real_client.get_ticker()

    async def get_recent_trades(self, limit: int) -> list[Trade]:
        return await self._real_client.get_recent_trades(limit)

    async def get_trades(self, handler: Callable[[list[Trade]], None], from_time: Timestamp) -> None:
        return await self._real_client.get_trades(handler, from_time)

    async def get_candles(self, period: CandlePeriod, from_time: Timestamp = None) -> list[Candle]:
        return await self._real_client.get_candles(period, from_time)

    async def get_asset_balances(self) -> tuple[AssetBalance, AssetBalance]:
        return \
            AssetBalance(asset=self.market.base_asset,total=Amount(0), available=Amount(0)), \
            AssetBalance(asset=self.market.quote_asset,total=Amount(0), available=Amount(0))

    async def place_order(self, order: MarketOrder | LimitOrder) -> str:
        quantity_filled = Amount(0)
        quote_quantity_filled = Amount(0)
        if isinstance(order, LimitOrder):
            if order.post_only \
                    and ((order.action == OrderAction.BUY and order.price >= self.market.ask_price)
                         or (order.action == OrderAction.SELL and order.price <= self.market.bid_price)):
                raise MarketException(
                    "Post-only limit order would have matched",
                    self.market.name,
                    MarketErrorCode.POST_ONLY_ORDER_CANCELLED)
            quantity = order.quantity
            quote_quantity = round_amount(order.price * order.quantity, self.market.quote_asset_precision)
            if order.time_in_force in [TimeInForce.IMMEDIATE_OR_CANCEL or TimeInForce.FILL_OR_KILL]:
                available_quantity = self._get_available_order_book_quantity(order.action, order.price)
                if available_quantity < order.quantity:
                    if order.time_in_force == TimeInForce.FILL_OR_KILL:
                        status = OrderStatus.CANCELLED
                    else:
                        status = OrderStatus.CANCELLED_AND_PARTIALLY_FILLED
                        quantity_filled = available_quantity
                        quote_quantity_filled = round_amount(order.price * quantity_filled, self.market.quote_asset_precision)
                else:
                    status = OrderStatus.FILLED
                    quantity_filled = quantity
                    quote_quantity_filled = quote_quantity_filled
            else:
                status = OrderStatus.PENDING
        else:
            price, quantity_filled = \
                self._get_market_order_fill_price_and_quantity(
                    order.action,
                    order.quantity,
                    order.is_base_quantity)
            quote_quantity_filled = round_amount(quantity_filled * price, self.market.quote_asset_precision)
            if order.is_base_quantity:
                quantity = order.quantity
                quote_quantity = round_amount(quantity * price, self.market.quote_asset_precision)
            else:
                quote_quantity = order.quantity
                quantity = round_amount(quote_quantity / price, self.market.base_asset_precision)
            status = \
                OrderStatus.CANCELLED_AND_PARTIALLY_FILLED if quantity_filled < quantity \
                else OrderStatus.FILLED
        order_details = FullOrderDetails(
            id=str(uuid.uuid4()),
            symbol=self.market.symbol,
            action=order.action,
            quantity=quantity,
            quantity_filled=quantity_filled,
            quote_quantity=quote_quantity,
            quote_quantity_filled=quote_quantity_filled,
            limit_price=order.price if isinstance(order, LimitOrder) else Amount(0),
            time_in_force=order.time_in_force if isinstance(order, LimitOrder) else None,
            creation_timestamp=Timestamp.now(tz=timezone.utc),
            status=status)
        self._orders[order_details.id] = order_details
        if status != OrderStatus.PENDING:
            asyncio.create_task(self._complete_order(order_details, status))
        return order_details.id

    def _get_available_order_book_quantity(self, action: OrderAction, price: Amount) -> Amount:
        quantity = Amount(0)
        entries: Iterator[OrderBookEntry] = \
            iter(self.market.asks.values()) if action == OrderAction.BUY \
            else reversed(self.market.bids.values())
        for entry in entries:
            if (action == OrderAction.BUY and price < entry.price) \
                  or (action == OrderAction.SELL and price > entry.price):
                break
            quantity += entry.quantity
        return quantity

    def _get_market_order_fill_price_and_quantity(
        self,
        action: OrderAction,
        order_amount: Amount,
        order_amount_is_base: bool
    ) -> tuple[Amount, Amount]:
        total_quantity = Amount(0)
        total_quote_quantity = Amount(0)
        entries: Iterator[OrderBookEntry] = \
            iter(self.market.asks.values()) if action == OrderAction.BUY \
            else reversed(self.market.bids.values())
        for entry in entries:
            quote_quantity = entry.quantity * entry.price
            if order_amount_is_base and total_quantity + entry.quantity >= order_amount:
                rem_amount = order_amount - total_quantity
                total_quantity += rem_amount
                total_quote_quantity += round_amount(rem_amount * entry.price, self.market.quote_asset_precision)
            elif not order_amount_is_base and total_quote_quantity + quote_quantity >= order_amount:
                rem_amount = order_amount - total_quote_quantity
                total_quote_quantity += rem_amount
                total_quantity += round_amount(rem_amount / entry.price, self.market.base_asset_precision)
                break
            total_quantity += entry.quantity
            total_quote_quantity += quote_quantity
        return round_amount(total_quote_quantity / total_quantity, self.market.quote_asset_precision), total_quantity

    async def _complete_order(
        self,
        order_details: FullOrderDetails,
        status: OrderStatus = OrderStatus.FILLED
    ) -> None:
        order_details.status = status
        if order_details.quantity_filled == 0:
            order_details.quantity_filled = order_details.quantity
        order_details.quantity_filled_fee = Amount(0)
        if order_details.quote_quantity_filled == 0:
            order_details.quote_quantity_filled = order_details.quote_quantity
        order_details.quote_quantity_filled_fee = Amount(0)
        is_limit_order = order_details.limit_price > 0
        taker_action = order_details.action
        if is_limit_order:
            taker_action = OrderAction.SELL if order_details.action == OrderAction.BUY else OrderAction.BUY
        self.market.raise_event(
            TradeEvent(
                timestamp=Timestamp.now(tz=timezone.utc),
                trade=Trade(
                    id=str(uuid.uuid4()),
                    timestamp=Timestamp.now(tz=timezone.utc),
                    symbol=self.market.symbol,
                    quantity=order_details.quantity,
                    price=order_details.limit_price if is_limit_order else self.market.current_price,
                    taker_action=taker_action),
                maker_order_id=order_details.id if is_limit_order else str(uuid.uuid4()),
                taker_order_id=order_details.id if not is_limit_order else str(uuid.uuid4())))

    async def get_order(self, order_id: str) -> FullOrderDetails:
        order = self._orders.get(order_id)
        if order is None:
            raise MarketException(f"No such order: {order_id}", self.market.name)
        return order

    async def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if order is not None:
            order.status = OrderStatus.CANCELLED
        return True

    async def get_open_orders(self) -> list[FullOrderDetails]:
        orders = []
        for order in self._orders.values():
            if order.status == OrderStatus.PENDING:
                orders.append(order)
        return orders

    async def on_trade(self, event: TradeEvent) -> None:
        await self._handle_order_fills(event.trade.price, event.trade.price)

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        await self._handle_order_fills(self.market.ask_price, self.market.bid_price)

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        await self._handle_order_fills(self.market.ask_price, self.market.bid_price)

    async def _handle_order_fills(self, buy_check_price: Amount, sell_check_price: Amount) -> None:
        for order in self._orders.values():
            if order.status == OrderStatus.PENDING \
                and order.limit_price > 0 \
                and ((order.action == OrderAction.BUY and buy_check_price <= order.limit_price)
                    or (order.action == OrderAction.SELL and sell_check_price >= order.limit_price)):
                await self._complete_order(order)
