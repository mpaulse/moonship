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

import aiohttp
import aiolimiter
import asyncio
import datetime
import hashlib
import hmac
import json

from moonship.core import *
from moonship.client.web import *
from typing import Any, Callable

API_BASE_URL = "https://api.altcointrader.co.za"
GET_TRADES_MAX_LIMIT = 500


class AltCoinTraderClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()
    limiter = aiolimiter.AsyncLimiter(1000, 60)

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get(f"moonship.markets.{market_name}.api_key")
        if not isinstance(api_key, str):
            api_key = app_config.get("moonship.altcointrader.api_key")
            if not isinstance(api_key, str):
                raise ConfigException("AltCoinTrader API key not configured")
        self.api_secret = app_config.get(f"moonship.markets.{market_name}.api_secret")
        if not isinstance(self.api_secret, str):
            self.api_secret = app_config.get("moonship.altcointrader.api_secret")
            if not isinstance(self.api_secret, str):
                raise ConfigException("AltCoinTrader API secret not configured")
        headers = {
            "Content-Type": "application/json",
            "X-API-KEY": api_key
        }
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(headers=headers),
            WebClientStreamParameters(url=f"{API_BASE_URL}/ws"))

    async def on_after_data_stream_connect(
        self,
        websocket: aiohttp.ClientWebSocketResponse,
        params: WebClientStreamParameters
    ) -> None:
        if params.url.endswith("/ws"):
            await websocket.send_json({
                "action": "subscribe",
                "channel": "orderbook",
                "market": self.market.symbol,
                "limit": 200  # Order book depth (max 200)
            })
            await websocket.send_json({
                "action": "subscribe",
                "channel": "trades",
                "market": self.market.symbol
            })

    async def on_data_stream_msg(self, msg: Any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        if isinstance(msg, dict):
            channel = msg.get("channel")
            data = msg.get("data")
            if channel == "orderbook" and isinstance(data, dict):
                self._on_order_book_stream_event(data)
            elif channel == "trades" and isinstance(data, list):
                self._on_trades_stream_event(data)
            elif channel == "error":
                self._on_error_stream_event(msg)

    def _on_order_book_stream_event(self, data: dict) -> None:
        bids = self._get_order_book_entries_from_stream(OrderAction.BUY, data.get("bids"))
        asks = self._get_order_book_entries_from_stream(OrderAction.SELL, data.get("asks"))
        current_bids = self.market.bids
        current_asks = self.market.asks
        if len(current_bids) == 0 and len(current_asks) == 0:
            orders: list[LimitOrder] = []
            for bid in bids.values():
                orders.append(bid)
            for ask in asks.values():
                orders.append(ask)
            self.market.raise_event(
                OrderBookInitEvent(
                    timestamp=Timestamp.now(tz=datetime.timezone.utc),
                    orders=orders))
        else:
            self._update_order_book_entries(bids, current_bids, OrderAction.BUY)
            self._update_order_book_entries(asks, current_asks, OrderAction.SELL)

    def _update_order_book_entries(
        self,
        orders: dict[Amount, LimitOrder],
        order_book_entries:
        OrderBookEntriesView,
        action: OrderAction
    ) -> None:
        for order in orders.values():
            self._add_order_book_item(order, order_book_entries)
        removed_order_ids: list[str] = []
        for price in order_book_entries.keys():
            if price not in orders:
                removed_order_ids.append(self._generate_order_book_order_id(price, action))
        for order_id in removed_order_ids:
            self.market.raise_event(
                OrderBookItemRemovedEvent(
                    timestamp=Timestamp.now(tz=datetime.timezone.utc),
                    order_id=order_id))

    def _add_order_book_item(self, order: LimitOrder, order_book_entries: OrderBookEntriesView) -> None:
        existing_order = order_book_entries.get(order.price)
        if existing_order is None or existing_order.quantity != order.quantity:
            self.market.raise_event(
                OrderBookItemAddedEvent(
                    timestamp=Timestamp.now(tz=datetime.timezone.utc),
                    order=order))

    def _get_order_book_entries_from_stream(
        self,
        action: OrderAction,
        order_book_entries: list[list[str]]
    ) -> dict[Amount, LimitOrder]:
        orders: dict[Amount, LimitOrder] = {}
        if isinstance(order_book_entries, list):
            for entry in order_book_entries:
                if isinstance(entry, list) and len(entry) == 2:
                    price = to_amount(entry[0])
                    quantity = to_amount(entry[1])
                    order = LimitOrder(
                        id=self._generate_order_book_order_id(price, action),
                        action=action,
                        price=price,
                        quantity=quantity)
                    orders[order.price] = order
        return orders

    def _generate_order_book_order_id(self, price: Amount, action: OrderAction) -> str:
        # The AltCoinTrader order book event does not contain individual order granularity,
        # so create mock orders with the order ID = action@price.
        return f"{action.name}@{price}"

    def _on_trades_stream_event(self, trades: list[dict]) -> None:
        # The received list is the most recent N (50) trades in descending order.
        # Ignore the initial stream event at start-up when the connection is established,
        # before the TradingEngine has initialized the market recent trades list.
        last_trade = next(self.market.recent_trades, None)
        if last_trade is None:
            return
        for trade_data in reversed(trades):
            if isinstance(trade_data, dict):
                trade = self._to_trade(trade_data)
                if trade.timestamp > last_trade.timestamp:
                    self.market.raise_event(
                        TradeEvent(
                            timestamp=trade.timestamp,
                            trade=trade))

    def _on_error_stream_event(self, msg: dict) -> None:
        error = msg.get("message")
        if isinstance(error, str):
            self.logger.error(f"Error: {error}")

    async def get_market_info(self, use_cached: bool = True) -> MarketInfo:
        async with self.market_info_lock:
            if not use_cached or self.market.symbol not in self.market_info:
                try:
                    async with self.limiter:
                        async with self.http_session.get(f"{API_BASE_URL}/markets") as rsp:
                            await self.handle_error_response(rsp)
                            markets = await rsp.json()
                            if isinstance(markets, list):
                                for market in markets:
                                    info = MarketInfo(
                                        symbol=market.get("symbol"),
                                        base_asset=market.get("base"),
                                        base_asset_precision=int(market.get("quantity_precision")),
                                        quote_asset_min_quantity=Amount(market.get("min_order_value")),
                                        quote_asset=market.get("quote"),
                                        quote_asset_precision=int(market.get("price_precision")),
                                        status=\
                                            MarketStatus.OPEN if market.get("status") == "active"
                                            else MarketStatus.CLOSED)
                                    self.market_info[info.symbol] = info
                except Exception as e:
                    raise MarketException(
                        f"Could not retrieve market info for {self.market.symbol}", self.market.name) from e
        return self.market_info[self.market.symbol]

    async def get_ticker(self) -> Ticker:
        try:
            async with self.limiter:
                async with self.http_session.get(f"{API_BASE_URL}/ticker/{self.market.symbol}") as rsp:
                    await self.handle_error_response(rsp)
                    ticker = await rsp.json()
                    return Ticker(
                        timestamp=to_utc_timestamp(ticker.get("timestamp") * 1000),
                        symbol=ticker.get("symbol"),
                        bid_price=self.market.bid_price,
                        ask_price=self.market.ask_price,
                        current_price=to_amount(ticker.get("last")))
        except Exception as e:
            raise MarketException(f"Could not retrieve ticker for {self.market.symbol}", self.market.name) from e

    async def get_recent_trades(self, limit: int) -> list[Trade]:
        """Gets the recent trades in descending order."""
        try:
            # AltCoinTrader does not support pagination of market trade history.
            # The number of trades retrievable is bounded by the API limit.
            return await self._get_trades(limit=limit if limit <= GET_TRADES_MAX_LIMIT else GET_TRADES_MAX_LIMIT)
        except Exception as e:
            raise MarketException(
                f"Could not retrieve recent trades for {self.market.symbol}", self.market.name) from e

    async def get_trades(self, handler: Callable[[list[Trade]], None], from_time: Timestamp) -> None:
        """Gets the trade in descending order."""
        try:
            # AltCoinTrader does not support pagination of market trade history.
            # The number of trades retrievable is bounded by the API limit.
            recent_trades = await self.get_recent_trades(GET_TRADES_MAX_LIMIT)
            trades = []
            for trade in recent_trades:
                if trade.timestamp >= from_time:
                    trades.append(trade)
                else:
                    break
            if len(trades) > 0:
                handler(trades)
        except Exception as e:
            raise MarketException(
                f"Could not retrieve trades for {self.market.symbol}", self.market.name) from e

    async def _get_trades(self, limit = GET_TRADES_MAX_LIMIT) -> list[Trade]:
        trades = []
        async with self.limiter:
            async with self.http_session.get(
                    f"{API_BASE_URL}/trades/{self.market.symbol}",
                    params={ "limit": limit }) as rsp:
                await self.handle_error_response(rsp)
                trades_data = await rsp.json()
                if isinstance(trades_data, list):
                    for trade in trades_data:
                        if isinstance(trade, dict):
                            trades.append(self._to_trade(trade))
        return trades

    async def get_candles(self, period: CandlePeriod, from_time: Timestamp = None) -> list[Candle]:
        # Not supported by AltCoinTrader
        return []

    async def place_order(self, order: MarketOrder | LimitOrder) -> str:
        request = {
            "market": self.market.symbol,
            "side": order.action.name.lower()
        }
        if isinstance(order, LimitOrder):
            # Post-only limit orders not natively supported by AltCoinTrader!!!
            if order.post_only \
                and ((order.action == OrderAction.BUY and order.price >= self.market.ask_price)
                    or (order.action == OrderAction.SELL and order.price <= self.market.bid_price)):
                raise MarketException(
                    "Post-only limit order would have matched",
                    self.market.name,
                    MarketErrorCode.POST_ONLY_ORDER_CANCELLED)
            path = "/orders"
            request["price"] = to_amount_str(order.price)
            request["quantity"] = to_amount_str(order.quantity)
            request["time_in_force"] = order.time_in_force.value
        else:
            path = "/orders/market"
            if order.action == OrderAction.BUY:
                # Buy orders must be the quote quantity
                amount = \
                    order.quantity if not order.is_base_quantity \
                    else round_amount(order.quantity * self.market.ask_price, self.market.quote_asset_precision)
                request["amount"] = to_amount_str(amount)
            else:
                # Sell orders must be the base quantity
                quantity = \
                    order.quantity if order.is_base_quantity \
                    else round_amount(order.quantity / self.market.bid_price, self.market.base_asset_precision)
                request["quantity"] = to_amount_str(quantity)
        request = json.dumps(request, indent=None)
        try:
            async with self.limiter:
                async with self.http_session.post(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("POST", path, request),
                        data=request) as rsp:
                    await self.handle_error_response(rsp)
                    order.id = (await rsp.json()).get("order_id")
                    # Similar to cancel_order(), work around concurrency issues at AltCoinTrader by
                    # adding a small delay after order placement to prevent funds reserved for the
                    # order potentially getting stuck if the order is cancelled in quick succession
                    # afterwards.
                    await asyncio.sleep(1)
                    return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name, self._get_error_code(e)) from e

    async def get_order(self, order_id: str) -> FullOrderDetails:
        try:
            async with self.limiter:
                path = f"/orders/{order_id}"
                async with self.http_session.get(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("GET", path)) as rsp:
                    await self.handle_error_response(rsp)
                    order_data = await rsp.json()
                    return self._to_order_details(order_data)
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market.name) from e

    async def cancel_order(self, order_id: str) -> bool:
        try:
            async with self.limiter:
                path = f"/orders/{order_id}"
                async with self.http_session.delete(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("DELETE", path)) as rsp:
                    if rsp.status == 404:
                        return False
                    await self.handle_error_response(rsp)
                    if rsp.status == 200:
                        # Add a small delay to work around concurrency issues at AltCoinTrader where funds
                        # reserved for the order are not immediately made available for a follow-up order
                        # (and in some cases get stuck!) after the order has been cancelled.
                        await asyncio.sleep(1)
                        return True
                    else:
                        return False
        except Exception as e:
            if isinstance(e, HttpResponseException) \
                    and e.status == 400 \
                    and isinstance(e.body, dict) \
                    and e.body.get("message") == "order is not cancelable":
                return False
            raise MarketException("Failed to cancel order", self.market.name) from e

    async def get_open_orders(self) -> list[FullOrderDetails]:
        orders: list[FullOrderDetails] = []
        try:
            async with self.limiter:
                path = f"/orders/open?market={self.market.symbol}"
                async with self.http_session.get(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("GET", path)) as rsp:
                    await self.handle_error_response(rsp)
                    orders_data = await rsp.json()
                    if isinstance(orders_data, list):
                        for order_data in orders_data:
                            if isinstance(order_data, dict):
                                orders.append(self._to_order_details(order_data))
        except Exception as e:
            raise MarketException("Could not retrieve open orders", self.market.name) from e
        return orders

    async def get_asset_balances(self) -> tuple[AssetBalance, AssetBalance]:
        base_asset_balance: AssetBalance | None = None
        quote_asset_balance: AssetBalance | None = None
        try:
            async with self.limiter:
                path = "/balances"
                async with self.http_session.get(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("GET", path)) as rsp:
                    await self.handle_error_response(rsp)
                    balance_list = await rsp.json()
                    if isinstance(balance_list, list):
                        for data in balance_list:
                            if isinstance(data, dict):
                                asset = data.get("currency")
                                if asset == self.market.base_asset:
                                    base_asset_balance = \
                                        AssetBalance(
                                            asset=asset,
                                            total=to_amount(data.get("total")),
                                            available=to_amount(data.get("available")))
                                elif asset == self.market.quote_asset:
                                    quote_asset_balance = \
                                        AssetBalance(
                                            asset=asset,
                                            total=to_amount(data.get("total")),
                                            available=to_amount(data.get("available")))
        except Exception as e:
            raise MarketException("Could not retrieve asset balances", self.market.name) from e
        if base_asset_balance is None:
            raise MarketException(f"{self.market.base_asset} balance not returned by exchange", self.market.name)
        if quote_asset_balance is None:
            raise MarketException(f"{self.market.quote_asset} balance not returned by exchange", self.market.name)
        return base_asset_balance, quote_asset_balance

    def _get_auth_headers(self, http_method: str, request_path: str, request_body: str = None) -> dict[str, str]:
        timestamp = str(int(utc_timestamp_now_msec() / 1000))
        msg = f"{timestamp}\n{http_method.upper()}\n{request_path}\n"
        if request_body is not None:
            msg += request_body
        signature = hmac.new(
            bytes(self.api_secret, encoding="utf-8"),
            bytes(msg, encoding="utf-8"),
            hashlib.sha256).hexdigest()
        return {
            "X-SIGNATURE": signature,
            "X-TIMESTAMP": timestamp
        }

    def _get_error_code(self, e: Exception) -> MarketErrorCode:
        if isinstance(e, HttpResponseException) and isinstance(e.body, dict):
            match e.body.get("code"):
                case "INSUFFICIENT_FUNDS":
                    return MarketErrorCode.INSUFFICIENT_FUNDS
        return MarketErrorCode.UNKNOWN

    def _to_trade(self, trade_data: dict[str, Any]) -> Trade:
        return Trade(
            id=trade_data.get("trade_id"),
            timestamp=to_utc_timestamp(trade_data.get("timestamp") * 1000),
            symbol=self.market.symbol,
            quantity=to_amount(trade_data.get("quantity")),
            price=to_amount(trade_data.get("price")),
            taker_action=OrderAction.BUY if trade_data.get("side") == "buy" else OrderAction.SELL)

    def _to_order_details(self, order_data: dict[str, Any]) -> FullOrderDetails:
        order = FullOrderDetails(
            id=order_data.get("order_id"),
            symbol=order_data.get("market"),
            action=OrderAction.BUY if order_data.get("side") == "buy" else OrderAction.SELL,
            quantity=to_amount(order_data.get("quantity")),
            quantity_filled=to_amount(order_data.get("filled")),
            limit_price=to_amount(order_data.get("price")),
            status=self._to_order_status(order_data),
            creation_timestamp=to_utc_timestamp(order_data.get("created_at") * 1000))
        try:
            order.time_in_force = TimeInForce(order_data.get("time_in_force"))
        except ValueError:
            pass
        return order

    def _to_order_status(self, order_data: dict[str, Any]) -> OrderStatus:
        status = order_data.get("status")
        quantity = to_amount(order_data.get("quantity"))
        quantity_remaining = to_amount(order_data.get("remaining"))
        if status == "rejected":
            return OrderStatus.REJECTED
        elif status == "cancelled":
            if quantity == quantity_remaining:
                return OrderStatus.CANCELLED
            elif quantity_remaining > 0:
                return OrderStatus.CANCELLED_AND_PARTIALLY_FILLED
            else:
                return OrderStatus.FILLED
        elif status == "partially_filled":
            return OrderStatus.PARTIALLY_FILLED
        elif status == "filled":
            return OrderStatus.FILLED
        return OrderStatus.PENDING
