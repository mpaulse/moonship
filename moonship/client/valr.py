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

import json

import aiohttp
import aiolimiter
import asyncio
import hmac
import hashlib

from dataclasses import dataclass
from datetime import timezone
from moonship.core import *
from moonship.client.web import *
from typing import Optional, Union

API_BASE_URL = "https://api.valr.com"
API_VERSION = "v1"
STREAM_BASE_URL = "wss://api.valr.com"

@dataclass
class ValrFullOrderDetails(FullOrderDetails):
    failed_reason: str = None


class ValrClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()
    limiter = aiolimiter.AsyncLimiter(120, 60)
    public_api_limiter = aiolimiter.AsyncLimiter(10, 60)

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get("moonship.valr.api_key")
        if not isinstance(api_key, str):
            raise StartUpException("VALR API key not configured")
        self.api_secret = app_config.get("moonship.valr.api_secret")
        if not isinstance(self.api_secret, str):
            raise StartUpException("VALR API secret not configured")
        headers = {"X-VALR-API-KEY": api_key}
        self.async_tasks: list[asyncio.Task] = []
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(headers=headers),
            [
                WebClientStreamParameters(url=f"{STREAM_BASE_URL}/ws/account", headers=headers),
                WebClientStreamParameters(url=f"{STREAM_BASE_URL}/ws/trade", headers=headers)
            ])

    async def close(self) -> None:
        await super().close()
        for task in self.async_tasks:
            task.cancel()

    async def get_market_info(self, use_cached=True) -> MarketInfo:
        async with self.market_info_lock:
            if not use_cached or self.market.symbol not in self.market_info:
                try:
                    status = MarketStatus.CLOSED
                    async with self.public_api_limiter:
                        async with self.http_session.get(f"{API_BASE_URL}/{API_VERSION}/public/status") as rsp:
                            await self.handle_error_response(rsp)
                            s = (await rsp.json()).get("status")
                            if s == "online":
                                status = MarketStatus.OPEN
                    async with self.public_api_limiter:
                        async with self.http_session.get(f"{API_BASE_URL}/{API_VERSION}/public/pairs") as rsp:
                            await self.handle_error_response(rsp)
                            pairs = await rsp.json()
                            for pair_info in pairs:
                                quote_asset_precision = 0
                                tick_size = pair_info.get("tickSize")
                                if isinstance(tick_size, str):
                                    s = tick_size.split(".")
                                    if len(s) == 2:
                                        quote_asset_precision = len(s[1])
                                info = MarketInfo(
                                    symbol=pair_info.get("symbol"),
                                    base_asset=pair_info.get("baseCurrency"),
                                    base_asset_precision=int(pair_info.get("baseDecimalPlaces")),
                                    base_asset_min_quantity=Amount(pair_info.get("minBaseAmount")),
                                    quote_asset=pair_info.get("quoteCurrency"),
                                    quote_asset_precision=quote_asset_precision,
                                    status=status)
                                self.market_info[info.symbol] = info
                except Exception as e:
                    raise MarketException(
                        f"Could not retrieve market info for {self.market.symbol}", self.market.name) from e
        return self.market_info[self.market.symbol]

    async def get_ticker(self) -> Ticker:
        try:
            async with self.public_api_limiter:
                async with self.http_session.get(
                        f"{API_BASE_URL}/{API_VERSION}/public/{self.market.symbol}/marketsummary") as rsp:
                    await self.handle_error_response(rsp)
                    ticker = await rsp.json()
                    return Ticker(
                        timestamp=self._to_timestamp(ticker.get("created")),
                        symbol=ticker.get("currencyPair"),
                        bid_price=to_amount(ticker.get("bidPrice")),
                        ask_price=to_amount(ticker.get("askPrice")),
                        current_price=to_amount(ticker.get("lastTradedPrice")))
        except Exception as e:
            raise MarketException(f"Could not retrieve ticker for {self.market.symbol}", self.market.name) from e

    async def get_recent_trades(self, limit) -> list[Trade]:
        try:
            trades: list[Trade] = []
            before_id = None
            for i in range(0, limit, 100):
                async with self.limiter:
                    path = f"/{API_VERSION}/marketdata/{self.market.symbol}/tradehistory?limit=100"
                    if before_id is not None:
                        path += f"&beforeId={before_id}"
                    async with self.http_session.get(
                            f"{API_BASE_URL}{path}",
                            headers=self._get_auth_headers("GET", path)) as rsp:
                        await self.handle_error_response(rsp)
                        trades_data = await rsp.json()
                        if isinstance(trades_data, list):
                            for data in trades_data:
                                trades.append(
                                    Trade(
                                        timestamp=self._to_timestamp(data.get("tradedAt")),
                                        symbol=data.get("currencyPair"),
                                        price=to_amount(data.get("price")),
                                        quantity=to_amount(data.get("quantity")),
                                        taker_action=OrderAction.BUY if data.get(
                                            "takerSide") == "buy" else OrderAction.SELL))
                                before_id = data.get("id")
            return trades
        except Exception as e:
            raise MarketException(f"Could not retrieve recent trades for {self.market.symbol}", self.market.name) from e

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        request = {
            "pair": self.market.symbol,
            "side": order.action.name
        }
        if isinstance(order, LimitOrder):
            order_type = "limit"
            request["postOnly"] = order.post_only
            request["price"] = to_amount_str(order.price)
            request["quantity"] = to_amount_str(order.quantity)
            request["timeInForce"] = "GTC"
        else:
            order_type = "market"
            if order.is_base_quantity:
                request["baseAmount"] = to_amount_str(order.quantity)
            else:
                request["quoteAmount"] = to_amount_str(order.quantity)
        request = json.dumps(request, indent=None)
        try:
            path = f"/{API_VERSION}/orders/{order_type}"
            async with self.limiter:
                async with self.http_session.post(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("POST", path, request),
                        data=request) as rsp:
                    await self.handle_error_response(rsp)
                    order.id = (await rsp.json()).get("id")
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name) from e
        order_details = await self.get_order(order.id)
        if order_details.status == OrderStatus.REJECTED:
            error_code = self._get_error_code(order_details.failed_reason)
            raise MarketException("Failed to place order", self.market.name, error_code)
        return order.id

    async def get_order(self, order_id: str) -> ValrFullOrderDetails:
        try:
            async with self.limiter:
                path = f"/{API_VERSION}/orders/{self.market.symbol}/orderid/{order_id}"
                async with self.http_session.get(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("GET", path)) as rsp:
                    await self.handle_error_response(rsp)
                    order_data = await rsp.json()
                    quantity = to_amount(order_data.get("originalQuantity"))
                    return ValrFullOrderDetails(
                        id=order_id,
                        symbol=order_data.get("currencyPair"),
                        action=self._to_order_action(order_data.get("orderSide")),
                        quantity=quantity,
                        quantity_filled=quantity - to_amount(order_data.get("remainingQuantity")),
                        limit_price=to_amount(order_data.get("originalPrice")),
                        status=self._to_order_status(order_data),
                        failed_reason=order_data.get("failedReason"),
                        creation_timestamp=self._to_timestamp(order_data.get("orderCreatedAt")))
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market.name) from e

    async def cancel_order(self, order_id: str) -> bool:
        request = {
            "orderId": order_id,
            "pair": self.market.symbol
        }
        try:
            async with self.limiter:
                path = f"/{API_VERSION}/orders/order"
                request = json.dumps(request, indent=None)
                async with self.http_session.delete(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("DELETE", path, request),
                        data=request) as rsp:
                    await self.handle_error_response(rsp)
                    return False  # Cancellation confirmation returned via stream or get_order() call
        except Exception as e:
            raise MarketException("Failed to cancel order", self.market.name) from e

    def _get_auth_headers(self, http_method: str, request_path: str, request_body: str = None) -> dict[str, str]:
        timestamp = str(utc_timestamp_now_msec())
        msg = timestamp + http_method.upper() + request_path
        if request_body is not None:
            msg += request_body
        signature = hmac.new(
            bytes(self.api_secret, encoding="utf-8"),
            bytes(msg, encoding="utf-8"),
            hashlib.sha512).hexdigest()
        return {
            "X-VALR-SIGNATURE": signature,
            "X-VALR-TIMESTAMP": timestamp
        }

    async def on_before_data_stream_connect(self, params: WebClientStreamParameters) -> None:
        auth_headers = self._get_auth_headers("GET", params.url[len(STREAM_BASE_URL):])
        for name, value in auth_headers.items():
            params.headers[name] = value

    async def on_after_data_stream_connect(
            self,
            websocket: aiohttp.ClientWebSocketResponse,
            params: WebClientStreamParameters) -> None:
        if params.url.endswith("/trade"):
            await websocket.send_json({
                "type": "SUBSCRIBE",
                "subscriptions": [
                    {
                        "event": "AGGREGATED_ORDERBOOK_UPDATE",
                        "pairs": [self.market.symbol]
                    },
                    {
                        "event": "NEW_TRADE",
                        "pairs": [self.market.symbol]
                    }
                ]
            })
        self.async_tasks.append(asyncio.create_task(self._ping_data_stream(websocket)))

    async def _ping_data_stream(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        while not self.closed and not websocket.closed:
            await websocket.send_json({
                "type": "PING"
            })
            await asyncio.sleep(30)

    async def on_data_stream_msg(self, msg: any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        if isinstance(msg, dict):
            event = msg.get("type")
            data = msg.get("data")
            if isinstance(data, dict):
                if event == "AGGREGATED_ORDERBOOK_UPDATE":
                    self._on_order_book_stream_event(data)
                elif event == "NEW_TRADE":
                    self._on_trade_stream_event(data)
                elif event == "ORDER_STATUS_UPDATE":
                    self._on_order_status_update(data)
                elif event == "FAILED_CANCEL_ORDER":
                    self._on_order_cancellation_failed(data)

    def _on_order_book_stream_event(self, data: dict[str, any]) -> None:
        bids = self._get_orders_from_stream(OrderAction.BUY, data.get("Bids"))
        asks = self._get_orders_from_stream(OrderAction.SELL, data.get("Asks"))
        if len(self.market.bids) == 0 and len(self.market.asks) == 0:
            orders: list[LimitOrder] = []
            for bid in bids.values():
                orders.append(bid)
            for ask in asks.values():
                orders.append(ask)
            self.market.raise_event(
                OrderBookInitEvent(
                    timestamp=Timestamp.now(tz=timezone.utc),
                    orders=orders))
        else:
            self._update_order_book_entries(bids, self.market.bids, OrderAction.BUY)
            self._update_order_book_entries(asks, self.market.asks, OrderAction.SELL)

    def _update_order_book_entries(
            self, orders: dict[Amount, LimitOrder], order_book_entries: OrderBookEntriesView, action: OrderAction
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
                    timestamp=Timestamp.now(tz=timezone.utc),
                    order_id=order_id))

    def _add_order_book_item(self, order: LimitOrder, order_book_entries: OrderBookEntriesView):
        existing_order = order_book_entries.get(order.price)
        if existing_order is None or existing_order.quantity != order.quantity:
            self.market.raise_event(
                OrderBookItemAddedEvent(
                    timestamp=Timestamp.now(tz=timezone.utc),
                    order=order))

    def _get_orders_from_stream(
            self, action: OrderAction, order_book_entries: list[dict[str, any]]
    ) -> dict[Amount, LimitOrder]:
        orders: dict[Amount, LimitOrder] = {}
        if isinstance(order_book_entries, list):
            for entry in order_book_entries:
                price = to_amount(entry.get("price"))
                quantity = to_amount(entry.get("quantity"))
                order = LimitOrder(
                    id=self._generate_order_book_order_id(price, action),
                    action=action,
                    price=price,
                    quantity=quantity)
                orders[order.price] = order
        return orders

    def _generate_order_book_order_id(self, price: Amount, action: OrderAction) -> str:
        # The VALR order book event does not contain individual order granularity,
        # so create mock orders with the order ID = action@price.
        return f"{action.name}@{price}"

    def _on_trade_stream_event(self, data: dict[str, any]) -> None:
        timestamp = self._to_timestamp(data.get("tradedAt"))
        self.market.raise_event(
            TradeEvent(
                timestamp=timestamp,
                trade=Trade(
                    timestamp=timestamp,
                    symbol=self.market.symbol,
                    quantity=to_amount(data.get("quantity")),
                    price=to_amount(data.get("price")),
                    taker_action=self._to_order_action(data.get("takerSide")))))

    def _on_order_status_update(self, data: dict[str, any]) -> None:
        quantity = to_amount(data.get("originalQuantity"))
        self.market.raise_event(
            OrderStatusUpdateEvent(
                order=ValrFullOrderDetails(
                    id=data.get("orderId"),
                    symbol=self.market.symbol,
                    action=self._to_order_action(data.get("orderSide")),
                    quantity=quantity,
                    quantity_filled=quantity - to_amount(data.get("remainingQuantity")),
                    limit_price=to_amount(data.get("originalPrice")),
                    status=self._to_order_status(data),
                    failed_reason=data.get("failedReason"),
                    creation_timestamp=self._to_timestamp(data.get("orderCreatedAt")))))

    def _on_order_cancellation_failed(self, data: dict[str, any]) -> None:
        order_id = data.get("orderId")
        error = data.get("message")
        self.market.logger.warning(f"Failed to cancel order {order_id}: {error}")

    def _to_timestamp(self, s: Optional[str]) -> Timestamp:
        if s is not None:
            if s[-1] == "Z":
                s = s[:-1] + "+00:00"
            return Timestamp.fromisoformat(s)
        return Timestamp.now(tz=timezone.utc)

    def _to_order_action(self, s: str) -> OrderAction:
        return OrderAction.BUY if s.upper() == "BUY" else OrderAction.SELL

    def _to_order_status(self, order_data: dict[str, any]) -> OrderStatus:
        status = order_data.get("orderStatusType")
        quantity = to_amount(order_data.get("originalQuantity"))
        rem_quantity = to_amount(order_data.get("remainingQuantity"))
        if "Failed" in status:
            return OrderStatus.REJECTED
        elif status == "Cancelled":
            if quantity == rem_quantity:
                return OrderStatus.CANCELLED
            elif rem_quantity > 0:
                return OrderStatus.CANCELLED_AND_PARTIALLY_FILLED
            else:
                return OrderStatus.FILLED
        elif status == "Partially Filled":
            return OrderStatus.PARTIALLY_FILLED
        elif status == "Filled":
            return OrderStatus.FILLED
        return OrderStatus.PENDING

    def _get_error_code(self, failed_reason: str) -> MarketErrorCode:
        failed_reason = failed_reason.lower()
        if failed_reason.startswith("post only cancelled"):
            return MarketErrorCode.POST_ONLY_ORDER_CANCELLED
        elif failed_reason.startswith("insufficient balance"):
            return MarketErrorCode.INSUFFICIENT_FUNDS
        return MarketErrorCode.UNKNOWN
