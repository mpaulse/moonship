#  Copyright (c) 2024, Marlon Paulse
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
import datetime
import hmac
import hashlib

from dataclasses import dataclass
from datetime import timezone
from moonship.core import *
from moonship.client.web import *
from typing import Optional, Union

API_BASE_URL = "https://api.valr.com"
API_VERSION_1 = "v1"
API_VERSION_2 = "v2"
STREAM_BASE_URL = "wss://api.valr.com"

@dataclass
class ValrFullOrderDetails(FullOrderDetails):
    failed_reason: str = None


class ValrClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()
    limiter = aiolimiter.AsyncLimiter(1000, 60)
    public_api_limiter = aiolimiter.AsyncLimiter(10, 60)

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get(f"moonship.markets.{market_name}.api_key")
        if not isinstance(api_key, str):
            api_key = app_config.get("moonship.valr.api_key")
            if not isinstance(api_key, str):
                raise ConfigException("VALR API key not configured")
        self.api_secret = app_config.get(f"moonship.markets.{market_name}.api_secret")
        if not isinstance(self.api_secret, str):
            self.api_secret = app_config.get("moonship.valr.api_secret")
            if not isinstance(self.api_secret, str):
                raise ConfigException("VALR API secret not configured")
        headers = {
            "Content-Type": "application/json",
            "X-VALR-API-KEY": api_key
        }
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
                        async with self.http_session.get(f"{API_BASE_URL}/{API_VERSION_1}/public/status") as rsp:
                            await self.handle_error_response(rsp)
                            s = (await rsp.json()).get("status")
                            if s == "online":
                                status = MarketStatus.OPEN
                    async with self.public_api_limiter:
                        async with self.http_session.get(f"{API_BASE_URL}/{API_VERSION_1}/public/pairs") as rsp:
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
                        f"{API_BASE_URL}/{API_VERSION_1}/public/{self.market.symbol}/marketsummary") as rsp:
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

    async def get_recent_trades(self, limit: int) -> list[Trade]:
        try:
            trades: list[Trade] = []
            before_id = None
            for i in range(0, limit, 100):
                async with self.limiter:
                    path = f"/{API_VERSION_1}/marketdata/{self.market.symbol}/tradehistory?limit=100"
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

    async def get_candles(self, period: CandlePeriod, from_time: Timestamp = None) -> list[Candle]:
        params = {
            "periodSeconds": period.value
        }
        if from_time is not None:
            params["startTime"] = from_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

            # Max. candles allowed to be returned is 100. The endTime must explicitly be
            # fixed to period * 100 seconds after the startTime or an error occurs if the
            # date range covers more than 100 candles.
            end_time = from_time + datetime.timedelta(seconds=period.value * 100)
            params["endTime"] = end_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        try:
            candles: list[Candle] = []
            async with self.public_api_limiter:
                async with self.http_session.get(
                        f"{API_BASE_URL}/{API_VERSION_1}/public/{self.market.symbol}/markprice/buckets",
                        params=params) as rsp:
                    await self.handle_error_response(rsp)
                    candle_data = await rsp.json()
                    if isinstance(candle_data, list):
                        for data in candle_data:
                            candle_start = self._to_timestamp(data.get("startTime"))
                            candles.append(
                                Candle(
                                    symbol=self.market.symbol,
                                    start_time=candle_start,
                                    end_time=candle_start
                                    + datetime.timedelta(seconds=period.value)
                                    - datetime.timedelta(milliseconds=1),
                                    period=period,
                                    open=to_amount(data.get("open")),
                                    close=to_amount(data.get("close")),
                                    high=to_amount(data.get("high")),
                                    low=to_amount(data.get("low"))))
            return list(reversed(candles))
        except Exception as e:
            raise MarketException(f"Could not retrieve candles for {self.market.symbol}", self.market.name) from e

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
            request["timeInForce"] = order.time_in_force.value
        else:
            order_type = "market"
            if order.is_base_quantity:
                request["baseAmount"] = to_amount_str(order.quantity)
            else:
                request["quoteAmount"] = to_amount_str(order.quantity)
        request = json.dumps(request, indent=None)
        try:
            path = f"/{API_VERSION_2}/orders/{order_type}"
            async with self.limiter:
                async with self.http_session.post(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("POST", path, request),
                        data=request) as rsp:
                    await self.handle_error_response(rsp)
                    order.id = (await rsp.json()).get("id")
                    return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name, self._get_error_code(e)) from e

    async def get_order(self, order_id: str) -> ValrFullOrderDetails:
        try:
            return await self._get_order(order_id, get_history_summary=True)
        except MarketException:
            # Orders that are not completed are invalid for history summary requests.
            return await self._get_order(order_id, get_history_summary=False)

    async def _get_order(self, order_id: str, get_history_summary: bool) -> ValrFullOrderDetails:
        try:
            async with self.limiter:
                path = f"/{API_VERSION_1}/orders/"
                if get_history_summary:
                    path += "history/summary"
                else:
                    path += self.market.symbol
                path += f"/orderid/{order_id}"
                async with self.http_session.get(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("GET", path)) as rsp:
                    await self.handle_error_response(rsp)
                    order_data = await rsp.json()

                    quantity = to_amount(order_data.get("originalQuantity"))
                    quantity_filled = quantity - to_amount(order_data.get("remainingQuantity"))

                    order_details = ValrFullOrderDetails(
                        id=order_id,
                        symbol=order_data.get("currencyPair"),
                        action=self._to_order_action(order_data.get("orderSide")),
                        quantity=quantity,
                        quantity_filled=quantity_filled,
                        limit_price=to_amount(order_data.get("originalPrice")),
                        status=self._to_order_status(order_data),
                        failed_reason=order_data.get("failedReason"),
                        creation_timestamp=self._to_timestamp(order_data.get("orderCreatedAt")))
                    try:
                        order_details.time_in_force = TimeInForce(order_data.get("timeInForce"))
                    except ValueError:
                        pass

                    # Only in history summary
                    price = to_amount(order_data.get("averagePrice"))
                    if price > 0:
                        order_details.quote_quantity_filled = quantity_filled * price
                    fee = to_amount(order_data.get("totalFee"))
                    fee_asset = order_data.get("feeCurrency")
                    if fee > 0 and fee_asset is not None:
                        if fee_asset == self.market.base_asset:
                            order_details.quantity_filled_fee = fee
                        else:
                            order_details.quote_quantity_filled_fee = fee

                    return order_details
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market.name) from e

    async def cancel_order(self, order_id: str) -> bool:
        request = {
            "orderId": order_id,
            "pair": self.market.symbol
        }
        try:
            async with self.limiter:
                path = f"/{API_VERSION_2}/orders/order"
                request = json.dumps(request, indent=None)
                async with self.http_session.delete(
                        f"{API_BASE_URL}{path}",
                        headers=self._get_auth_headers("DELETE", path, request),
                        data=request) as rsp:
                    if rsp.status == 404:
                        return False
                    await self.handle_error_response(rsp)
                    return rsp.status == 200
        except Exception as e:
            if isinstance(e, HttpResponseException) \
                    and e.status == 400 \
                    and isinstance(e.body, dict) \
                    and e.body.get("message") == "Could not find the order to cancel":
                return False
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
        elif status == "Expired":
            if rem_quantity > 0:
                return OrderStatus.CANCELLED_AND_PARTIALLY_FILLED
            else:
                return OrderStatus.EXPIRED
        return OrderStatus.PENDING

    def _get_error_code(self, e: Exception) -> MarketErrorCode:
        if isinstance(e, HttpResponseException) and isinstance(e.body, dict):
            match e.body.get("code"):
                case -1:
                    return MarketErrorCode.NO_SUCH_ORDER
                case -6:
                    return MarketErrorCode.INSUFFICIENT_FUNDS
                case -19:
                    return MarketErrorCode.POST_ONLY_ORDER_CANCELLED
        return MarketErrorCode.UNKNOWN
