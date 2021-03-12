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

import aiohttp
import aiolimiter
import asyncio

from moonship.core import *
from moonship.client.web import *
from typing import Union

API_BASE_URL = "https://api.luno.com/api/1"
EXCHANGE_API_BASE_URL = "https://api.luno.com/api/exchange/1"
STREAM_BASE_URL = "wss://ws.luno.com/api/1/stream"
LUNO_MAX_DECIMALS = MAX_DECIMALS


class LunoClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()
    market_data_limiter = aiolimiter.AsyncLimiter(5, 1)
    limiter = aiolimiter.AsyncLimiter(25, 1)

    def __init__(self, market_name: str, app_config: Config):
        key_id = app_config.get("moonship.luno.key_id")
        if not isinstance(key_id, str):
            raise StartUpException("Luno API key ID not configured")
        key_secret = app_config.get("moonship.luno.key_secret")
        if not isinstance(key_secret, str):
            raise StartUpException("Luno API key secret not configured")
        self.data_stream_seq_num = -1
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(
                stream_url=f"{STREAM_BASE_URL}/{app_config.get(f'moonship.markets.{market_name}.symbol')}",
                auth=aiohttp.BasicAuth(key_id, key_secret)))

    async def get_market_info(self, use_cached=True) -> MarketInfo:
        async with self.market_info_lock:
            if not use_cached or self.market.symbol not in self.market_info:
                try:
                    async with self.market_data_limiter:
                        async with self.http_session.get(f"{EXCHANGE_API_BASE_URL}/markets") as rsp:
                            await self.handle_error_response(rsp)
                            markets = (await rsp.json()).get("markets")
                            for market_data in markets:
                                info = MarketInfo(
                                    symbol=market_data.get("market_id"),
                                    base_asset=market_data.get("base_currency"),
                                    base_asset_precision=market_data.get("volume_scale"),
                                    base_asset_min_quantity=Amount(market_data.get("min_volume")),
                                    quote_asset=market_data.get("counter_currency"),
                                    quote_asset_precision=market_data.get("price_scale"),
                                    status=self._to_market_status(market_data.get("trading_status")))
                                self.market_info[info.symbol] = info
                except Exception as e:
                    raise MarketException(
                        f"Could not retrieve market info for {self.market.symbol}", self.market.name) from e
        return self.market_info[self.market.symbol]

    async def get_ticker(self) -> Ticker:
        try:
            async with self.market_data_limiter:
                async with self.http_session.get(f"{API_BASE_URL}/ticker", params={"pair": self.market.symbol}) as rsp:
                    await self.handle_error_response(rsp)
                    ticker = await rsp.json()
                    return Ticker(
                        timestamp=to_utc_timestamp(ticker.get("timestamp")),
                        symbol=ticker.get("pair"),
                        bid_price=to_amount(ticker.get("bid")),
                        ask_price=to_amount(ticker.get("ask")),
                        current_price=to_amount(ticker.get("last_trade")))
        except Exception as e:
            raise MarketException(f"Could not retrieve ticker for {self.market.symbol}", self.market.name) from e

    async def get_recent_trades(self, limit) -> list[Trade]:
        params = {
            "pair": self.market.symbol
        }
        try:
            async with self.market_data_limiter:
                async with self.http_session.get(f"{API_BASE_URL}/trades", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    trades: list[Trade] = []
                    trades_data = (await rsp.json())["trades"]
                    if isinstance(trades_data, list):
                        for data in trades_data:
                            trades.append(
                                Trade(
                                    timestamp=to_utc_timestamp(data.get("timestamp")),
                                    symbol=data.get("pair"),
                                    price=to_amount(data.get("price")),
                                    quantity=to_amount(data.get("volume")),
                                    taker_action=OrderAction.BUY if data.get("is_buy") is True else OrderAction.SELL))
                    return trades
        except Exception as e:
            raise MarketException(f"Could not retrieve recent trades for {self.market.symbol}", self.market.name) from e

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        request = {
            "pair": self.market.symbol
        }
        if isinstance(order, LimitOrder):
            order_type = "postorder"
            request["type"] = "BID" if order.action == OrderAction.BUY else "ASK"
            request["post_only"] = order.post_only
            request["price"] = to_amount_str(order.price, LUNO_MAX_DECIMALS)
            request["volume"] = to_amount_str(order.quantity, LUNO_MAX_DECIMALS)
        else:
            order_type = "marketorder"
            request["type"] = order.action.name
            if order.is_base_quantity:
                request["base_volume"] = to_amount_str(order.quantity, LUNO_MAX_DECIMALS)
            else:
                request["counter_volume"] = to_amount_str(order.quantity, LUNO_MAX_DECIMALS)
        try:
            async with self.limiter:
                async with self.http_session.post(f"{API_BASE_URL}/{order_type}", data=request) as rsp:
                    await self.handle_error_response(rsp)
                    order.id = (await rsp.json()).get("order_id")
                    return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name, self._get_error_code(e)) from e

    async def get_order(self, order_id: str) -> FullOrderDetails:
        try:
            async with self.limiter:
                async with self.http_session.get(f"{API_BASE_URL}/orders/{order_id}") as rsp:
                    await self.handle_error_response(rsp)
                    order_data = await rsp.json()
                    state = order_data.get("state")
                    quantity_filled = to_amount(order_data.get("base"))
                    return FullOrderDetails(
                        id=order_id,
                        symbol=order_data.get("pair"),
                        action=self._to_order_action(order_data.get("type")),
                        quantity_filled=quantity_filled,
                        quote_quantity_filled=to_amount(order_data.get("counter")),
                        limit_price=to_amount(order_data.get("limit_price")),
                        limit_quantity=to_amount(order_data.get("limit_volume")),
                        status=OrderStatus.CANCELLED if order_data.get("expiration_timestamp") != 0 and state == "COMPLETE"
                        else OrderStatus.FILLED if state == "COMPLETE"
                        else OrderStatus.PARTIALLY_FILLED if quantity_filled > 0
                        else OrderStatus.PENDING,
                        creation_timestamp=to_utc_timestamp(order_data.get("creation_timestamp")))
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market.name) from e

    async def cancel_order(self, order_id: str) -> bool:
        request = {
            "order_id": order_id
        }
        try:
            async with self.limiter:
                async with self.http_session.post(f"{API_BASE_URL}/stoporder", data=request) as rsp:
                    if rsp.status == 404:
                        return False
                    await self.handle_error_response(rsp)
                    return bool((await rsp.json()).get("success"))
        except Exception as e:
            raise MarketException("Failed to cancel order", self.market.name) from e

    async def on_after_data_stream_connect(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        await websocket.send_json({
            "api_key_id": self.session_params.auth.login,
            "api_key_secret": self.session_params.auth.password
        })

    async def on_data_stream_msg(self, msg: any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        if not isinstance(msg, dict):
            return
        elif self.data_stream_seq_num < 0:
            self.data_stream_seq_num = int(msg.get("sequence"))
            orders: list[LimitOrder] = []
            self._get_orders_from_stream(OrderAction.BUY, msg.get("bids"), orders)
            self._get_orders_from_stream(OrderAction.SELL, msg.get("asks"), orders)
            self.market.raise_event(
                OrderBookInitEvent(
                    timestamp=to_utc_timestamp(msg.get("timestamp")),
                    orders=orders))
        else:
            self.data_stream_seq_num += 1
            if self.data_stream_seq_num != int(msg.get("sequence")):
                self.data_stream_seq_num = -1
                raise MarketException("Data stream out of sequence", self.market.name)
            timestamp = to_utc_timestamp(msg.get("timestamp"))
            self._on_trade_stream_events(msg.get("trade_updates"), timestamp)
            self._on_order_book_entry_added_stream_event(msg.get("create_update"), timestamp)
            self._on_order_book_entry_removed_stream_event(msg.get("delete_update"), timestamp)
            self._on_order_book_entry_added_stream_event(msg.get("status_update"), timestamp)

    def _on_trade_stream_events(self, events: list[dict], timestamp: Timestamp) -> None:
        if isinstance(events, list):
            bids = self.market.bids
            for data in events:
                if isinstance(data, dict):
                    quantity = to_amount(data.get("base"))
                    price = to_amount(data.get("counter")) / quantity
                    maker_order_id = data.get("maker_order_id")
                    bids_entry = bids.get(price)
                    taker_action = \
                        OrderAction.SELL if bids_entry is not None and maker_order_id in bids_entry \
                        else OrderAction.BUY
                    self.market.raise_event(
                        TradeEvent(
                            timestamp=timestamp,
                            trade=Trade(
                                timestamp=timestamp,
                                symbol=self.market.symbol,
                                quantity=quantity,
                                price=price,
                                taker_action=taker_action),
                            maker_order_id=maker_order_id,
                            taker_order_id=data.get("taker_order_id")))

    def _on_order_book_entry_added_stream_event(self, event: dict, timestamp: Timestamp) -> None:
        if isinstance(event, dict):
            self.market.raise_event(
                OrderBookItemAddedEvent(
                    timestamp=timestamp,
                    order=self._get_order_from_stream(self._to_order_action(event.get("type")), event)))

    def _on_order_book_entry_removed_stream_event(self, event: dict, timestamp: Timestamp) -> None:
        if isinstance(event, dict):
            self.market.raise_event(
                OrderBookItemRemovedEvent(
                    timestamp=timestamp,
                    order_id=event.get("order_id")))

    def _on_market_status_stream_event(self, event: dict, timestamp: Timestamp) -> None:
        if isinstance(event, dict):
            self.market.raise_event(
                MarketStatusEvent(
                    timestamp=timestamp,
                    status=self._to_market_status(event.get("status"))))

    def _get_orders_from_stream(self, action: OrderAction, order_data: list[dict], orders: list[LimitOrder]):
        for data in order_data:
            orders.append(self._get_order_from_stream(action, data))

    def _get_order_from_stream(self, action: OrderAction, order_data) -> LimitOrder:
        return LimitOrder(
            id=order_data.get("id") if "id" in order_data else order_data.get("order_id"),
            action=action,
            price=to_amount(order_data.get("price")),
            quantity=to_amount(order_data.get("volume")))

    def _to_market_status(self, s: str) -> MarketStatus:
        return MarketStatus.OPEN if s == "ACTIVE" \
            else MarketStatus.OPEN_POST_ONLY if s == "POSTONLY" \
            else MarketStatus.CLOSED

    def _to_order_action(self, s: str) -> OrderAction:
        return OrderAction.BUY if s == "BID" or s == "BUY" else OrderAction.SELL

    def _get_error_code(self, e: Exception) -> MarketErrorCode:
        if isinstance(e, HttpResponseException) and isinstance(e.body, dict):
            error = e.body.get("error")
            error_code = e.body.get("error_code")
            if error_code == "ErrInsufficientFunds":
                return MarketErrorCode.INSUFFICIENT_FUNDS
            elif error_code == "ErrOrderCanceled" and error is not None and "post-only" in error:
                return MarketErrorCode.POST_ONLY_ORDER_CANCELLED
        return MarketErrorCode.UNKNOWN
