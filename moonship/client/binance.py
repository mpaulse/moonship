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
import asyncio
import hmac
import hashlib
import urllib.parse

from datetime import timezone
from moonship.core import *
from moonship.client.web import *
from typing import Union

API_BASE_URL = "https://api.binance.com/api/v3"
STREAM_BASE_URL = "wss://stream.binance.com:9443/stream"


class BinanceClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get("moonship.binance.api_key")
        if not isinstance(api_key, str):
            raise StartUpException("Binance API key not configured")
        self.secret_key = app_config.get("moonship.binance.secret_key")
        if not isinstance(self.secret_key, str):
            raise StartUpException("Binance secret key not configured")
        self.last_order_book_update_id = -1
        self.order_book_event_buf: list[dict] = []
        symbol = app_config.get(f"moonship.markets.{market_name}.symbol").lower()
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(
                stream_url=f"{STREAM_BASE_URL}?streams={symbol}@trade/{symbol}@depth",
                headers={"X-MBX-APIKEY": api_key}))

    async def get_market_info(self, use_cached=True) -> MarketInfo:
        async with self.market_info_lock:
            if not use_cached or self.market.symbol not in self.market_info:
                try:
                    async with self.http_session.get(f"{API_BASE_URL}/exchangeInfo") as rsp:
                        await self.handle_error_response(rsp)
                        markets = (await rsp.json()).get("symbols")
                        for market_data in markets:
                            min_quantity = Amount(1)
                            filters = market_data.get("filters")
                            for filter in filters:
                                if filter.get("filterType") == "LOT_SIZE":
                                    min_quantity = Amount(filter.get("minQty"))
                                    break
                            info = MarketInfo(
                                symbol=market_data.get("symbol"),
                                base_asset=market_data.get("baseAsset"),
                                base_asset_precision=market_data.get("baseAssetPrecision"),
                                base_asset_min_quantity=min_quantity,
                                quote_asset=market_data.get("quoteAsset"),
                                quote_asset_precision=market_data.get("quoteAssetPrecision"),
                                status=MarketStatus.OPEN if market_data.get("status") == "TRADING"
                                else MarketStatus.CLOSED)
                            self.market_info[info.symbol] = info
                except Exception as e:
                    raise MarketException(
                        f"Could not retrieve market info for {self.market.symbol}", self.market.name) from e
        return self.market_info[self.market.symbol]

    async def get_ticker(self) -> Ticker:
        try:
            price_ticker, order_book_ticker = \
                await asyncio.gather(self._get_price_ticker(), self._get_order_book_ticker())
            return Ticker(
                timestamp=Timestamp.now(tz=timezone.utc),
                symbol=price_ticker.get("symbol"),
                bid_price=to_amount(order_book_ticker.get("bidPrice")),
                ask_price=to_amount(order_book_ticker.get("askPrice")),
                current_price=to_amount(price_ticker.get("price")))
        except Exception as e:
            raise MarketException(f"Could not retrieve ticker for {self.market.symbol}", self.market.name) from e

    async def _get_price_ticker(self) -> dict:
        async with self.http_session.get(
                f"{API_BASE_URL}/ticker/price",
                params={"symbol": self.market.symbol}) as rsp:
            await self.handle_error_response(rsp)
            return await rsp.json()

    async def _get_order_book_ticker(self) -> dict:
        async with self.http_session.get(
                f"{API_BASE_URL}/ticker/bookTicker",
                params={"symbol": self.market.symbol}) as rsp:
            await self.handle_error_response(rsp)
            return await rsp.json()

    async def get_recent_trades(self, limit) -> list[Trade]:
        params = {
            "symbol": self.market.symbol,
            "limit": limit
        }
        try:
            async with self.http_session.get(f"{API_BASE_URL}/trades", params=params) as rsp:
                await self.handle_error_response(rsp)
                trades: list[Trade] = []
                trades_data = await rsp.json()
                if isinstance(trades_data, list):
                    for data in trades_data:
                        trades.append(
                            Trade(
                                timestamp=to_utc_timestamp(data.get("time")),
                                symbol=self.market.symbol,
                                price=to_amount(data.get("price")),
                                quantity=to_amount(data.get("qty"))))
                return trades
        except Exception as e:
            raise MarketException(f"Could not recent trades for {self.market.symbol}", self.market.name) from e

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        request = {
            "symbol": self.market.symbol,
            "side": order.action.name,
            "type": "MARKET" if isinstance(order, MarketOrder)
            else "LIMIT_MAKER" if order.post_only
            else "LIMIT",
            "newOrderRespType": "ACK"
        }
        if isinstance(order, LimitOrder):
            request["price"] = to_amount_str(order.price)
            request["quantity"] = to_amount_str(order.quantity)
        else:
            if order.is_base_quantity:
                request["quantity"] = to_amount_str(order.quantity)
            else:
                request["quoteOrderQty"] = to_amount_str(order.quantity)
        request = self._url_encode_and_sign(request)
        try:
            async with self.http_session.post(
                    f"{API_BASE_URL}/order",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=request) as rsp:
                await self.handle_error_response(rsp)
                order.id = (await rsp.json()).get("orderId")
                return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name) from e

    async def get_order(self, order_id: str) -> FullOrderDetails:
        params = self._url_encode_and_sign({
            "symbol": self.market.symbol,
            "orderId": order_id
        })
        try:
            async with self.http_session.get(f"{API_BASE_URL}/order", params=params) as rsp:
                await self.handle_error_response(rsp)
                order_data = await rsp.json()
                status = order_data.get("status")
                return FullOrderDetails(
                    id=order_id,
                    symbol=order_data.get("symbol"),
                    action=OrderAction[order_data.get("side")],
                    quantity_filled=to_amount(order_data.get("executedQty")),
                    quote_quantity_filled=to_amount(order_data.get("cummulativeQuoteQty")),
                    limit_price=to_amount(order_data.get("price")),
                    limit_quantity=to_amount(order_data.get("origQty")),
                    status=OrderStatus.PENDING if status == "NEW"
                    else OrderStatus.CANCELLATION_PENDING if status == "PENDING_CANCEL"
                    else OrderStatus.CANCELLED if status == "CANCELED"
                    else OrderStatus[status],
                    creation_timestamp=to_utc_timestamp(order_data.get("time")))
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market.name) from e

    async def cancel_order(self, order_id: str) -> bool:
        request = self._url_encode_and_sign({
            "symbol": self.market.symbol,
            "orderId": order_id,
            "timestamp": utc_timestamp_now_msec()
        })
        try:
            async with self.http_session.delete(
                    f"{API_BASE_URL}/order",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=request) as rsp:
                if rsp.status == 404:
                    return False
                await self.handle_error_response(rsp)
                return (await rsp.json()).get("status") == "CANCELED"
        except Exception as e:
            raise MarketException("Failed to cancel order", self.market.name) from e

    def _url_encode_and_sign(self, data: dict) -> str:
        params = urllib.parse.urlencode(data, encoding="utf-8")
        signature = hmac.new(bytes(self.secret_key), bytes(params), hashlib.sha256).hexdigest()
        return f"{params}&signature={signature}"

    async def on_before_data_stream_connect(self) -> None:
        self.last_order_book_update_id = -1
        self.order_book_event_buf.clear()

    def _get_orders_from_stream(
            self,
            action: OrderAction,
            order_book_entries: list[[str, str]],
            orders: list[LimitOrder]
    ):
        for entry in order_book_entries:
            orders.append(self._get_order_from_stream(action, entry))

    def _get_order_from_stream(self, action: OrderAction, order_book_entry: [str, str]):
        # The Binance order book does not contain individual order granularity,
        # so create mock orders with the order ID = action@price.
        price = order_book_entry[0]
        quantity = to_amount(order_book_entry[1])
        return LimitOrder(id=f"{action.name}@{price}", action=action, price=to_amount(price), quantity=quantity)

    async def on_data_stream_msg(self, msg: any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        if isinstance(msg, dict):
            data = msg.get("data")
            if isinstance(data, dict):
                event = data.get("e")
                if event == "trade":
                    self._on_trade_stream_event(data)
                elif event == "depthUpdate":
                    self._on_order_book_stream_event(data)

    def _on_trade_stream_event(self, event: dict) -> None:
        if isinstance(event, dict):
            buyer_order_id = event.get("b")
            buyer_is_maker = event.get("m")
            seller_order_id = event.get("a")
            quantity = to_amount(event.get("q"))
            self.market.raise_event(
                TradeEvent(
                    timestamp=to_utc_timestamp(event.get("E")),
                    trade=Trade(
                        timestamp=to_utc_timestamp(event.get("T")),
                        symbol=event.get("s"),
                        quantity=quantity,
                        price=to_amount(event.get("p"))),
                    maker_order_id=buyer_order_id if buyer_is_maker else seller_order_id,
                    taker_order_id=buyer_order_id if not buyer_is_maker else seller_order_id))

    def _on_order_book_stream_event(self, event: dict) -> None:
        if self.last_order_book_update_id < 0:
            if len(self.order_book_event_buf) == 0:
                asyncio.create_task(self._init_order_book())
            self.order_book_event_buf.append(event)
        else:
            first_update_id = event.get("U")
            last_update_id = event.get("u")
            if first_update_id <= self.last_order_book_update_id + 1 <= last_update_id:
                timestamp = to_utc_timestamp(event.get("E"))
                self._raise_order_book_update_events(OrderAction.BUY, event.get("b"), timestamp)  # Bids
                self._raise_order_book_update_events(OrderAction.SELL, event.get("a"), timestamp)  # Asks
                self.last_order_book_update_id = last_update_id

    async def _init_order_book(self) -> None:
        params = {
            "symbol": self.market.symbol,
            "limit": 100
        }
        try:
            async with self.http_session.get(f"{API_BASE_URL}/depth", params=params) as rsp:
                await self.handle_error_response(rsp)
                data = await rsp.json()
                orders: list[LimitOrder] = []
                self.last_order_book_update_id = int(data.get("lastUpdateId"))
                self._get_orders_from_stream(OrderAction.BUY, data.get("bids"), orders)
                self._get_orders_from_stream(OrderAction.SELL, data.get("asks"), orders)
                self.market.raise_event(
                    OrderBookInitEvent(
                        timestamp=Timestamp.now(tz=timezone.utc),
                        orders=orders))
                for event in self.order_book_event_buf:
                    self._on_order_book_stream_event(event)
                self.order_book_event_buf.clear()
        except Exception as e:
            raise MarketException(f"Could not retrieve order book information", self.market.name) from e

    def _raise_order_book_update_events(
            self,
            action: OrderAction,
            order_book_entries: list[[str, str]],
            timestamp: Timestamp):
        if isinstance(order_book_entries, list):
            for entry in order_book_entries:
                if isinstance(entry, list):
                    order = self._get_order_from_stream(action, entry)
                    if order.quantity > 0:
                        self.market.raise_event(OrderBookItemAddedEvent(timestamp=timestamp, order=order))
                    else:
                        self.market.raise_event(OrderBookItemRemovedEvent(timestamp=timestamp, order_id=order.id))
