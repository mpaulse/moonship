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
import aiohttp
import asyncio
import logging

from moonship.core import *
from typing import Optional, Union

API_BASE_URL = "https://api.luno.com/api/1"
FEED_BASE_URL = "wss://ws.luno.com/api/1/stream"

logger = logging.getLogger(__name__)


def to_market_status(s: str) -> MarketStatus:
    return MarketStatus.OPEN if s == "ACTIVE" \
        else MarketStatus.OPEN_POST_ONLY if s == "POSTONLY" \
        else MarketStatus.CLOSED


def to_order_action(s: str) -> OrderAction:
    return OrderAction.BUY if s == "BID" or s == "BUY" else OrderAction.SELL


def to_amount_str(a: Amount) -> str:
    return str(round(a, 6))  # Get an API error if there are too many decimal places


class AbstractLunoClient(abc.ABC):
    http_session: Optional[aiohttp.ClientSession]

    def __init__(self, market_name: str, symbol: str, app_config: Config):
        self.market_name = market_name
        self.symbol = symbol
        self.key_id = app_config.get("moonship.luno.key_id")
        if not isinstance(self.key_id, str):
            raise StartUpException("Luno API key ID not configured")
        self.key_secret = app_config.get("moonship.luno.key_secret")
        if not isinstance(self.key_secret, str):
            raise StartUpException("Luno API key secret not configured")

    @abc.abstractmethod
    async def connect(self):
        pass

    async def close(self):
        if self.http_session is not None:
            s = self.http_session
            self.http_session = None
            await s.close()

    def is_closed(self):
        return self.http_session is None or self.http_session.closed


class LunoClient(AbstractLunoClient, MarketClient):

    async def connect(self):
        self.http_session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(self.key_id, self.key_secret),
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=15))

    async def get_ticker(self) -> Ticker:
        try:
            async with self.http_session.get(f"{API_BASE_URL}/ticker", params={"pair": self.symbol}) as rsp:
                ticker = await rsp.json()
                return Ticker(
                    timestamp=to_utc_timestamp(ticker.get("timestamp")),
                    symbol=ticker.get("pair"),
                    ask_price=to_amount(ticker.get("bid")),
                    bid_price=to_amount(ticker.get("ask")),
                    current_price=to_amount(ticker.get("last_trade")),
                    status=to_market_status(ticker.get("status")))
        except Exception as e:
            raise MarketException(f"Could not retrieve ticker for {self.symbol}", self.market_name) from e

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        request = {
            "pair": self.symbol
        }
        if isinstance(order, LimitOrder):
            order_type = "postorder"
            request["type"] = "BID" if order.action == OrderAction.BUY else "ASK"
            request["post_only"] = True
            request["price"] = order.price
            request["volume"] = order.volume
        else:
            order_type = "marketorder"
            request["type"] = order.action.name
            if order.action == OrderAction.BUY:
                request["counter_volume"] = order.amount
            else:
                request["base_volume"] = order.amount
        try:
            async with self.http_session.post(f"{API_BASE_URL}/{order_type}", data=request) as rsp:
                order.id = (await rsp.json()).get("order_id")
                return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market_name) from e

    async def get_order(self, order_id: str) -> FullOrderDetails:
        try:
            async with self.http_session.get(f"{API_BASE_URL}/orders/{order_id}") as rsp:
                order_data = await rsp.json()
                state = order_data.get("state")
                return FullOrderDetails(
                    id=order_id,
                    action=to_order_action(order_data.get("type")),
                    base_amount_filled=to_amount(order_data("base")),
                    counter_amount_filled=to_amount(order_data("counter")),
                    limit_price=to_amount(order_data.get("limit_price")),
                    limit_volume=to_amount(order_data.get("limit_volume")),
                    status=
                    OrderStatus.CANCELLED if order_data.get("expiration_timestamp") is not None and state == "COMPLETE"
                    else OrderStatus.COMPLETE if state == "COMPLETE"
                    else OrderStatus.PENDING,
                    created_timestamp=to_utc_timestamp(order_data.get("creation_timestamp")))
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market_name) from e

    async def cancel_order(self, order_id: str) -> bool:
        request = {
            "order_id": order_id
        }
        try:
            async with self.http_session.post(f"{API_BASE_URL}/stoporder", data=request) as rsp:
                return bool((await rsp.json()).get("success"))
        except Exception as e:
            raise MarketException("Failed to cancel order", self.market_name) from e


class LunoFeed(AbstractLunoClient, MarketFeed):

    def __init__(self, market_name: str, symbol: str, app_config: Config):
        super().__init__(market_name, symbol, app_config)

    async def connect(self):
        self.http_session = aiohttp.ClientSession()
        asyncio.create_task(self.process_feed())

    async def process_feed(self):
        while not self.is_closed():
            try:
                seq_no = -1
                async with self.http_session.ws_connect(f"{FEED_BASE_URL}/{self.symbol}") as websocket:
                    await websocket.send_json({
                        "api_key_id": self.key_id,
                        "api_key_secret": self.key_secret
                    })
                    while not self.is_closed() and not websocket.closed:
                        msg = await websocket.receive_json()
                        if seq_no < 0:
                            seq_no = int(msg.get("sequence"))
                            orders: list[LimitOrder] = []
                            self.get_orders(OrderAction.BUY, msg.get("bids"), orders)
                            self.get_orders(OrderAction.SELL, msg.get("asks"), orders)
                            self.raise_event(
                                OrderBookInitEvent(
                                    timestamp=to_utc_timestamp(msg.get("timestamp")),
                                    market_name=self.market_name,
                                    symbol=self.symbol,
                                    status=to_market_status(msg.get("status")),
                                    orders=orders))
                        elif isinstance(msg, dict):
                            seq_no += 1
                            if seq_no != int(msg.get("sequence")):
                                raise MarketException("Feed out of sequence", self.market_name)
                            timestamp = to_utc_timestamp(msg.get("timestamp"))
                            updates = msg.get("trade_updates")
                            if updates is not None:
                                trades = self.get_trades(timestamp, updates)
                                for trade in trades:
                                    self.raise_event(trade)
                            updates = msg.get("create_update")
                            if updates is not None:
                                self.raise_event(
                                    OrderBookItemAddedEvent(
                                        timestamp=timestamp,
                                        market_name=self.market_name,
                                        symbol=self.symbol,
                                        order=self.get_order(to_order_action(updates.get("type")), updates)))
                            updates = msg.get("delete_update")
                            if updates is not None:
                                self.raise_event(
                                    OrderBookItemRemovedEvent(
                                        timestamp=timestamp,
                                        market_name=self.market_name,
                                        symbol=self.symbol,
                                        order_id=updates.get("order_id")))
                            updates = msg.get("status_update")
                            if updates is not None:
                                self.raise_event(
                                    MarketStatusEvent(
                                        timestamp=timestamp,
                                        market_name=self.market_name,
                                        symbol=self.symbol,
                                        status=to_market_status(updates.get("status"))))
            except Exception as e:
                if not self.is_closed():
                    logger.exception("Feed error", e)
                    await asyncio.sleep(1)

    def get_orders(self, action: OrderAction, order_data: list[dict], orders: list[LimitOrder]):
        for data in order_data:
            orders.append(self.get_order(action, data))

    def get_order(self, action: OrderAction, order_data) -> LimitOrder:
        return LimitOrder(
            id=order_data.get("id") if "id" in order_data else order_data.get("order_id"),
            action=action,
            price=to_amount(order_data.get("price")),
            volume=to_amount(order_data.get("volume")))

    def get_trades(self, timestamp, trade_data: list[dict]) -> list[TradeEvent]:
        trades: list[TradeEvent] = []
        for data in trade_data:
            trades.append(
                TradeEvent(
                    timestamp=timestamp,
                    market_name=self.market_name,
                    symbol=self.symbol,
                    base_amount=to_amount(data.get("base")),
                    counter_amount=to_amount(data.get("counter")),
                    maker_order_id=data.get("maker_order_id"),
                    taker_order_id=data.get("taker_order_id")))
        return trades
