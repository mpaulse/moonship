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

from moonship.core import *
from moonship.client.web import *
from typing import Union

API_BASE_URL = "https://api.luno.com/api/1"
STREAM_BASE_URL = "wss://ws.luno.com/api/1/stream"
LUNO_MAX_DECIMALS = MAX_DECIMALS


def to_market_status(s: str) -> MarketStatus:
    return MarketStatus.OPEN if s == "ACTIVE" \
        else MarketStatus.OPEN_POST_ONLY if s == "POSTONLY" \
        else MarketStatus.CLOSED


def to_order_action(s: str) -> OrderAction:
    return OrderAction.BUY if s == "BID" or s == "BUY" else OrderAction.SELL


class LunoClient(AbstractWebClient):
    data_stream_seq_num = -1

    def __init__(self, market_name: str, app_config: Config):
        key_id = app_config.get("moonship.luno.key_id")
        if not isinstance(key_id, str):
            raise StartUpException("Luno API key ID not configured")
        key_secret = app_config.get("moonship.luno.key_secret")
        if not isinstance(key_secret, str):
            raise StartUpException("Luno API key secret not configured")
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(
                stream_url=f"{STREAM_BASE_URL}/{app_config.get(f'moonship.markets.{market_name}.symbol')}",
                auth=aiohttp.BasicAuth(key_id, key_secret)))

    async def get_ticker(self) -> Ticker:
        try:
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

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        request = {
            "pair": self.market.symbol
        }
        if isinstance(order, LimitOrder):
            order_type = "postorder"
            request["type"] = "BID" if order.action == OrderAction.BUY else "ASK"
            request["post_only"] = order.post_only
            request["price"] = to_amount_str(order.price, LUNO_MAX_DECIMALS)
            request["volume"] = to_amount_str(order.volume, LUNO_MAX_DECIMALS)
        else:
            order_type = "marketorder"
            request["type"] = order.action.name
            if order.is_base_amount:
                request["base_volume"] = to_amount_str(order.amount, LUNO_MAX_DECIMALS)
            else:
                request["counter_volume"] = to_amount_str(order.amount, LUNO_MAX_DECIMALS)
        try:
            async with self.http_session.post(f"{API_BASE_URL}/{order_type}", data=request) as rsp:
                await self.handle_error_response(rsp)
                order.id = (await rsp.json()).get("order_id")
                return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name) from e

    async def get_order(self, order_id: str) -> FullOrderDetails:
        try:
            async with self.http_session.get(f"{API_BASE_URL}/orders/{order_id}") as rsp:
                await self.handle_error_response(rsp)
                order_data = await rsp.json()
                state = order_data.get("state")
                base_amount_filled = to_amount(order_data.get("base"))
                return FullOrderDetails(
                    id=order_id,
                    action=to_order_action(order_data.get("type")),
                    base_amount_filled=base_amount_filled,
                    counter_amount_filled=to_amount(order_data.get("counter")),
                    limit_price=to_amount(order_data.get("limit_price")),
                    limit_volume=to_amount(order_data.get("limit_volume")),
                    status=OrderStatus.CANCELLED if order_data.get("expiration_timestamp") != 0 and state == "COMPLETE"
                    else OrderStatus.FILLED if state == "COMPLETE"
                    else OrderStatus.PARTIALLY_FILLED if base_amount_filled > 0
                    else OrderStatus.PENDING,
                    creation_timestamp=to_utc_timestamp(order_data.get("creation_timestamp")))
        except Exception as e:
            raise MarketException(f"Could not retrieve details of order {order_id}", self.market.name) from e

    async def cancel_order(self, order_id: str) -> bool:
        request = {
            "order_id": order_id
        }
        try:
            async with self.http_session.post(f"{API_BASE_URL}/stoporder", data=request) as rsp:
                if rsp.status == 404:
                    return False
                await self.handle_error_response(rsp)
                return bool((await rsp.json()).get("success"))
        except Exception as e:
            raise MarketException("Failed to cancel order", self.market.name) from e

    async def init_data_stream(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
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
                    status=to_market_status(msg.get("status")),
                    orders=orders))
        else:
            self.data_stream_seq_num += 1
            if self.data_stream_seq_num != int(msg.get("sequence")):
                self.data_stream_seq_num = -1
                raise MarketException("Data stream out of sequence", self.market.name)
            timestamp = to_utc_timestamp(msg.get("timestamp"))

            updates = msg.get("trade_updates")
            if updates is not None:
                trades = self._get_trades_from_stream(timestamp, updates)
                for trade in trades:
                    self.market.raise_event(trade)

            updates = msg.get("create_update")
            if updates is not None:
                self.market.raise_event(
                    OrderBookItemAddedEvent(
                        timestamp=timestamp,
                        order=self._get_order_from_stream(
                            to_order_action(updates.get("type")),
                            updates)))

            updates = msg.get("delete_update")
            if updates is not None:
                self.market.raise_event(
                    OrderBookItemRemovedEvent(
                        timestamp=timestamp,
                        order_id=updates.get("order_id")))

            updates = msg.get("status_update")
            if updates is not None:
                self.market.raise_event(
                    MarketStatusEvent(
                        timestamp=timestamp,
                        status=to_market_status(updates.get("status"))))

    def _get_orders_from_stream(self, action: OrderAction, order_data: list[dict], orders: list[LimitOrder]):
        for data in order_data:
            orders.append(self._get_order_from_stream(action, data))

    def _get_order_from_stream(self, action: OrderAction, order_data) -> LimitOrder:
        return LimitOrder(
            id=order_data.get("id") if "id" in order_data else order_data.get("order_id"),
            action=action,
            price=to_amount(order_data.get("price")),
            volume=to_amount(order_data.get("volume")))

    def _get_trades_from_stream(self, timestamp, trade_data: list[dict]) -> list[TradeEvent]:
        trades: list[TradeEvent] = []
        for data in trade_data:
            trades.append(
                TradeEvent(
                    timestamp=timestamp,
                    base_amount=to_amount(data.get("base")),
                    counter_amount=to_amount(data.get("counter")),
                    maker_order_id=data.get("maker_order_id"),
                    taker_order_id=data.get("taker_order_id")))
        return trades
