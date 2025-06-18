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

import aiohttp
import aiolimiter
import asyncio
import datetime

from moonship.core import *
from moonship.client.web import *
from typing import Any, Callable

API_BASE_URL = "https://api.luno.com/api/1"
EXCHANGE_API_BASE_URL = "https://api.luno.com/api/exchange/1"
EXCHANGE_API_V2_BASE_URL = "https://api.luno.com/api/exchange/2"
STREAM_BASE_URL = "wss://ws.luno.com/api/1/stream"
LUNO_MAX_DECIMALS = MAX_DECIMALS
GET_TRADES_MAX_LIMIT = 100


class LunoClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()
    limiter = aiolimiter.AsyncLimiter(300, 60)

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get(f"moonship.markets.{market_name}.api_key")
        if not isinstance(api_key, str):
            api_key = app_config.get("moonship.luno.api_key")
            if not isinstance(api_key, str):
                raise ConfigException("Luno API key not configured")
        api_secret = app_config.get(f"moonship.markets.{market_name}.api_secret")
        if not isinstance(api_secret, str):
            api_secret = app_config.get("moonship.luno.api_secret")
            if not isinstance(api_secret, str):
                raise ConfigException("Luno API secret not configured")
        self.data_stream_seq_num = -1
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(
                auth=aiohttp.BasicAuth(api_key, api_secret)),
            WebClientStreamParameters(
                url=f"{STREAM_BASE_URL}/{app_config.get(f'moonship.markets.{market_name}.symbol')}"))

    async def get_market_info(self, use_cached=True) -> MarketInfo:
        async with self.market_info_lock:
            if not use_cached or self.market.symbol not in self.market_info:
                try:
                    async with self.limiter:
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
            async with self.limiter:
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

    async def get_recent_trades(self, limit: int) -> list[Trade]:
        """Gets the recent trades in descending order."""
        try:
            return await self._get_trades()
        except Exception as e:
            raise MarketException(
                f"Could not retrieve recent trades for {self.market.symbol}", self.market.name) from e

    async def get_trades(self, handler: Callable[[list[Trade]], None], from_time: Timestamp) -> None:
        """Gets the trades in descending order per chunk."""
        from_time = from_time - datetime.timedelta(milliseconds=1) # Time is exclusive
        try:
            while True:
                trades = await self._get_trades(from_time)
                if len(trades) > 0:
                    handler(trades)
                    # Ensure trades with this same timestamp that are not in this chunk are also retrieved.
                    # Result in duplicates for the handler to take care of, but that is better than gaps.
                    from_time = trades[0].timestamp - datetime.timedelta(milliseconds=1)
                if len(trades) < GET_TRADES_MAX_LIMIT:
                    break
        except Exception as e:
            raise MarketException(
                f"Could not retrieve trades trades for {self.market.symbol}", self.market.name) from e

    async def _get_trades(self, from_timestamp_excl: Timestamp = None) -> list[Trade]:
        params = {
            "pair": self.market.symbol,
            "limit": GET_TRADES_MAX_LIMIT
        }
        if from_timestamp_excl is not None:
            params["since"] = int(from_timestamp_excl.timestamp() * 1000)
        async with self.limiter:
            async with self.http_session.get(f"{API_BASE_URL}/trades", params=params) as rsp:
                await self.handle_error_response(rsp)
                trades: list[Trade] = []
                trades_data = (await rsp.json()).get("trades")
                if isinstance(trades_data, list):
                    for data in trades_data:
                        if isinstance(data, dict):
                            trades.append(
                                Trade(
                                    id=str(data.get("sequence")),
                                    timestamp=to_utc_timestamp(data.get("timestamp")),
                                    symbol=self.market.symbol,
                                    price=to_amount(data.get("price")),
                                    quantity=to_amount(data.get("volume")),
                                    taker_action=OrderAction.BUY if data.get("is_buy") is True else OrderAction.SELL))
                return trades

    async def get_candles(self, period: CandlePeriod, from_time: Timestamp = None) -> list[Candle]:
        params = {
            "pair": self.market.symbol,
            "duration": period.value
        }
        if from_time is not None:
            params["since"] = int(from_time.timestamp() * 1000)
        else:
            params["since"] = utc_timestamp_now_msec() - (period.value * 1000 * 1000)  # Max. 1000 candles returned
        try:
            async with self.limiter:
                async with self.http_session.get(f"{EXCHANGE_API_BASE_URL}/candles", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    candles: list[Candle] = []
                    candle_data = (await rsp.json()).get("candles")
                    if isinstance(candle_data, list):
                        for data in candle_data:
                            if isinstance(data, dict):
                                candle_start = to_utc_timestamp(data.get("timestamp"))
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
                                        low=to_amount(data.get("low")),
                                        volume=to_amount(data.get("volume"))))
                    return candles
        except Exception as e:
            raise MarketException(f"Could not retrieve candles for {self.market.symbol}", self.market.name) from e

    async def place_order(self, order: MarketOrder | LimitOrder) -> str:
        request = {
            "pair": self.market.symbol
        }
        if isinstance(order, LimitOrder):
            order_type = "postorder"
            request["type"] = "BID" if order.action == OrderAction.BUY else "ASK"
            request["post_only"] = order.post_only
            request["price"] = to_amount_str(order.price, LUNO_MAX_DECIMALS)
            request["volume"] = to_amount_str(order.quantity, LUNO_MAX_DECIMALS)
            request["time_in_force"] = order.time_in_force.value
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
                    order_details = FullOrderDetails(
                        id=order_id,
                        symbol=order_data.get("pair"),
                        action=self._to_order_action(order_data.get("type")),
                        quantity=to_amount(order_data.get("limit_volume")),
                        quantity_filled=to_amount(order_data.get("base")),
                        quantity_filled_fee=to_amount(order_data.get("fee_base")),
                        quote_quantity_filled=to_amount(order_data.get("counter")),
                        quote_quantity_filled_fee=to_amount(order_data.get("fee_counter")),
                        limit_price=to_amount(order_data.get("limit_price")),
                        status=self._to_order_status(order_data),
                        creation_timestamp=to_utc_timestamp(order_data.get("creation_timestamp")))
                    try:
                        order_details.time_in_force = TimeInForce(order_data.get("time_in_force"))
                    except ValueError:
                        pass
                    return order_details
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

    async def get_open_orders(self) -> list[FullOrderDetails]:
        orders: list[FullOrderDetails] = []
        params = {
            "pair": self.market.symbol,
            "closed": "false"
        }
        try:
            async with self.limiter:
                async with self.http_session.get(f"{EXCHANGE_API_V2_BASE_URL}/listorders", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    orders_data = (await rsp.json()).get("orders")
                    if isinstance(orders_data, list):
                        for order_data in orders_data:
                            order_details = FullOrderDetails(
                                id=order_data.get("order_id"),
                                symbol=order_data.get("pair"),
                                action=self._to_order_action(order_data.get("side")),
                                quantity=to_amount(order_data.get("limit_volume")),
                                quantity_filled=to_amount(order_data.get("base")),
                                quantity_filled_fee=to_amount(order_data.get("fee_base")),
                                quote_quantity_filled=to_amount(order_data.get("counter")),
                                quote_quantity_filled_fee=to_amount(order_data.get("fee_counter")),
                                limit_price=to_amount(order_data.get("limit_price")),
                                status=self._to_order_status(order_data),
                                creation_timestamp=to_utc_timestamp(order_data.get("creation_timestamp")))
                            try:
                                order_details.time_in_force = TimeInForce(order_data.get("time_in_force"))
                            except ValueError:
                                pass
                            orders.append(order_details)
        except Exception as e:
            raise MarketException(f"Could not retrieve open orders", self.market.name) from e
        return orders

    async def get_asset_balances(self) -> tuple[AssetBalance, AssetBalance]:
        params = {
            "assets": [self.market.base_asset, self.market.quote_asset],
        }
        base_asset_balance: AssetBalance | None = None
        quote_asset_balance: AssetBalance | None = None
        try:
            async with self.limiter:
                async with self.http_session.get(f"{API_BASE_URL}/balance", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    balance_list = (await rsp.json()).get("balance")
                    if isinstance(balance_list, list):
                        for balance_data in balance_list:
                            if isinstance(balance_data, dict):
                                total = to_amount(balance_data.get("balance"))
                                available = total \
                                        - to_amount(balance_data.get("reserved"))  \
                                        - to_amount(balance_data.get("unconfirmed"))
                                balance = AssetBalance(asset=balance_data.get("asset"), available=available, total=total)
                                if balance.asset == self.market.base_asset:
                                    base_asset_balance = balance
                                elif balance.asset == self.market.quote_asset:
                                    quote_asset_balance = balance
        except Exception as e:
            raise MarketException("Could not retrieve asset balances", self.market.name) from e
        if base_asset_balance is None:
            raise MarketException(f"{self.market.base_asset} balance not returned by exchange", self.market.name)
        if quote_asset_balance is None:
            raise MarketException(f"{self.market.quote_asset} balance not returned by exchange", self.market.name)
        return base_asset_balance, quote_asset_balance

    async def on_after_data_stream_connect(
            self,
            websocket: aiohttp.ClientWebSocketResponse,
            params: WebClientStreamParameters) -> None:
        await websocket.send_json({
            "api_key_id": self.session_params.auth.login,
            "api_key_secret": self.session_params.auth.password
        })

    async def on_data_stream_msg(self, msg: Any, websocket: aiohttp.ClientWebSocketResponse) -> None:
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
                                id=str(data.get("sequence")),
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

    def _to_order_status(self, order_data: dict[str, Any]) -> OrderStatus:
        exp_timestamp = order_data.get("expiration_timestamp")
        state = order_data.get("state")
        if state is None:
            state = order_data.get("status")
        quantity = to_amount(order_data.get("limit_volume"))
        quantity_filled = to_amount(order_data.get("base"))
        if state == "COMPLETE":
            if exp_timestamp != 0 or (quantity != 0 and quantity != quantity_filled):
                return OrderStatus.CANCELLED
            else:
                return OrderStatus.FILLED
        elif quantity_filled > 0:
            return OrderStatus.PARTIALLY_FILLED
        else:
            return OrderStatus.PENDING

    def _get_error_code(self, e: Exception) -> MarketErrorCode:
        if isinstance(e, HttpResponseException) and isinstance(e.body, dict):
            error = e.body.get("error")
            error_code = e.body.get("error_code")
            if error_code == "ErrInsufficientFunds":
                return MarketErrorCode.INSUFFICIENT_FUNDS
            elif error_code == "ErrOrderCanceled" and error is not None and "post-only" in error:
                return MarketErrorCode.POST_ONLY_ORDER_CANCELLED
        return MarketErrorCode.UNKNOWN
