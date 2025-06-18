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
import hmac
import hashlib
import urllib.parse

from datetime import timezone
from moonship.core import *
from moonship.client.web import *
from typing import Any, Callable

API_BASE_URL = "https://api.binance.com/api/v3"
STREAM_BASE_URL = "wss://stream.binance.com:9443/stream"
GET_TRADES_MAX_LIMIT = 1000


class BinanceClient(AbstractWebClient):
    market_info: dict[str, MarketInfo] = {}
    market_info_lock = asyncio.Lock()
    request_weight_limiter = aiolimiter.AsyncLimiter(6000, 60)
    order_limiter = aiolimiter.AsyncLimiter(100, 10)

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get(f"moonship.markets.{market_name}.api_key")
        if not isinstance(api_key, str):
            api_key = app_config.get("moonship.binance.api_key")
            if not isinstance(api_key, str):
                raise ConfigException("Binance API key not configured")
        self.api_secret = app_config.get(f"moonship.markets.{market_name}.api_secret")
        if not isinstance(self.api_secret, str):
            self.api_secret = app_config.get("moonship.binance.api_secret")
            if not isinstance(self.api_secret, str):
                raise ConfigException("Binance API secret not configured")
        self.last_order_book_update_id = -1
        self.order_book_event_buf: list[dict] = []
        self.order_details_cache: dict[str, FullOrderDetails] = {}
        symbol = app_config.get(f"moonship.markets.{market_name}.symbol").lower()
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(
                headers={"X-MBX-APIKEY": api_key}),
            WebClientStreamParameters(
                url=f"{STREAM_BASE_URL}?streams={symbol}@trade/{symbol}@depth"))

    async def get_market_info(self, use_cached=True) -> MarketInfo:
        async with self.market_info_lock:
            if not use_cached or self.market.symbol not in self.market_info:
                try:
                    async with self.request_weight_limiter:
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
        async with self.request_weight_limiter:
            async with self.http_session.get(
                    f"{API_BASE_URL}/ticker/price",
                    params={"symbol": self.market.symbol}) as rsp:
                await self.handle_error_response(rsp)
                return await rsp.json()

    async def _get_order_book_ticker(self) -> dict:
        async with self.request_weight_limiter:
            async with self.http_session.get(
                    f"{API_BASE_URL}/ticker/bookTicker",
                    params={"symbol": self.market.symbol}) as rsp:
                await self.handle_error_response(rsp)
                return await rsp.json()

    async def get_recent_trades(self, limit) -> list[Trade]:
        """Gets the recent trades in ascending order."""
        try:
            return await self._get_trades(f"{API_BASE_URL}/trades", { "limit": limit })
        except Exception as e:
            raise MarketException(
                f"Could not retrieve recent trades for {self.market.symbol}", self.market.name) from e

    async def get_trades(self, handler: Callable[[list[Trade]], None], from_time: Timestamp) -> None:
        """Gets the trades in ascending order."""
        try:
            from_id = None
            while True:
                params = { "fromId": from_id } if from_id is not None \
                    else { "startTime" : int(from_time.timestamp() * 1000) }
                params["limit"] = GET_TRADES_MAX_LIMIT
                trades = await self._get_trades(f"{API_BASE_URL}/aggTrades", params)
                if len(trades) == 0:
                    break
                handler(trades)
                from_id = int(trades[-1].id)
        except Exception as e:
            raise MarketException(
                f"Could not retrieve trades for {self.market.symbol}", self.market.name) from e

    async def _get_trades(self, url: str, params: dict[str, Any]) -> list[Trade]:
        params["symbol"] = self.market.symbol
        async with self.request_weight_limiter:
            async with self.http_session.get(url, params=params) as rsp:
                await self.handle_error_response(rsp)
                trades: list[Trade] = []
                trades_data = await rsp.json()
                if isinstance(trades_data, list):
                    for data in trades_data:
                        if isinstance(data, dict):
                            id = data.get("id")
                            if id is None:
                                id = data.get("a") # Aggregate trade ID

                            timestamp = data.get("time")
                            if timestamp is None:
                                timestamp = data.get("T")

                            price = data.get("price")
                            if price is None:
                                price = data.get("p")

                            quantity = data.get("qty")
                            if quantity is None:
                                quantity = data.get("q")

                            is_buyer_maker = data.get("isBuyerMaker")
                            if is_buyer_maker is None:
                                is_buyer_maker = data.get("m")

                            trades.append(
                                Trade(
                                    id=str(id),
                                    timestamp=to_utc_timestamp(timestamp),
                                    symbol=self.market.symbol,
                                    price=to_amount(price),
                                    quantity=to_amount(quantity),
                                    taker_action=OrderAction.SELL if is_buyer_maker is True else OrderAction.BUY))
                return trades

    async def get_candles(self, period: CandlePeriod, from_time: Timestamp = None) -> list[Candle]:
        match period:
            case CandlePeriod.ONE_MIN:
                interval = "1m"
            case CandlePeriod.FIVE_MIN:
                interval = "5m"
            case CandlePeriod.FIFTEEN_MIN:
                interval = "15m"
            case CandlePeriod.THIRTY_MIN:
                interval = "30m"
            case CandlePeriod.HOUR:
                interval = "1h"
            case CandlePeriod.DAY:
                interval = "1d"
            case _:
                interval = "5m"
        params = {
            "symbol": self.market.symbol,
            "interval": interval
        }
        if from_time is not None:
            params["startTime"] = int(from_time.timestamp()) * 1000
        try:
            async with self.request_weight_limiter:
                async with self.http_session.get(f"{API_BASE_URL}/klines", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    candles: list[Candle] = []
                    candle_data = await rsp.json()
                    if isinstance(candle_data, list):
                        for data in candle_data:
                            if isinstance(data, list):
                                if len(data) >= 7:
                                    candles.append(
                                        Candle(
                                            symbol=self.market.symbol,
                                            period=period,
                                            start_time=to_utc_timestamp(data[0]),
                                            open=to_amount(data[1]),
                                            high=to_amount(data[2]),
                                            low=to_amount(data[3]),
                                            close=to_amount(data[4]),
                                            volume=to_amount(data[5]),
                                            end_time=to_utc_timestamp(data[6]),
                                            buy_volume=to_amount(data[9])))
                    return candles
        except Exception as e:
            raise MarketException(f"Could not retrieve candles for {self.market.symbol}", self.market.name) from e

    async def place_order(self, order: MarketOrder | LimitOrder) -> str:
        request = {
            "symbol": self.market.symbol,
            "side": order.action.name,
            "type": "MARKET" if isinstance(order, MarketOrder)
            else "LIMIT_MAKER" if order.post_only
            else "LIMIT",
            "newOrderRespType": "ACK",
            "timestamp": utc_timestamp_now_msec()
        }
        if isinstance(order, LimitOrder):
            request["price"] = to_amount_str(order.price)
            request["quantity"] = to_amount_str(order.quantity)
            request["timeInForce"] = order.time_in_force.value
        else:
            if order.is_base_quantity:
                request["quantity"] = to_amount_str(order.quantity)
            else:
                request["quoteOrderQty"] = to_amount_str(order.quantity)
        request = self._url_encode_and_sign(request)
        try:
            async with self.request_weight_limiter:
                async with self.order_limiter:
                    async with self.http_session.post(
                            f"{API_BASE_URL}/order",
                            headers={"Content-Type": "application/x-www-form-urlencoded"},
                            data=request) as rsp:
                        await self.handle_error_response(rsp)
                        order.id = (await rsp.json()).get("orderId")
                        return order.id
        except Exception as e:
            raise MarketException("Failed to place order", self.market.name, self._get_error_code(e)) from e

    async def get_order(self, order_id: str) -> FullOrderDetails:
        order_details = self.order_details_cache.get(order_id)
        if order_details is not None:
            return order_details
        params = self._url_encode_and_sign({
            "symbol": self.market.symbol,
            "orderId": order_id,
            "timestamp": utc_timestamp_now_msec()
        })
        try:
            async with self.request_weight_limiter:
                async with self.http_session.get(f"{API_BASE_URL}/order", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    return self._get_order_details(await rsp.json())
        except Exception as e:
            error_code = self._get_error_code(e)
            if error_code == MarketErrorCode.NO_SUCH_ORDER:
                await asyncio.sleep(1)
                order_details = self.order_details_cache.get(order_id)
                if order_details is not None:
                    return order_details
            raise MarketException(
                f"Could not retrieve details of order {order_id}",
                self.market.name,
                error_code) from e

    def _get_order_details(self, order_data) -> FullOrderDetails:
        status = order_data.get("status")
        order_details = FullOrderDetails(
            id=order_data.get("orderId"),
            symbol=order_data.get("symbol"),
            action=OrderAction[order_data.get("side")],
            quantity=to_amount(order_data.get("origQty")),
            quote_quantity=to_amount(order_data.get("origQuoteOrderQty")),
            quantity_filled=to_amount(order_data.get("executedQty")),
            quote_quantity_filled=to_amount(order_data.get("cummulativeQuoteQty")),
            limit_price=to_amount(order_data.get("price")),
            status=OrderStatus.PENDING if status == "NEW"
            else OrderStatus.CANCELLATION_PENDING if status == "PENDING_CANCEL"
            else OrderStatus.CANCELLED if status == "CANCELED"
            else OrderStatus[status],
            creation_timestamp=to_utc_timestamp(order_data.get("time")))
        try:
            order_details.time_in_force = TimeInForce(order_data.get("timeInForce"))
        except ValueError:
            pass
        return order_details

    async def cancel_order(self, order_id: str) -> bool:
        request = self._url_encode_and_sign({
            "symbol": self.market.symbol,
            "orderId": order_id,
            "timestamp": utc_timestamp_now_msec()
        })
        try:
            async with self.request_weight_limiter:
                async with self.http_session.delete(
                        f"{API_BASE_URL}/order",
                        headers={"Content-Type": "application/x-www-form-urlencoded"},
                        data=request) as rsp:
                    if rsp.status == 404:
                        return False
                    await self.handle_error_response(rsp)
                    return (await rsp.json()).get("status") == "CANCELED"
        except Exception as e:
            error_code = self._get_error_code(e)
            if error_code == MarketErrorCode.NO_SUCH_ORDER:
                return True
            raise MarketException("Failed to cancel order", self.market.name, error_code) from e

    async def get_open_orders(self) -> list[FullOrderDetails]:
        params = self._url_encode_and_sign({
            "symbol": self.market.symbol,
            "timestamp": utc_timestamp_now_msec()
        })
        orders: list[FullOrderDetails] = []
        try:
            async with self.request_weight_limiter:
                async with self.http_session.get(f"{API_BASE_URL}/openOrders", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    order_list = await rsp.json()
                    if isinstance(order_list, list):
                        for order_data in order_list:
                            if isinstance(order_data, dict):
                                orders.append(self._get_order_details(order_data))
        except Exception as e:
            raise MarketException("Could not retrieve open orders", self.market.name) from e
        return orders

    async def get_asset_balances(self) -> tuple[AssetBalance, AssetBalance]:
        base_asset_balance: AssetBalance | None = None
        quote_asset_balance: AssetBalance | None = None
        params = self._url_encode_and_sign({
            "omitZeroBalances": "false",
            "timestamp": utc_timestamp_now_msec()
        })
        try:
            async with self.request_weight_limiter:
                async with self.http_session.get(f"{API_BASE_URL}/account", params=params) as rsp:
                    await self.handle_error_response(rsp)
                    account_data = await rsp.json()
                    if isinstance(account_data, dict):
                        balance_list = account_data.get("balances")
                        if isinstance(balance_list, list):
                            for data in balance_list:
                                if isinstance(data, dict):
                                    asset = data.get("asset")
                                    available = to_amount(data.get("available"))
                                    if asset == self.market.base_asset:
                                        base_asset_balance = \
                                            AssetBalance(
                                                asset=asset,
                                                total=available + to_amount(data.get("locked")),
                                                available=available)
                                    elif asset == self.market.quote_asset:
                                        quote_asset_balance = \
                                            AssetBalance(
                                                asset=asset,
                                                total=available + to_amount(data.get("locked")),
                                                available=available)
        except Exception as e:
            raise MarketException("Could not retrieve asset balances", self.market.name) from e
        if base_asset_balance is None:
            raise MarketException(f"{self.market.base_asset} balance not returned by exchange", self.market.name)
        if quote_asset_balance is None:
            raise MarketException(f"{self.market.quote_asset} balance not returned by exchange", self.market.name)
        return base_asset_balance, quote_asset_balance

    def _url_encode_and_sign(self, data: dict) -> str:
        params = urllib.parse.urlencode(data, encoding="utf-8")
        signature = hmac.new(
            bytes(self.api_secret, encoding="utf-8"),
            bytes(params, encoding="utf-8"),
            hashlib.sha256).hexdigest()
        return f"{params}&signature={signature}"

    async def on_before_data_stream_connect(self, params: WebClientStreamParameters) -> None:
        self.last_order_book_update_id = -1
        self.order_book_event_buf.clear()
        async with self.http_session.post(f"{API_BASE_URL}/userDataStream") as rsp:
            await self.handle_error_response(rsp)
            listen_key = (await rsp.json()).get("listenKey")
            params.url += f"/{listen_key}"

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

    async def on_data_stream_msg(self, msg: Any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        if isinstance(msg, dict):
            data = msg.get("data")
            if isinstance(data, dict):
                event = data.get("e")
                if event == "executionReport":
                    self._on_order_update_stream_event(data)
                elif event == "trade":
                    self._on_trade_stream_event(data)
                elif event == "depthUpdate":
                    self._on_order_book_stream_event(data)

    def _on_order_update_stream_event(self, event: dict) -> None:
        if isinstance(event, dict):
            status = event.get("X")
            order_details = FullOrderDetails(
                id=str(event.get("i")),
                symbol=event.get("s"),
                action=OrderAction[event.get("S")],
                quantity=to_amount(event.get("q")),
                quote_quantity=to_amount(event.get("Q")),
                quantity_filled=to_amount(event.get("z")),
                quote_quantity_filled=to_amount(event.get("Z")),
                limit_price=to_amount(event.get("p")),
                status=OrderStatus.PENDING if status == "NEW"
                else OrderStatus.CANCELLATION_PENDING if status == "PENDING_CANCEL"
                else OrderStatus.CANCELLED if status == "CANCELED"
                else OrderStatus[status],
                creation_timestamp=to_utc_timestamp(event.get("O")))
            fee = to_amount(event.get("n"))
            fee_asset = event.get("N")
            if fee > 0 and fee_asset is not None:
                if fee_asset == self.market.base_asset:
                    order_details.quantity_filled_fee = fee
                else:
                    order_details.quote_quantity_filled_fee = fee
            try:
                order_details.time_in_force = TimeInForce(event.get("f"))
            except ValueError:
                pass
            self.order_details_cache[order_details.id] = order_details

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
                        id=str(event.get("t")),
                        timestamp=to_utc_timestamp(event.get("T")),
                        symbol=event.get("s"),
                        quantity=quantity,
                        price=to_amount(event.get("p")),
                        taker_action=OrderAction.SELL if buyer_is_maker else OrderAction.BUY),
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

    def _get_error_code(self, e: Exception) -> MarketErrorCode:
        if isinstance(e, HttpResponseException) and isinstance(e.body, dict):
            error_code = e.body.get("code")
            error = e.body.get("msg")
            if error_code == -2013 or error_code == -2011 and "Unknown order" in error:
                return MarketErrorCode.NO_SUCH_ORDER
            elif error_code == -2010:  # New order rejected
                if "immediately match and take" in error:
                    return MarketErrorCode.POST_ONLY_ORDER_CANCELLED
                elif "insufficient balance" in error:
                    return MarketErrorCode.INSUFFICIENT_FUNDS
        return MarketErrorCode.UNKNOWN
