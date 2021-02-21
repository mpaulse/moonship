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

API_BASE_URL = "https://api.binance.com"
API_V3_BASE_URL = API_BASE_URL + "/api/v3"
STREAM_BASE_URL = "wss://stream.binance.com:9443/stream"


class BinanceClient(AbstractWebClient):

    def __init__(self, market_name: str, app_config: Config):
        api_key = app_config.get("moonship.binance.api_key")
        if not isinstance(api_key, str):
            raise StartUpException("Binance API key not configured")
        self.secret_key = app_config.get("moonship.binance.secret_key")
        if not isinstance(self.secret_key, str):
            raise StartUpException("Binance secret key not configured")
        super().__init__(
            market_name,
            app_config,
            WebClientSessionParameters(
                stream_url=f"{STREAM_BASE_URL}/{app_config.get(f'moonship.markets.{market_name}.symbol')}",
                headers={"X-MBX-APIKEY": api_key}))

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
                f"{API_V3_BASE_URL}/ticker/price",
                params={"symbol": self.market.symbol}) as rsp:
            await self.handle_error_response(rsp)
            return await rsp.json()

    async def _get_order_book_ticker(self) -> dict:
        async with self.http_session.get(
                f"{API_V3_BASE_URL}/ticker/bookTicker",
                params={"symbol": self.market.symbol}) as rsp:
            await self.handle_error_response(rsp)
            return await rsp.json()

    async def place_order(self, order: Union[MarketOrder, LimitOrder]) -> str:
        pass

    async def get_order(self, order_id: str) -> FullOrderDetails:
        pass

    async def cancel_order(self, order_id: str) -> bool:
        request = self._url_encode_and_sign({
            "symbol": self.market.symbol,
            "orderId": order_id,
            "timestamp": utc_timestamp_now_msec()
        })
        try:
            async with self.http_session.delete(
                    f"{API_V3_BASE_URL}/order",
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

    def get_data_stream_url(self) -> str:
        pass

    async def init_data_stream(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        pass

    async def on_data_stream_msg(self, msg: any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        pass
