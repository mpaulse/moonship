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

from dataclasses import dataclass
from moonship.core import *
from typing import Optional

__all__ = [
    "AbstractWebClient",
    "WebClientResponseErrorMessage",
    "WebClientSessionParameters"
]


@dataclass
class WebClientSessionParameters:
    auth: aiohttp.BasicAuth = None
    headers: dict = None
    stream_url: str = None


@dataclass
class WebClientResponseErrorMessage:
    reason: str
    body: str


class AbstractWebClient(MarketClient, abc.ABC):
    http_session: Optional[aiohttp.ClientSession]

    def __init__(
            self,
            market_name: str,
            app_config: Config,
            session_params: WebClientSessionParameters
    ) -> None:
        super().__init__(market_name, app_config)
        self.session_params = session_params

    async def connect(self) -> None:
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self._log_http_activity)
        trace_config.on_request_chunk_sent.append(self._log_http_activity)
        trace_config.on_response_chunk_received.append(self._log_http_activity)
        trace_config.on_request_end.append(self._log_http_activity)
        self.http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=15),
            auth=self.session_params.auth,
            headers=self.session_params.headers,
            trace_configs=[trace_config])
        asyncio.create_task(self.process_data_stream())

    async def _log_http_activity(self, session: aiohttp.ClientSession, context, params: any) -> None:
        self.market.logger.debug(params)

    async def close(self) -> None:
        if self.http_session is not None:
            s = self.http_session
            self.http_session = None
            await s.close()

    @property
    def closed(self) -> bool:
        return self.http_session is None or self.http_session.closed

    async def process_data_stream(self):
        while not self.closed:
            try:
                async with self.http_session.ws_connect(self.session_params.stream_url) as websocket:
                    await self.init_data_stream(websocket)
                    while not self.closed and not websocket.closed:
                        await self.on_data_stream_msg(await websocket.receive_json(), websocket)
            except Exception as e:
                if not self.closed:
                    self.market.logger.exception("Data stream error", exc_info=e)
                    await asyncio.sleep(1)

    @abc.abstractmethod
    async def init_data_stream(self, websocket: aiohttp.ClientWebSocketResponse) -> None:
        pass

    @abc.abstractmethod
    async def on_data_stream_msg(self, msg: any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        pass

    async def handle_error_response(self, response: aiohttp.ClientResponse) -> None:
        if response.status >= 400:
            msg = WebClientResponseErrorMessage(response.reason, await response.text())
            response.release()
            raise aiohttp.ClientResponseError(
                response.request_info,
                response.history,
                status=response.status,
                message=str(msg),
                headers=response.headers)
