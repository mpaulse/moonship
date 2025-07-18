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

import abc
import aiohttp
import asyncio

from dataclasses import dataclass
from moonship.core import *
from typing import Any

__all__ = [
    "AbstractWebClient",
    "WebClientSessionParameters",
    "WebClientStreamParameters"
]


@dataclass
class WebClientSessionParameters:
    auth: aiohttp.BasicAuth = None
    headers: dict = None


@dataclass
class WebClientStreamParameters:
    url: str = None
    headers: dict = None


class AbstractWebClient(MarketClient, abc.ABC):

    def __init__(
            self,
            market_name: str,
            app_config: Config,
            session_params: WebClientSessionParameters,
            stream_params: WebClientStreamParameters | list[WebClientStreamParameters] | None = None) -> None:
        super().__init__(market_name, app_config)
        self.session_params = session_params
        self.stream_params = stream_params
        self.http_session: aiohttp.ClientSession | None = None

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
        if isinstance(self.stream_params, WebClientStreamParameters):
            asyncio.create_task(self._process_data_stream(self.stream_params))
        elif isinstance(self.stream_params, list):
            for stream_params in self.stream_params:
                asyncio.create_task(self._process_data_stream(stream_params))

    async def _log_http_activity(self, session: aiohttp.ClientSession, context, params: Any) -> None:
        self.logger.debug(params)

    async def close(self) -> None:
        if self.http_session is not None:
            s = self.http_session
            self.http_session = None
            await s.close()

    @property
    def closed(self) -> bool:
        return self.http_session is None or self.http_session.closed

    async def _process_data_stream(self, params: WebClientStreamParameters):
        while not self.closed:
            try:
                await self.on_before_data_stream_connect(params)
                async with self.http_session.ws_connect(params.url, headers=params.headers) as websocket:
                    await self.on_after_data_stream_connect(websocket, params)
                    while not self.closed and not websocket.closed:
                        await self.on_data_stream_msg(await websocket.receive_json(), websocket)
            except Exception:
                if not self.closed:
                    self.logger.exception("Data stream error")
                    await asyncio.sleep(1)

    async def on_before_data_stream_connect(self, params: WebClientStreamParameters) -> None:
        pass

    async def on_after_data_stream_connect(
            self,
            websocket: aiohttp.ClientWebSocketResponse,
            params: WebClientStreamParameters) -> None:
        pass

    @abc.abstractmethod
    async def on_data_stream_msg(self, msg: Any, websocket: aiohttp.ClientWebSocketResponse) -> None:
        pass

    async def handle_error_response(self, response: aiohttp.ClientResponse) -> None:
        if response.status >= 400:
            try:
                body = await response.json()
            except aiohttp.ContentTypeError:
                body = await response.text()
            response.release()
            raise HttpResponseException(
                response.request_info,
                response.history,
                status=response.status,
                reason=response.reason,
                headers=response.headers,
                body=body)
