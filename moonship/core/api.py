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

import aiohttp.web
import aiohttp_session
import bcrypt
import logging
import os
import ssl
import uuid

from aiohttp.web import HTTPException, Request, Response, StreamResponse
from json import JSONDecodeError
from moonship.core import *
from moonship.core.redis import *
from moonship.core.service import *
from typing import Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


class APIService(Service):

    def __init__(self, config: Config) -> None:
        self.config = config
        self.port = self._get_port(config)
        self.ssl_context = self._get_ssl_context(config)
        self.user = config.get("moonship.api.user")
        if not isinstance(self.user, str):
            raise StartUpException("No API user configured")
        password = config.get("moonship.api.password")
        if not isinstance(password, str):
            raise StartUpException("No API password configured")
        self.access_log_format = config.get("moonship.api.access_log_format")
        if not isinstance(self.access_log_format, str):
            self.access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i'
        self.password = password.encode("utf-8")
        self.web_app_runner: Optional[aiohttp.web.AppRunner] = None
        self.session_store: Optional[RedisSessionStore] = None
        self.shared_cache = RedisSharedCache(config)
        self.message_bus = RedisMessageBus(config)

    async def start(self) -> None:
        await self.message_bus.start()
        web_app = aiohttp.web.Application(middlewares=[self.handle_error], logger=logger)
        web_app.add_routes([
            aiohttp.web.post("/login", self.post_login),
            aiohttp.web.get("/logout", self.get_logout),
            aiohttp.web.get("/engines", self.get_engines),
            aiohttp.web.get("/{engine}/strategies/{strategy}", self.get_strategy)
        ])
        web_app.on_response_prepare.append(self.on_prepare_response)
        self.session_store = RedisSessionStore(self.config)
        aiohttp_session.setup(web_app, self.session_store)
        web_app.middlewares.append(self.verify_session)
        self.web_app_runner = aiohttp.web.AppRunner(
            web_app,
            logger=logger,
            access_log=logger,
            access_log_format=self.access_log_format)
        await self.web_app_runner.setup()
        site = aiohttp.web.TCPSite(self.web_app_runner, port=self.port, ssl_context=self.ssl_context)
        await site.start()
        logger.info(f"Listening on port {self.port}")

    async def stop(self) -> None:
        if self.session_store is not None:
            await self.session_store.close()
        if self.web_app_runner is not None:
            await self.web_app_runner.cleanup()
        await self.message_bus.close()
        await self.shared_cache.close()

    async def post_login(self, req: Request) -> StreamResponse:
        try:
            req_body = await req.json()
        except JSONDecodeError:
            return self._bad_request("Missing or bad request body")
        user = req_body.get("user")
        if not isinstance(user, str):
            return self._bad_request("Missing or bad user field")
        password = req_body.get("password")
        if not isinstance(password, str):
            return self._bad_request("Missing or bad password field")
        session = await aiohttp_session.get_session(req)
        if user == self.user and bcrypt.checkpw(password.encode("utf-8"), self.password):
            if session.new:
                session.set_new_identity(uuid.uuid4().hex)
                session["user"] = self.user
            return self._ok({
                "session_token": session.identity
            })
        session.invalidate()
        return self._unauthorized("Invalid user or password")

    async def get_logout(self, req: Request) -> StreamResponse:
        session = await aiohttp_session.get_session(req)
        session.invalidate()
        return self._ok()

    async def get_engines(self, req: Request) -> StreamResponse:
        engines = []
        engine_names = await self.shared_cache.set_elements("engines")
        for engine_name in engine_names:
            engine: dict[str, any] = await self.shared_cache.map_entries(engine_name)
            engine["name"] = engine_name
            engine["strategies"] = list(await self.shared_cache.set_elements(f"{engine_name}.strategies"))
            engines.append(engine)
        return self._ok({"engines": engines})

    async def get_strategy(self, req: Request) -> StreamResponse:
        engine_name = req.match_info["engine"]
        strategy_name = req.match_info["strategy"]
        key = f"{engine_name}.{strategy_name}"
        strategy: dict[str, any] = await self.shared_cache.map_entries(key)
        if len(strategy) == 0:
            return self._not_found("No such strategy")
        strategy["name"] = strategy_name
        strategy["engine"] = engine_name
        strategy["config"] = await self.shared_cache.map_entries(f"{key}.config")
        markets = strategy["config"].get("markets")
        if isinstance(markets, str):
            strategy["config"]["markets"] = markets.split(",")
        return self._ok(strategy)

    async def on_prepare_response(self, req: Request, rsp: StreamResponse) -> None:
        rsp.headers["Server"] = "Moonship"

    @aiohttp.web.middleware
    async def verify_session(
            self,
            req: Request,
            handler: Callable[[Request], Awaitable[StreamResponse]]) -> StreamResponse:
        if req.path != "/login":
            session = await aiohttp_session.get_session(req)
            if session.new:
                return self._unauthorized("Access denied")
        return await handler(req)

    @aiohttp.web.middleware
    async def handle_error(
            self,
            req: Request,
            handler: Callable[[Request], Awaitable[StreamResponse]]) -> StreamResponse:
        try:
            response = await handler(req)
            if response.status < 400 or response.content_type == "application/json":
                return response
            return self._error_response("error", response.reason, response.status)
        except HTTPException as e:
            return self._error_response("error", e.reason, e.status)
        except Exception as e:
            logger.exception("Internal server error", exc_info=e)
            return self._error_response("error", "Internal server error", 500)

    def _ok(self, json_rsp_body: dict = None) -> Response:
        if json_rsp_body is not None:
            return aiohttp.web.json_response(json_rsp_body, status=200)
        return Response(status=200)

    def _bad_request(self, error_title: str) -> Response:
        return self._error_response("validation", error_title, 400)

    def _unauthorized(self, error_title: str) -> Response:
        return self._error_response("access", error_title, 401)

    def _not_found(self, error_title: str) -> Response:
        return self._error_response("resource", error_title, 404)

    def _error_response(self, type: str, title: str, status: int) -> Response:
        return aiohttp.web.json_response(
            {
                "type": type,
                "title": title,
                "status": status
            },
            status=status)

    def _get_port(self, config: Config) -> int:
        port = config.get("moonship.api.port", 8080)
        if isinstance(port, str) and len(port) > 1 and port.startswith("$"):
            try:
                port = int(os.environ[port[1:]])
            except KeyError:
                raise StartUpException(f"No {port[1:]} environment variable set")
        if not isinstance(port, int) or port < 0 or port > 65535:
            raise StartUpException(f"Invalid API port configuration: {port}")
        return port

    def _get_ssl_context(self, config: Config) -> Optional[ssl.SSLContext]:
        ssl_context = None
        ssl_cert = config.get("moonship.api.ssl_cert")
        ssl_key = config.get("moonship.api.ssl_key")
        if isinstance(ssl_cert, str) and isinstance(ssl_key, str):
            ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
            ssl_context.load_cert_chain(ssl_cert, ssl_key)
        return ssl_context

