#  Copyright (c) 2025, Marlon Paulse
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
from moonship.core.ipc import *
from moonship.core.redis import *
from moonship.core.service import *
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


class APIService(Service):

    def __init__(self, config: Config) -> None:
        self.config = config
        self.port = self.get_port(config)
        self.ssl_context = self.get_ssl_context(config)
        self.user = config.get("moonship.api.user")
        if not isinstance(self.user, str):
            raise ConfigException("No API user configured")
        password = config.get("moonship.api.password")
        if not isinstance(password, str):
            raise ConfigException("No API password configured")
        self.idle_session_expiry_msec = config.get("moonship.api.idle_session_expiry", 0)
        if not isinstance(self.idle_session_expiry_msec, int):
            raise ConfigException("Invalid API idle session expiry time")
        self.idle_session_expiry_msec *= 60_000
        if self.idle_session_expiry_msec <= 0:
            self.idle_session_expiry_msec = None
        self.access_log_format = config.get("moonship.api.access_log_format")
        if not isinstance(self.access_log_format, str):
            self.access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i'
        self.password = password.encode("utf-8")
        self.web_app_runner: aiohttp.web.AppRunner | None = None
        self.session_store: RedisSessionStore | None = None
        self.shared_cache = SharedCacheDataAccessor(RedisSharedCache(config))
        self.message_bus = RedisMessageBus(config)

    async def start(self) -> None:
        await self.shared_cache.open()
        await self.message_bus.start()
        web_app = aiohttp.web.Application(middlewares=[self.handle_error], logger=logger)
        web_app.add_routes([
            aiohttp.web.post("/login", self.login),
            aiohttp.web.get("/logout", self.logout),
            aiohttp.web.get("/strategies", self.get_strategies),
            aiohttp.web.get("/strategies/{engine}/{strategy}", self.get_strategy),
            aiohttp.web.post("/strategies/{engine}/{strategy}", self.add_strategy),
            aiohttp.web.patch("/strategies/{engine}/{strategy}", self.update_strategy),
            aiohttp.web.delete("/strategies/{engine}/{strategy}", self.remove_strategy),
            aiohttp.web.post("/strategies/{engine}/{strategy}/start", self.start_strategy),
            aiohttp.web.post("/strategies/{engine}/{strategy}/stop", self.stop_strategy)
        ])
        web_app.on_response_prepare.append(self.on_prepare_response)
        self.session_store = RedisSessionStore(self.config, self.idle_session_expiry_msec)
        await self.session_store.open()
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

    async def login(self, req: Request) -> StreamResponse:
        try:
            req_body = await req.json()
        except JSONDecodeError:
            return self.bad_request("Missing or bad request body")
        user = req_body.get("user")
        if not isinstance(user, str):
            return self.bad_request("Missing or bad user field")
        password = req_body.get("password")
        if not isinstance(password, str):
            return self.bad_request("Missing or bad password field")
        session = await aiohttp_session.get_session(req)
        if user == self.user and bcrypt.checkpw(password.encode("utf-8"), self.password):
            if session.new:
                session.set_new_identity(uuid.uuid4().hex)
                session["user"] = self.user
            return self.ok({
                "session_token": session.identity
            })
        session.invalidate()
        return self.unauthorized("Invalid user or password")

    async def logout(self, req: Request) -> StreamResponse:
        session = await aiohttp_session.get_session(req)
        session.invalidate()
        return self.ok()

    async def get_strategies(self, req: Request) -> StreamResponse:
        criteria: dict[str, str] = { param: value for param, value in req.query.items() }
        return self.ok({"strategies": await self.shared_cache.get_strategies(criteria)})

    async def get_strategy(self, req: Request) -> StreamResponse:
        strategy = await self.shared_cache.get_strategy(
            req.match_info["strategy"],
            req.match_info["engine"])
        if strategy is None:
            return self.not_found("No such strategy")
        return self.ok(strategy)

    async def add_strategy(self, req: Request) -> StreamResponse:
        return await self.configure_strategy(req, "add")

    async def update_strategy(self, req: Request) -> StreamResponse:
        return await self.configure_strategy(req, "update")

    async def configure_strategy(self, req: Request, command: str) -> StreamResponse:
        try:
            req_body = await req.json()
        except JSONDecodeError:
            return self.bad_request("Missing or bad request body")
        config = req_body.get("config")
        if not isinstance(config, dict):
            return self.bad_request("Missing or invalid config field")
        return await self.invoke_strategy_command(command, req, {"config": config})

    async def remove_strategy(self, req: Request) -> StreamResponse:
        return await self.invoke_strategy_command("remove", req)

    async def start_strategy(self, req: Request) -> StreamResponse:
        return await self.invoke_strategy_command("start", req)

    async def stop_strategy(self, req: Request) -> StreamResponse:
        return await self.invoke_strategy_command("stop", req)

    async def invoke_strategy_command(self, command: str, req: Request, data: dict[str, Any] = None) -> StreamResponse:
        if data is None:
            data = {}
        return self.handle_msg_bus_response(
            await self.message_bus.publish_and_receive(
                {
                    "command": command,
                    "strategy": req.match_info["strategy"],
                    "engine": req.match_info["engine"]
                } | data,
                "moonship:message:request",
                "moonship:message:response"))

    def handle_msg_bus_response(self, rsp: dict[str, Any]) -> StreamResponse:
        result = rsp.get("result")
        if result is not None:
            match result:
                case MessageResult.SUCCESS.value:
                    return self.ok()
                case MessageResult.MISSING_OR_INVALID_PARAMETER.value:
                    return self.bad_request(f"Missing or bad {rsp.get('parameter')} field")
        return self.error_response()

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
                return self.unauthorized("Access denied")
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
            return self.error_response(response.reason, response.status)
        except HTTPException as e:
            return self.error_response(e.reason, e.status)
        except TimeoutError:
            return self.timeout()
        except Exception:
            logger.exception("Internal server error")
            return self.error_response()

    def ok(self, json_rsp_body: dict = None) -> Response:
        if json_rsp_body is not None:
            return aiohttp.web.json_response(json_rsp_body, status=200)
        return Response(status=200)

    def bad_request(self, error: str) -> Response:
        return self.error_response(error, 400)

    def unauthorized(self, error: str) -> Response:
        return self.error_response(error, 401)

    def not_found(self, error: str) -> Response:
        return self.error_response(error, 404)

    def timeout(self) -> Response:
        return self.error_response("Operation timeout", 504)

    def error_response(self, error: str = "Internal server error", status: int = 500) -> Response:
        return aiohttp.web.json_response(
            {
                "error": error,
                "status": status
            },
            status=status)

    def get_port(self, config: Config) -> int:
        port = config.get("moonship.api.port", 8080)
        if isinstance(port, str) and len(port) > 1 and port.startswith("$"):
            try:
                port = int(os.environ[port[1:]])
            except KeyError:
                raise ConfigException(f"No {port[1:]} environment variable set")
        if not isinstance(port, int) or port < 0 or port > 65535:
            raise ConfigException(f"Invalid API port configuration: {port}")
        return port

    def get_ssl_context(self, config: Config) -> ssl.SSLContext | None:
        ssl_context = None
        ssl_cert = config.get("moonship.api.ssl_cert")
        ssl_key = config.get("moonship.api.ssl_key")
        if isinstance(ssl_cert, str) and isinstance(ssl_key, str):
            ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
            ssl_context.load_cert_chain(ssl_cert, ssl_key)
        return ssl_context

