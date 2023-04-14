#  Copyright (c) 2023, Marlon Paulse
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

import aiohttp.typedefs
import enum

from typing import Tuple, Optional

__all__ = [
    "ConfigException",
    "HttpResponseException",
    "MarketErrorCode",
    "MarketException",
    "ShutdownException",
    "StartUpException"
]


class HttpResponseException(aiohttp.ClientResponseError):

    def __init__(
            self,
            request_info: aiohttp.RequestInfo,
            history: Tuple[aiohttp.ClientResponse, ...],
            status: Optional[int] = None,
            reason: str = "",
            headers: Optional[aiohttp.typedefs.LooseHeaders] = None,
            body: any = None):
        super().__init__(
            request_info,
            history,
            status=status,
            message=str({"reason": reason, "body": body}),
            headers=headers)
        self.body = body


class MarketErrorCode(enum.Enum):
    UNKNOWN = 0
    INSUFFICIENT_FUNDS = 2001
    POST_ONLY_ORDER_CANCELLED = 2002
    NO_SUCH_ORDER = 2003


class MarketException(Exception):

    def __init__(self, message: str, market_name: str, error_code=MarketErrorCode.UNKNOWN):
        super().__init__(f"{market_name}: {message}")
        self.market_name = market_name
        self.error_code = error_code


class StartUpException(Exception):
    pass


class ConfigException(StartUpException):
    pass


class ShutdownException(Exception):
    pass
