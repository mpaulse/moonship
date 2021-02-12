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

from dataclasses import dataclass, field
from datetime import datetime as Timestamp
from decimal import Decimal as Amount
from enum import Enum


class MarketStatus(Enum):
    DISABLED = 0
    ACTIVE = 1
    POST_ONLY = 2


@dataclass()
class Ticker:
    timestamp: Timestamp
    symbol: str
    ask_price: Amount
    bid_price: Amount
    current_price: Amount
    status: MarketStatus

    @property
    def spread(self) -> Amount:
        return self.ask_price - self.bid_price


class OrderStatus(Enum):
    PENDING = 0
    COMPLETE = 1


@dataclass()
class Order:
    id: str
    symbol: str
    price: Amount
    volume: Amount
    volume_filled: Amount = Amount(0)
    is_buy: bool = True
    is_limit_order: bool = True
    status: OrderStatus = OrderStatus.PENDING
    created_timestamp: Timestamp = None

