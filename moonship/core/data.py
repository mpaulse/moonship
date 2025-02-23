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

import abc
import decimal

from dataclasses import dataclass
from datetime import datetime as Timestamp, timezone
from decimal import Decimal as Amount
from enum import Enum

__all__ = [
    "Amount",
    "Candle",
    "CandlePeriod",
    "FullOrderDetails",
    "LimitOrder",
    "MarketInfo",
    "MarketOrder",
    "MarketStatus",
    "MAX_DECIMALS",
    "OrderAction",
    "OrderStatus",
    "round_amount",
    "Rounding",
    "Ticker",
    "TimeInForce",
    "Timestamp",
    "to_amount",
    "to_amount_str",
    "to_utc_timestamp",
    "Trade",
    "utc_timestamp_now_msec"
]

MAX_DECIMALS = 8


class MarketStatus(Enum):
    CLOSED = 0
    OPEN = 1
    OPEN_POST_ONLY = 2


@dataclass
class MarketInfo:
    symbol: str
    base_asset: str
    base_asset_precision: int
    base_asset_min_quantity: Amount
    quote_asset: str
    quote_asset_precision: int
    status: MarketStatus


@dataclass
class Ticker:
    timestamp: Timestamp
    symbol: str
    ask_price: Amount
    bid_price: Amount
    current_price: Amount

    @property
    def spread(self) -> Amount:
        return self.ask_price - self.bid_price


class OrderAction(Enum):
    BUY = 0
    SELL = 1


@dataclass
class Trade:
    id: str
    timestamp: Timestamp
    symbol: str
    price: Amount
    quantity: Amount
    taker_action: OrderAction


class OrderStatus(Enum):
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY FILLED"
    FILLED = "FILLED"
    CANCELLATION_PENDING = "CANCELLATION PENDING"
    CANCELLED = "CANCELLED"
    CANCELLED_AND_PARTIALLY_FILLED = "CANCELLED AND PARTIALLY FILLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


@dataclass
class AbstractOrder(abc.ABC):
    action: OrderAction
    id: str = None


@dataclass
class MarketOrder(AbstractOrder):
    quantity: Amount = Amount(0)
    is_base_quantity: bool = True


class TimeInForce(Enum):
    FILL_OR_KILL = "FOK"
    GOOD_TILL_CANCELLED = "GTC"
    IMMEDIATE_OR_CANCEL = "IOC"


@dataclass
class LimitOrder(AbstractOrder):
    price: Amount = Amount(0)
    quantity: Amount = Amount(0)
    post_only: bool = True
    time_in_force: TimeInForce | None = None


@dataclass
class FullOrderDetails(AbstractOrder):
    symbol: str = None
    quantity: Amount = Amount(0)
    quote_quantity: Amount = Amount(0)
    limit_price: Amount = Amount(0)
    status: OrderStatus = OrderStatus.PENDING
    quantity_filled: Amount = Amount(0)
    quantity_filled_fee: Amount = Amount(0)
    quote_quantity_filled: Amount = Amount(0)
    quote_quantity_filled_fee: Amount = Amount(0)
    creation_timestamp: Timestamp = None
    time_in_force: TimeInForce = None


class Rounding(Enum):
    ROUND_05UP = decimal.ROUND_05UP
    ROUND_CEILING = decimal.ROUND_CEILING
    ROUND_DOWN = decimal.ROUND_DOWN
    ROUND_FLOOR = decimal.ROUND_FLOOR
    ROUND_HALF_DOWN = decimal.ROUND_HALF_DOWN
    ROUND_HALF_UP = decimal.ROUND_HALF_UP
    ROUND_HALF_EVEN = decimal.ROUND_HALF_EVEN
    ROUND_UP = decimal.ROUND_UP


class CandlePeriod(Enum):
    ONE_MIN = 60
    FIVE_MIN = 300
    FIFTEEN_MIN = 900
    THIRTY_MIN = 1800
    HOUR = 3600
    DAY = 86400


@dataclass
class Candle:
    symbol: str
    start_time: Timestamp
    end_time: Timestamp
    period: CandlePeriod
    open: Amount
    high: Amount
    low: Amount
    close: Amount
    volume: Amount | None = None
    buy_volume: Amount | None = None


def to_amount(s: str) -> Amount:
    return Amount(s) if s is not None else Amount(0)


def to_amount_str(a: Amount, max_decimals=MAX_DECIMALS, rounding=Rounding.ROUND_FLOOR) -> str:
    if max_decimals is not None:
        a = round_amount(a, max_decimals, rounding)
    s = str(a)
    if "." in s:
        s = s.rstrip("0")
        if s[-1] == ".":
            s = s[:-1]
    return s


def round_amount(a: Amount, num_decimals, rounding=Rounding.ROUND_FLOOR) -> Amount:
    return a.quantize(
        Amount("0." + "".join(["0" for _ in range(0, num_decimals)])),
        rounding=rounding.value)


def to_utc_timestamp(utc_ts_msec: int) -> Timestamp:
    return Timestamp.fromtimestamp(utc_ts_msec / 1000, timezone.utc)


def utc_timestamp_now_msec() -> int:
    return int(Timestamp.now(tz=timezone.utc).timestamp() * 1000)
