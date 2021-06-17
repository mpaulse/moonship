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

import logging

from moonship.core import *
from typing import Union

__all__ = [
    "TradingAlgo"
]


class TradingAlgo(MarketSubscriber):

    def __init__(self, strategy_name: str, markets: dict[str, Market], app_config: Config):
        self.strategy_name = strategy_name
        self.markets = markets
        self.logger = logging.getLogger(f"moonship.strategy.{strategy_name}")
        self.running = False

    async def on_started(self) -> None:
        pass

    async def on_stopped(self) -> None:
        pass

    async def on_order_book_update(
            self,
            event: Union[OrderBookInitEvent, OrderBookItemAddedEvent, OrderBookItemRemovedEvent]) -> None:
        pass

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        await self.on_order_book_update(event)

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        await self.on_order_book_update(event)

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        await self.on_order_book_update(event)


class Strategy:

    def __init__(self, name: str, algo: TradingAlgo, market_names: list[str], auto_start=True) -> None:
        self.name = name
        self.algo = algo
        self.market_names = market_names
        self.auto_start = auto_start

    async def start(self) -> None:
        if not self.algo.running:
            for market in self.algo.markets.values():
                market.subscribe(self.algo)
            self.algo.running = True
            await self.algo.on_started()

    async def stop(self) -> None:
        if self.algo.running:
            self.algo.running = False
            await self.algo.on_stopped()
            for market in self.algo.markets.values():
                market.unsubscribe(self.algo)

    @property
    def is_running(self) -> bool:
        return self.algo.running
