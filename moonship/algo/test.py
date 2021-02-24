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


class LogMarketInfo(TradingAlgo):

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        self.log_market_info(event)

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        self.log_market_info(event)

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        self.log_market_info(event)

    async def on_ticker(self, event: TickerEvent) -> None:
        self.log_market_info(event)

    def log_market_info(self, event: MarketEvent):
        if self.logger.isEnabledFor(logging.DEBUG):
            market = self.markets[event.market_name]
            bids = market.bids.values()
            asks = market.asks.values()
            n = min(min(len(bids), len(asks)), 10)
            s = f"\n========== Market info: {event.market_name} ==========\n"
            s += f"{'Bids':>18} | {'Asks':18}\n"
            for i in range(0, n):
                b = bids[-1 - i]
                a = asks[i]
                s += f"{b.quantity:8} {b.price:8} | {a.price:8} {a.quantity:8}\n"
            s += "\n"
            s += f"Price: {market.current_price.quantize(market.bid_price)}\n"
            s += f"Bid: {market.bid_price}\n"
            s += f"Ask: {market.ask_price}\n"
            s += f"Spread: {market.spread}\n"
            self.logger.debug(s)
