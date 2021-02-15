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

import importlib
import logging

from .algo import *
from .config import *
from .data import *
from .error import *
from .market import *
from typing import Optional

logger = logging.getLogger(__name__)


class Strategy:
    def __init__(self, name: str, algo: TradingAlgo):
        self.name = name
        self.algo = algo


class MarketManager(MarketFeedSubscriber):

    def __init__(self, market: Market) -> None:
        self.market = market
        self.market.subscribe_to_feed(self)

    async def open(self) -> None:
        self.market._status = MarketStatus.OPEN
        await self.market._feed.connect()
        await self.market._client.connect()

    async def close(self) -> None:
        await self.market._client.close()
        await self.market._feed.close()
        self.market._current_price = Amount(0)
        self.market._order_book.clear()
        self.market._status = MarketStatus.CLOSED

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        self.market._status = event.status
        self.market._order_book.clear()
        for order in event.orders:
            self.market._order_book.add_order(order)
        self.log_market_info()

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        self.market._order_book.add_order(event.order)
        self.log_market_info()

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        self.market._order_book.remove_order(event.order_id)
        self.log_market_info()

    async def on_market_status_update(self, event: MarketStatusEvent) -> None:
        self.market._status = event.status

    async def on_trade(self, event: TradeEvent) -> None:
        self.market._current_price = Amount(event.counter_amount / event.base_amount)
        self.market._order_book.remove_order(event.maker_order_id)
        self.market._feed.raise_event(
            TickerEvent(
                timestamp=event.timestamp,
                symbol=event.symbol,
                ticker=Ticker(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    current_price=self.market.current_price,
                    bid_price=self.market.bid_price,
                    ask_price=self.market.ask_price,
                    status=self.market.status)))

    def log_market_info(self):
        if logger.isEnabledFor(logging.DEBUG):
            bids = self.market.bids.values()
            asks = self.market.asks.values()
            n = min(min(len(bids), len(asks)), 10)
            s = f"\n========== Market info: {self.market.name} =========\n"
            s += f"{'Bids':18} | {'Asks':18}\n"
            for i in range(0, n):
                b = bids[len(bids)-1-i]
                a = asks[i]
                s += f"{b.volume:8} {b.price:8} | {a.price:8} {a.volume:8}\n"
            s += "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
            s += f"Price: {self.market.current_price}\n"
            s += f"Bid: {self.market.bid_price}\n"
            s += f"Ask: {self.market.ask_price}\n"
            s += f"Spread: {self.market.spread}\n"
            logger.debug(s)


class TradeEngine:
    markets: dict[str, MarketManager] = {}
    strategies: dict[str, Strategy] = {}

    def __init__(self, config: Config) -> None:
        self._init_markets(config)
        self._init_strategies(config)

    def _init_markets(self, config: Config) -> None:
        markets_config = config.get("moonship.markets")
        if not isinstance(markets_config, Config):
            raise StartUpException("No market configuration specified")
        for market_name, market_config in markets_config.items():
            symbol = market_config.get("symbol")
            if not isinstance(symbol, str):
                raise StartUpException(f"No symbol configured for market: {market_name}")
            client_class_name = market_config.get("client")
            if not isinstance(client_class_name, str):
                raise StartUpException(f"No client configured for market: {market_name}")
            client_class = self._get_class(client_class_name)
            if client_class is None or not issubclass(client_class, MarketClient):
                raise StartUpException(f"Invalid client specified for market: {market_name}")
            client = client_class(market_name, symbol, config)
            feed_class_name = market_config.get("feed")
            if not isinstance(feed_class_name, str):
                raise StartUpException(f"No feed configured for market: {market_name}")
            feed_class = self._get_class(feed_class_name)
            if feed_class is None or not issubclass(feed_class, MarketFeed):
                raise StartUpException(f"Invalid feed specified for market: {market_name}")
            feed = feed_class(market_name, symbol, config)
            self.markets[market_name] = MarketManager(Market(market_name, symbol, client, feed))

    def _init_strategies(self, config: Config) -> None:
        pass

    async def start(self):
        for market_name, market in self.markets.items():
            logger.info(f"Opening market: {market_name}")
            await market.open()

    async def stop(self):
        for market_name, market in self.markets.items():
            logger.info(f"Closing market: {market_name}")
            await market.close()

    def _get_class(self, class_name: str) -> Optional[type]:
        try:
            module_name, class_name = class_name.rsplit(".", 1)
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except:
            return None
