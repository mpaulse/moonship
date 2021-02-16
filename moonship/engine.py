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

from datetime import timezone
from moonship import *
from typing import Optional

logger = logging.getLogger(__name__)


class Strategy:

    def __init__(self, name: str, algo: TradingAlgo, market_names: list[str], auto_start=True) -> None:
        self.name = name
        self.algo = algo
        self.market_names = market_names
        self.auto_start = auto_start


class MarketManager(MarketFeedSubscriber):

    def __init__(self, market: Market) -> None:
        self.market = market
        self.market.subscribe_to_feed(self)

    async def open(self) -> None:
        self.market._status = MarketStatus.OPEN
        await self.market._feed.connect()
        await self.market._client.connect()
        ticker = await self.market._client.get_ticker()
        self.set_current_price(ticker.current_price)
        self.market._feed.raise_event(
            TickerEvent(
                timestamp=Timestamp.now(tz=timezone.utc),
                market_name=self.market.name,
                symbol=self.market.symbol,
                ticker=ticker))

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

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        self.market._order_book.add_order(event.order)

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        self.market._order_book.remove_order(event.order_id)

    async def on_market_status_update(self, event: MarketStatusEvent) -> None:
        self.market._status = event.status

    async def on_trade(self, event: TradeEvent) -> None:
        self.set_current_price(event.counter_amount / event.base_amount)
        self.market._order_book.remove_order(event.maker_order_id)
        self.market._feed.raise_event(
            TickerEvent(
                timestamp=event.timestamp,
                market_name=event.market_name,
                symbol=event.symbol,
                ticker=Ticker(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    current_price=self.market.current_price,
                    bid_price=self.market.bid_price,
                    ask_price=self.market.ask_price,
                    status=self.market.status)))

    def set_current_price(self, price: Amount) -> None:
        self.market._current_price = price.quantize(Amount("0.00"))


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
                raise StartUpException(f"No symbol configured for {market_name} market")
            client_class_name = market_config.get("client")
            if not isinstance(client_class_name, str):
                raise StartUpException(f"No client configured for {market_name} market")
            client_class = self._get_class(client_class_name)
            if client_class is None or not issubclass(client_class, MarketClient):
                raise StartUpException(f"Invalid client specified for {market_name} market: {client_class_name}")
            client = client_class(market_name, symbol, config)
            feed_class_name = market_config.get("feed")
            if not isinstance(feed_class_name, str):
                raise StartUpException(f"No feed configured for {market_name} market")
            feed_class = self._get_class(feed_class_name)
            if feed_class is None or not issubclass(feed_class, MarketFeed):
                raise StartUpException(f"Invalid feed specified for {market_name} market: {feed_class_name}")
            feed = feed_class(market_name, symbol, config)
            self.markets[market_name] = MarketManager(Market(market_name, symbol, client, feed))

    def _init_strategies(self, config: Config) -> None:
        strategies_config = config.get("moonship.strategies")
        if not isinstance(strategies_config, Config):
            raise StartUpException("No strategy configuration specified")
        for strategy_name, strategy_config in strategies_config.items():
            markets_names = strategy_config.get("markets")
            if not isinstance(markets_names, list):
                logger.warning(f"No markets configured for {strategy_name} strategy. Ignoring.")
                continue
            markets: [str, Market] = {}
            for market_name in markets_names:
                market = self.markets[market_name]
                if market is None:
                    raise StartUpException(f"Invalid market specified for {strategy_name} strategy: {market_name}")
                markets[market_name] = market.market
            algo_class_name = strategy_config.get("algo")
            if not isinstance(algo_class_name, str):
                raise StartUpException(f"No algo configured for {strategy_name} strategy")
            algo_class = self._get_class(algo_class_name)
            if algo_class is None or not issubclass(algo_class, TradingAlgo):
                raise StartUpException(f"Invalid algo specified for {strategy_name} strategy: {algo_class_name}")
            algo = algo_class(strategy_name, markets, config)
            auto_start = strategy_config.get("auto_start")
            if not isinstance(auto_start, bool):
                auto_start = True
            self.strategies[strategy_name] = Strategy(strategy_name, algo, markets_names, auto_start)

    async def start(self) -> None:
        for strategy in self.strategies.values():
            if strategy.auto_start:
                await self.start_strategy(strategy.name)

    async def start_strategy(self, name: str) -> None:
        strategy = self.strategies.get(name)
        if strategy is not None:
            for market_name in strategy.market_names:
                market = self.markets[market_name]
                if market.market.status == MarketStatus.CLOSED:
                    logger.info(f"Opening {market_name} market...")
                    await market.open()
            logger.info(f"Starting {strategy.name} strategy...")
            await strategy.algo.start()

    async def stop(self) -> None:
        for strategy_name in self.strategies.keys():
            await self.stop_strategy(strategy_name)
        for market in self.markets.values():
            if market.market.status != MarketStatus.CLOSED:
                logger.info(f"Stopping {market.market.name} market...")
                await market.close()

    async def stop_strategy(self, name: str) -> None:
        strategy = self.strategies.get(name)
        if strategy is not None:
            logger.info(f"Stopping {strategy.name} strategy...")
            await strategy.algo.stop()

    def _get_class(self, class_name: str) -> Optional[type]:
        try:
            module_name, class_name = class_name.rsplit(".", 1)
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except:
            return None
