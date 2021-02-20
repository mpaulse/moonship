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
from moonship.core import *
from moonship.core.strategy import Strategy
from typing import Optional

logger = logging.getLogger(__name__)


class MarketManager(MarketSubscriber):

    def __init__(self, market: Market) -> None:
        self.market = market
        self.market.subscribe(self)

    async def open(self) -> None:
        self.market._status = MarketStatus.OPEN
        await self.market._feed.connect()
        await self.market._client.connect()
        # TODO: get pending orders
        ticker = await self.market.get_ticker()
        self.market._current_price = ticker.current_price
        self.market.raise_event(
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
        self.market._current_price = event.counter_amount / event.base_amount
        self.market._order_book.remove_order(event.maker_order_id)
        self.market.raise_event(
            TickerEvent(
                timestamp=event.timestamp,
                ticker=Ticker(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    current_price=self.market.current_price,
                    bid_price=self.market.bid_price,
                    ask_price=self.market.ask_price,
                    status=self.market.status)))
        pending_order_id = \
            event.maker_order_id if event.maker_order_id in self.market._pending_order_ids \
                else event.taker_order_id if event.taker_order_id in self.market._pending_order_ids \
                else None
        await self.market._update_order_status(pending_order_id)


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
            cls = self._load_class("client", market_config, MarketClient)
            client = cls(market_name, config)
            cls = self._load_class("feed", market_config, MarketFeed)
            feed = cls(market_name, config)
            self.markets[market_name] = MarketManager(Market(market_name, symbol, client, feed))

    def _init_strategies(self, config: Config) -> None:
        strategies_config = config.get("moonship.strategies")
        if not isinstance(strategies_config, Config):
            raise StartUpException("No strategy configuration specified")
        for strategy_name, strategy_config in strategies_config.items():
            markets_names = strategy_config.get("markets")
            if not isinstance(markets_names, list) or len(markets_names) == 0:
                logger.warning(f"No markets configured for {strategy_name} strategy. Ignoring.")
                continue
            markets: [str, Market] = {}
            for market_name in markets_names:
                market = self.markets[market_name]
                if market is None:
                    raise StartUpException(f"Invalid market specified for {strategy_name} strategy: {market_name}")
                markets[market_name] = market.market
            cls = self._load_class("algo", strategy_config, TradingAlgo)
            algo = cls(strategy_name, markets, config)
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
                    await market.open()
                    logger.info(f"Opened {market_name} market")
            await strategy.start()
            logger.info(f"Started {strategy.name} strategy")

    async def stop(self) -> None:
        for strategy_name in self.strategies.keys():
            await self.stop_strategy(strategy_name)
        for market in self.markets.values():
            if market.market.status != MarketStatus.CLOSED:
                await market.close()
                logger.info(f"Closed {market.market.name} market")

    async def stop_strategy(self, name: str) -> None:
        strategy = self.strategies.get(name)
        if strategy is not None and strategy.is_running:
            await strategy.stop()
            logger.info(f"Stopped {strategy.name} strategy")

    def _load_class(self, key: str, config: Config, expected_type: type) -> type:
        class_name = config.get(key)
        if not isinstance(class_name, str):
            raise StartUpException(f"Missing configuration: {config.key}.{key}")
        cls, version = self._get_class_and_version(class_name)
        if cls is None or not issubclass(cls, expected_type):
            raise StartUpException(f"Invalid configuration: {config.key}.{key}")
        logger.info(f"Loaded {key} {class_name if version is None else f'{class_name} (version {version})'}")
        return cls

    def _get_class_and_version(self, class_name: str) -> (Optional[type], Optional[str]):
        try:
            module_name, class_name = class_name.rsplit(".", 1)
            module = importlib.import_module(module_name)
            return getattr(module, class_name), getattr(module, "__version__", None)
        except:
            logger.exception("Module load error")
            return None, None
