#  Copyright (c) 2024, Marlon Paulse
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

import asyncio
import importlib
import logging
import nanoid

from datetime import timedelta, timezone
from moonship.core import *
from moonship.core.ipc import *
from moonship.core.redis import *
from moonship.core.service import *
from moonship.core.strategy import Strategy
from typing import Optional

DEFAULT_MAX_RECENT_TRADE_LIST_SIZE = 10_000
DEFAULT_MAX_RECENT_CANDLE_LIST_SIZE = 10_000
DEFAULT_ENGINE_NAME = "engine"

logger = logging.getLogger(__name__)


class MarketManager(MarketSubscriber):

    def __init__(self, market: Market, max_recent_trade_list_size: int) -> None:
        self.market = market
        self.max_recent_trade_list_size = max_recent_trade_list_size
        self.market.subscribe(self)

    async def open(self) -> None:
        self.market._status = MarketStatus.OPEN
        await self.market._client.connect()
        await asyncio.gather(
            self._init_current_price(),
            self._init_recent_trade_list(),
            self._init_recent_candles(),
            self._init_market_info())
        asyncio.create_task(self._poll_candles())

    async def _init_current_price(self) -> None:
        ticker = await self.market.get_ticker()
        self.market._current_price = ticker.current_price
        self.market.raise_event(
            TickerEvent(
                timestamp=Timestamp.now(tz=timezone.utc),
                market_name=self.market.name,
                symbol=self.market.symbol,
                ticker=ticker))

    async def _init_recent_trade_list(self) -> None:
        trades = await self.market.get_recent_trades()
        for trade in trades:
            self._add_trade(trade)

    async def _init_recent_candles(self) -> None:
        candles = await self.market.get_candles()
        for candle in candles:
            self._add_candle(candle)

    async def _init_market_info(self) -> None:
        info = await self.market._client.get_market_info()
        self.market._status = info.status
        self.market._base_asset = info.base_asset
        self.market._base_asset_precision = info.base_asset_precision
        self.market._base_asset_min_quantity = info.base_asset_min_quantity
        self.market._quote_asset = info.quote_asset
        self.market._quote_asset_precision = info.quote_asset_precision

    def _add_trade(self, trade: Trade) -> None:
        if len(self.market._recent_trades) >= self.max_recent_trade_list_size:
            self.market._recent_trades.pop(0)
        self.market._recent_trades.add(trade)

    def _add_candle(self, candle: Candle) -> None:
        if len(self.market._recent_candles) >= DEFAULT_MAX_RECENT_CANDLE_LIST_SIZE:
            self.market._recent_candles.pop(0)
        if len(self.market._recent_candles) > 0 and self.market._recent_candles[-1].start_time == candle.start_time:
            self.market._recent_candles.pop()
        self.market._recent_candles.add(candle)

    async def close(self) -> None:
        await self.market._client.close()
        self.market._current_price = Amount(0)
        self.market._order_book.clear()
        self.market._recent_trades.clear()
        self.market._status = MarketStatus.CLOSED

    async def on_order_book_init(self, event: OrderBookInitEvent) -> None:
        self.market._order_book.clear()
        for order in event.orders:
            self.market._order_book.add(order)

    async def on_order_book_item_added(self, event: OrderBookItemAddedEvent) -> None:
        self.market._order_book.add(event.order)
        await self._check_pending_order_updates()

    async def on_order_book_item_removed(self, event: OrderBookItemRemovedEvent) -> None:
        self.market._order_book.remove(event.order_id)

    async def on_market_status_update(self, event: MarketStatusEvent) -> None:
        self.market._status = event.status

    async def on_trade(self, event: TradeEvent) -> None:
        self.market._current_price = event.trade.price
        self.market._order_book.remove(event.maker_order_id, event.trade.quantity)
        self._add_trade(event.trade)
        self.market.raise_event(
            TickerEvent(
                timestamp=event.timestamp,
                ticker=Ticker(
                    timestamp=event.timestamp,
                    symbol=event.symbol,
                    current_price=self.market.current_price,
                    bid_price=self.market.bid_price,
                    ask_price=self.market.ask_price)))
        pending_order_id = \
            event.maker_order_id if event.maker_order_id in self.market._pending_orders \
                else event.taker_order_id if event.taker_order_id in self.market._pending_orders \
                else None
        if pending_order_id is not None:
            order = self.market._pending_orders.get(pending_order_id)
            if order is not None:
                order.quantity_filled += event.trade.quantity
                order.quote_quantity_filled += event.trade.quantity * event.trade.price
                if order.quantity == order.quantity_filled or order.quote_quantity == order.quote_quantity_filled:
                    order.status = OrderStatus.FILLED
                else:
                    order.status = OrderStatus.PARTIALLY_FILLED
            logger.debug(f"Trade executed for pending order {pending_order_id}")
            await self.market._handle_pending_order_update(pending_order_id)
        else:  # In case trade events for pending orders did not arrive or were lost
            await self._check_pending_order_updates(market_price_changed=True)

    async def _check_pending_order_updates(self, market_price_changed: bool = False) -> None:
        pending_order_ids: list[str] = []
        for order in self.market._pending_orders.values():
            if order.limit_price > 0 \
                    and ((order.action == OrderAction.BUY
                          and (order.limit_price >= self.market.ask_price
                               or (market_price_changed and order.limit_price >= self.market.current_price)))
                         or (order.action == OrderAction.SELL
                             and (order.limit_price <= self.market.bid_price
                                  or (market_price_changed and order.limit_price <= self.market.current_price)))):
                logger.debug(f"Checking if pending order {order.id} was updated")
                pending_order_ids.append(order.id)
        for order_id in pending_order_ids:
            await self.market._handle_pending_order_update(order_id)

    async def on_order_status_update(self, event: OrderStatusUpdateEvent) -> None:
        # For MarketClients capable of receiving order update stream events
        self.market._remove_completed_pending_order(event.order)

    async def _poll_candles(self) -> None:
        while True:
            try:
                now = Timestamp.now(timezone.utc)
                latest_candle: Optional[Candle] = None
                from_time = None

                if len(self.market._recent_candles) > 0:
                    latest_candle = self.market._recent_candles[-1]
                    if now <= latest_candle.end_time:
                        from_time = latest_candle.start_time
                    else:
                        from_time = latest_candle.end_time + timedelta(milliseconds=1)

                candles = await self.market.get_candles(from_time=from_time)
                for candle in candles:
                    if now > candle.end_time:
                        self._add_candle(candle)
                        self.market.raise_event(CandleEvent(candle=candle))

                if len(candles) > 0:
                    latest_candle = candles[-1]

                poll_time = self.market._default_candle_period.value
                if latest_candle is not None:
                    if now <= latest_candle.end_time:
                        poll_time = (latest_candle.end_time - now).total_seconds()
                    else:
                        poll_time = (timedelta(seconds=poll_time) - (now - latest_candle.end_time)).total_seconds()
                await asyncio.sleep(poll_time)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(f"Error retrieving candles")


class TradingEngine(Service):

    def __init__(self, config: Config) -> None:
        self.config = config
        self.markets: dict[str, MarketManager] = {}
        self.strategies: dict[str, Strategy] = {}
        self.id = nanoid.generate(size=8)
        self.name = config.get("moonship.engine.name")
        if not isinstance(self.name, str):
            self.name = DEFAULT_ENGINE_NAME
        self.shared_cache: Optional[SharedCacheDataAccessor] = None
        self.message_bus: Optional[MessageBus] = None
        if config.get("moonship.redis") is not None:
            self.shared_cache = SharedCacheDataAccessor(RedisSharedCache(config))
            self.message_bus = RedisMessageBus(config)
        self.init_markets(config)
        self.init_strategies(config)

    def init_markets(self, config: Config) -> None:
        markets_config = config.get("moonship.markets")
        if not isinstance(markets_config, Config):
            raise ConfigException("No market configuration specified")
        for market_name, market_config in markets_config.items():
            symbol = market_config.get("symbol")
            if not isinstance(symbol, str):
                raise ConfigException(f"No symbol configured for {market_name} market")
            cls = self.load_class("client", market_config, MarketClient)
            client = cls(market_name, config)
            max_recent_trade_list_size = market_config.get("max_recent_trade_list_size")
            candle_period = market_config.get("candle_period", CandlePeriod.FIVE_MIN.value)
            if not isinstance(max_recent_trade_list_size, int):
                max_recent_trade_list_size = DEFAULT_MAX_RECENT_TRADE_LIST_SIZE
            self.markets[market_name] = \
                MarketManager(
                    Market(
                        market_name,
                        symbol,
                        client,
                        candle_period=candle_period),
                    max_recent_trade_list_size)

    def init_strategies(self, config: Config) -> None:
        strategies_config = config.get("moonship.strategies")
        if not isinstance(strategies_config, Config):
            raise ConfigException("No strategy configuration specified")
        for strategy_name in strategies_config.keys():
            self.strategies[strategy_name] = self.init_strategy(strategy_name, config)

    def init_strategy(self, name: str, app_config: Config) -> Strategy:
        strategy_config = app_config.get(f"moonship.strategies.{name}")
        if not isinstance(strategy_config, Config):
            raise ConfigException(f"No configuration for {name} strategy")
        market_names = strategy_config.get("markets")
        if not isinstance(market_names, list) or len(market_names) == 0:
            raise ConfigException(f"No markets configured for {name} strategy")
        markets: [str, Market] = {}
        for market_name in market_names:
            market = self.markets.get(market_name)
            if market is None:
                raise ConfigException(f"Invalid market specified for {name} strategy: {market_name}")
            markets[market_name] = market.market
        algo_class = self.load_class("algo", strategy_config, TradingAlgo)
        strategy = Strategy(
            name,
            self.name,
            self.id,
            algo_class,
            markets,
            self.shared_cache)
        strategy.init_config(app_config)
        return strategy

    async def start(self) -> None:
        if self.shared_cache is not None:
            await self.shared_cache.open()
            await self.shared_cache.add_engine(
                self.name,
                self.id,
                {s: self.config.get(f"moonship.strategies.{s}") for s in self.strategies.keys()})
            asyncio.create_task(self.purge_orphaned_cache_entries())
        if self.message_bus is not None:
            await self.message_bus.start()
            await self.message_bus.subscribe("moonship:message:request", self.on_msg_received)
        for strategy in self.strategies.values():
            if strategy.auto_start:
                await self.start_strategy(strategy.name)

    async def start_strategy(self, name: str) -> None:
        strategy = self.strategies.get(name)
        if strategy is not None and not strategy.active:
            for market_name in strategy.markets.keys():
                market = self.markets[market_name]
                if market.market.status == MarketStatus.CLOSED:
                    await market.open()
                    if market.market.status == MarketStatus.OPEN:
                        logger.info(f"Opened {market_name} market")
                    else:
                        logger.warning(f"Could not open {market_name} market")
                        await market.close()
            await strategy.start()
            logger.info(f"Started {strategy.name} strategy")

    async def stop(self) -> None:
        if self.message_bus is not None:
            await self.message_bus.close()
        for strategy_name in self.strategies.keys():
            await self.stop_strategy(strategy_name)
        for market in self.markets.values():
            if market.market.status != MarketStatus.CLOSED:
                await market.close()
                logger.info(f"Closed {market.market.name} market")
        if self.shared_cache is not None:
            await self.shared_cache.remove_engine(self.name, self.id, [s for s in self.strategies.keys()])
            await self.shared_cache.close()

    async def stop_strategy(self, name: str) -> None:
        strategy = self.strategies.get(name)
        if strategy is not None and strategy.active:
            await strategy.stop()
            logger.info(f"Stopped {strategy.name} strategy")

    async def on_msg_received(self, msg: dict[str, any], channel: str) -> None:
        if msg.get("engine") != self.name:
            return

        command = msg.get("command")
        if command is None:
            result = MessageResult.MISSING_OR_INVALID_PARAMETER
            rsp_data = {"parameter": "command"}
        else:
            try:
                match command:
                    case "add":
                        result, rsp_data = await self.on_configure_strategy_msg(msg, add=True)
                    case "update":
                        result, rsp_data = await self.on_configure_strategy_msg(msg, add=False)
                    case "remove":
                        result, rsp_data = await self.on_remove_strategy_msg(msg)
                    case "start":
                        result, rsp_data = await self.on_start_strategy_msg(msg)
                    case "stop":
                        result, rsp_data = await self.on_stop_strategy_msg(msg)
                    case _:
                        result = MessageResult.UNSUPPORTED
                        rsp_data = {}
            except Exception:
                logger.exception(f"Error processing {command} command")
                result = MessageResult.FAILED
                rsp_data = {}
        rsp = {
            "id": msg.get("id"),
            "result": result.value
        }
        rsp |= rsp_data
        await self.message_bus.publish(rsp, "moonship:message:response")

    async def on_configure_strategy_msg(self, msg: dict[str, any], add: bool) -> (MessageResult, dict[str, any]):
        name = msg.get("strategy")
        if name is None or (add and name in self.strategies) or (not add and name not in self.strategies):
            return MessageResult.MISSING_OR_INVALID_PARAMETER, {"parameter": "strategy"}
        config = msg.get("config")
        if not isinstance(config, dict):
            return MessageResult.MISSING_OR_INVALID_PARAMETER, {"parameter": "config"}
        try:
            new_app_config = self.config.copy()
            if not add:
                strategy_config = new_app_config.get(f"moonship.strategies.{name}")
                strategy_config |= config
                config = strategy_config
            new_app_config.set(f"moonship.strategies.{name}", config)
            strategy = self.init_strategy(name, new_app_config)
            auto_start = strategy.auto_start
            if not add:
                old_strategy = self.strategies[name]
                if old_strategy.active:
                    await self.stop_strategy(name)
                    auto_start = True
            self.strategies[name] = strategy
            self.config = new_app_config
            await self.shared_cache.add_strategy(
                name,
                self.config.get(f"moonship.strategies.{name}"),
                self.name,
                self.id)
            if auto_start:
                await self.start_strategy(name)
            return MessageResult.SUCCESS, {}
        except ConfigException:
            logger.exception(f"Failed to {'add' if add else 'update'} strategy {name}")
            return MessageResult.MISSING_OR_INVALID_PARAMETER, {"parameter": "config"}

    async def on_remove_strategy_msg(self, msg: dict[str, any]) -> (MessageResult, dict[str, any]):
        strategy = msg.get("strategy")
        if strategy is None or strategy not in self.strategies:
            return MessageResult.MISSING_OR_INVALID_PARAMETER, {"parameter": "strategy"}
        await self.stop_strategy(strategy)
        del self.strategies[strategy]
        self.config.remove(f"moonship.strategies.{strategy}")
        await self.shared_cache.remove_strategy(strategy, self.name, self.id)
        return MessageResult.SUCCESS, {}

    async def on_start_strategy_msg(self, msg: dict[str, any]) -> (MessageResult, dict[str, any]):
        strategy = msg.get("strategy")
        if strategy is None or strategy not in self.strategies:
            return MessageResult.MISSING_OR_INVALID_PARAMETER, {"parameter": "strategy"}
        await self.start_strategy(strategy)
        return MessageResult.SUCCESS, {}

    async def on_stop_strategy_msg(self, msg: dict[str, any]) -> (MessageResult, dict[str, any]):
        strategy = msg.get("strategy")
        if strategy is None or strategy not in self.strategies:
            return MessageResult.MISSING_OR_INVALID_PARAMETER, {"parameter": "strategy"}
        await self.stop_strategy(strategy)
        return MessageResult.SUCCESS, {}

    def load_class(self, key: str, config: Config, expected_type: type) -> type:
        class_name = config.get(key)
        if not isinstance(class_name, str):
            raise ConfigException(f"Missing configuration: {config.key}.{key}")
        cls, version = self.get_class_and_version(class_name)
        if cls is None or not issubclass(cls, expected_type):
            raise ConfigException(f"Invalid configuration: {config.key}.{key}")
        logger.info(f"Loaded {key} {class_name if version is None else f'{class_name} (version {version})'}")
        return cls

    def get_class_and_version(self, class_name: str) -> (Optional[type], Optional[str]):
        try:
            module_name, class_name = class_name.rsplit(".", 1)
            module = importlib.import_module(module_name)
            return getattr(module, class_name), getattr(module, "__version__", None)
        except Exception:
            logger.exception("Module load error")
            return None, None

    async def purge_orphaned_cache_entries(self) -> None:
        await asyncio.sleep(60)
        await self.shared_cache.purge_orphaned_engine_entries(self.name, self.id)
