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

import abc
import aiosmtplib
import asyncio
import datetime
import json
import logging
import rule_engine

from email.mime.text import MIMEText
from moonship.core import *
from moonship.core.ipc import SharedCacheDataAccessor
from moonship.core.redis import RedisSharedCache
from moonship.core.service import Service
from typing import Optional

__all__ = [
    "Monitor"
]

logger = logging.getLogger(__name__)


class Action(abc.ABC):

    @abc.abstractmethod
    async def execute(self, alert: "Alert", strategy_data: dict[str, any]) -> None:
        pass


class EmailAction(Action):

    def __init__(self, config: Config) -> None:
        email_config = config.get("moonship.monitor.email")
        if not isinstance(email_config, Config):
            raise ConfigException("No email configuration specified")
        self._smtp_host = email_config.get("smtp_host")
        if not isinstance(self._smtp_host, str):
            raise ConfigException("No SMTP host configuration specified")
        self._smtp_port = email_config.get("smtp_port", 587)
        if not isinstance(self._smtp_port, int) or self._smtp_port < 0 or self._smtp_port > 65535:
            raise ConfigException(f"Invalid email SMTP port configuration: {self._smtp_port}")
        self._username = email_config.get("username")
        if not isinstance(self._username, str):
            raise ConfigException("No email username configuration specified")
        self._password = email_config.get("password")
        if not isinstance(self._password, str):
            raise ConfigException("No email password configuration specified")
        self._from_address = email_config.get("from")
        if not isinstance(self._from_address, str):
            raise ConfigException("No email from-address configuration specified")
        self._to_addresses = email_config.get("to")
        if not isinstance(self._to_addresses, str) and not isinstance(self._to_addresses, list):
            raise ConfigException("No email to-address configuration specified")
        if isinstance(self._to_addresses, str):
            self._to_addresses = [self._to_addresses]

    async def execute(self, alert: "Alert", strategy_data: dict[str, any]) -> None:
        logger.debug(f"Sending email to {self._to_addresses} for {alert.name} alert")
        msg = MIMEText(
f"""Hello,

The {alert.name} alert was triggered for the {strategy_data['name']} strategy running on {strategy_data['engine']}.

Alert time:
{datetime.datetime.now(datetime.timezone.utc)}

Strategy data:
{json.dumps(strategy_data, indent=4)}
""")
        msg["Subject"] = f"Moonship alert: {alert.name} - {strategy_data['name']}@{strategy_data['engine']}"
        msg["From"] = f"Moonship Monitor <{self._from_address}>"
        msg["To"] = ", ".join(self._to_addresses)
        await aiosmtplib.send(
            msg,
            hostname=self._smtp_host,
            port=self._smtp_port,
            start_tls=True,
            username=self._username,
            password=self._password)


class Alert:

    @property
    def name(self) -> str:
        return self._name

    def __init__(self, name: str, condition: str, actions: list[Action]) -> None:
        self._name = name
        try:
            self._rule = rule_engine.Rule(
                condition,
                rule_engine.Context(default_timezone="utc", default_value=None))
        except Exception as e:
            raise ConfigException(f"Invalid condition configured for {name} alert: {condition}") from e
        self._actions = actions
        self._matched_strategy_refs: set[str] = set()

    async def process(self, strategies_data: list[dict[str, any]]) -> None:
        for strategy_data in strategies_data:
            strategy_ref = f"{strategy_data.get("name")}@{strategy_data.get("engine")}"
            if self._rule.matches(strategy_data):
                if strategy_ref not in self._matched_strategy_refs:
                    self._matched_strategy_refs.add(strategy_ref)
                    logger.info(f"{self._name} alert triggered for {strategy_ref} strategy")
                    for action in self._actions:
                        await action.execute(self, strategy_data)
            elif strategy_ref in self._matched_strategy_refs:
                self._matched_strategy_refs.remove(strategy_ref)


class Monitor(Service):

    def __init__(self, config: Config) -> None:
        self._poll_interval_sec = config.get("moonship.monitor.poll_interval", 5)
        if not isinstance(self._poll_interval_sec, int) or self._poll_interval_sec <= 0:
            raise ConfigException("Invalid monitor poll interval")
        self._actions: dict[str, Action] = {}
        self._init_actions(config)
        self._alerts: list[Alert] = []
        self._init_alerts(config)
        self._shared_cache = SharedCacheDataAccessor(RedisSharedCache(config))
        self._run_task: Optional[asyncio.Task] = None

    def _init_actions(self, config: Config) -> None:
        if "moonship.monitor.email" in config:
            self._actions["email"] = EmailAction(config)

    def _init_alerts(self, config: Config) -> None:
        alerts_config = config.get("moonship.monitor.alerts")
        if not isinstance(alerts_config, Config):
            raise ConfigException("No monitor alert configuration specified")
        for alert_name, alert_config in alerts_config.items():
            condition = alert_config.get("condition")
            if not isinstance(condition, str):
                raise ConfigException(f"No condition configured for {alert_name} alert")
            actions: list[Action] = []
            action_types = alert_config.get("actions", [])
            if isinstance(action_types, str):
                action_types = [action_types]
            for action_type in action_types:
                action = self._actions.get(action_type)
                if action is None:
                    raise ConfigException(f"Invalid action configured for {alert_name} alert: {action_type}")
                actions.append(action)
            if len(actions) == 0:
                logger.warning(f"No actions configured for {alert_name} alert")
            self._alerts.append(Alert(alert_name, condition, actions))

    async def start(self) -> None:
        await self._shared_cache.open()
        self._run_task = asyncio.create_task(self._run())
        logger.info("Started")

    async def stop(self) -> None:
        self._run_task.cancel()
        await self._shared_cache.close()

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._poll_interval_sec)
            strategies = await self._shared_cache.get_strategies()
            for alert in self._alerts:
                try:
                    await alert.process(strategies)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(f"Error processing {alert.name} alert")
