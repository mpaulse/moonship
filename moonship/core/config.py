#  Copyright (c) 2025 Marlon Paulse
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

from __future__ import annotations

import copy
import os
import yaml

from moonship.core.error import ConfigException
from typing import Any, ItemsView, Iterator, KeysView

__all__ = [
    "Config",
    "ConfigItemsView"
]


def convert_config_value(value: Any, parent_key: str, key: str) -> Any:
    if isinstance(value, dict):
        value = Config(value, f"{parent_key}.{key}" if parent_key is not None else key)
    elif isinstance(value, float):
        value = str(value)
    return value


class ConfigItemsView(ItemsView):

    def __init__(self, config_dict: dict, key: str):
        super().__init__(config_dict)
        self.key = key

    def __contains__(self, item) -> bool:
        key, value = item
        try:
            v = self._mapping[key]
        except KeyError:
            return False
        else:
            return v is value or v == value

    def __iter__(self) -> Iterator[tuple[str, Config | Any]]:
        for key in self._mapping:
            value = convert_config_value(self._mapping[key], self.key, key)
            yield key, value


class Config:

    def __init__(self, config_dict: dict[str, Any] = None, key: str = None) -> None:
        if config_dict is None:
            config_dict = {}
        self._dict = config_dict
        self._key = key

    @property
    def dict(self) -> dict[str, Any]:
        return self._dict

    @property
    def key(self) -> str:
        return self._key

    def __ior__(self, other: Config | dict) -> Config:
        if isinstance(other, dict):
            self._dict |= other
        else:
            self._dict |= other._dict
        return self

    def __iter__(self) -> Iterator[Any]:
        return iter(self._dict)

    def __contains__(self, key) -> bool:
        return not self.get(key) is None

    def keys(self) -> KeysView:
        return self._dict.keys()

    def items(self) -> ItemsView[str, Config | Any]:
        return ConfigItemsView(self._dict, self._key)

    def get(self, key: str, default: Any = None) -> Config | Any:
        keys = key.split(".")
        value = self._dict
        for i in range(0, len(keys)):
            value = value.get(keys[i])
            if value is None or (not isinstance(value, dict) and i < len(keys) - 1):
                return default
        return convert_config_value(value, self._key, key)

    def set(self, key: str, value: Any) -> None:
        keys = key.split(".")
        parent = self._dict
        for i in range(0, len(keys) - 1):
            p = parent.get(keys[i])
            if p is None:
                p = {}
                parent[keys[i]] = p
            elif not isinstance(parent, dict):
                raise ConfigException(f"{'.'.join(keys[0:i+1])} is not a dict value")
            parent = p
        parent[keys[-1]] = value if not isinstance(value, Config) else value.dict

    def remove(self, key: str) -> None:
        keys = key.split(".")
        parent = self._dict
        for i in range(0, len(keys) - 1):
            parent = parent.get(keys[i])
            if parent is None:
                break
        if isinstance(parent, dict):
            del parent[keys[-1]]

    def copy(self) -> Config:
        return Config(copy.deepcopy(self._dict), self._key)

    @staticmethod
    def load_from_file(config_filename: str) -> Config:
        config = {
            "moonship": {
            }
        }
        if os.path.isfile(config_filename):
            with open(config_filename, "r") as config_file:
                config = yaml.safe_load(config_file)
        return Config(config)
