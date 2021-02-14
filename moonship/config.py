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

import os
import yaml

from collections import KeysView
from typing import Union

__all__ = [
    "Config"
]


class Config:

    def __init__(self, config_dict: dict) -> None:
        self.dict = config_dict

    def __ior__(self, other: "Config") -> "Config":
        self.dict |= other.dict
        return self

    def get(self, key: str) -> Union["Config", any]:
        keys = key.split(".")
        value = self.dict
        for i in range(0, len(keys)):
            value = value.get(keys[i])
            if value is None or (not isinstance(value, dict) and i < len(keys) - 1):
                return None
        if isinstance(value, dict):
            value = Config(value)
        return value

    @property
    def children_keys(self) -> KeysView[str]:
        return self.dict.keys()

    @staticmethod
    def load_from_file(config_filename: str) -> "Config":
        config = {
            "moonship": {
            }
        }
        if os.path.isfile(config_filename):
            with open(config_filename, "r") as config_file:
                config = yaml.safe_load(config_file)
        return Config(config)
