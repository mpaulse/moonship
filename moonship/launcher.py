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

import asyncio
import logging
import logging.config
import os
import os.path
import sys
import yaml

from .tradeengine import TradeEngine

logger = logging.getLogger(__package__)


def load_config() -> dict:
    config = {
        "moonship": {
        }
    }
    config_file = "config.yml" if len(sys.argv) < 2 else sys.argv[1]
    if os.path.isfile(config_file):
        with open(config_file, "r") as config_file:
            config = yaml.safe_load(config_file)
    return config


def configure_logging(app_config: dict) -> None:
    logging_config = {
        "version": 1,
        "root": {
            "handlers": ["stdout"]
        },
        "loggers": {
            "moonship": {
                "level": "INFO"
            }
        },
        "handlers": {
            "stdout": {
                "class": "logging.StreamHandler",
                "formatter": "log",
                "stream": "ext://sys.stdout"
            }
        },
        "formatters": {
            "log": {
                "format": "[%(asctime)s] %(levelname)s - %(name)s: %(message)s"
            }
        },
        "disable_existing_loggers": False
    }

    config = app_config.get("moonship").get("logging")
    if config is not None:
        logging_config |= config

    handlers: dict = logging_config.get("handlers")
    if handlers is not None:
        for handler in handlers.values():
            log_file_path = handler.get("filename")
            if log_file_path is not None:
                log_dir = os.path.dirname(log_file_path)
                if not os.path.isdir(log_dir):
                    os.makedirs(log_dir, exist_ok=True)

    logging.config.dictConfig(logging_config)


def launch():
    config = load_config()
    configure_logging(config)

    logger.info(
        """Launching...
        
            __  ___                       __    _     
           /  |/  /___  ____  ____  _____/ /_  (_)___ 
          / /|_/ / __ \/ __ \/ __ \/ ___/ __ \/ / __ \\
         / /  / / /_/ / /_/ / / / (__  ) / / / / /_/ /
        /_/  /_/\____/\____/_/ /_/____/_/ /_/_/ .___/ 
                                             /_/      
        """)

    event_loop = asyncio.get_event_loop()

    engine = TradeEngine(config)
    event_loop.create_task(engine.start())

    try:
        event_loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down.")
        event_loop.close()


if __name__ == "__main__":
    launch()
