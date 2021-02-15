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
import sys

from .config import *
from .engine import TradeEngine
from .error import *

logger = logging.getLogger(__package__)


def configure_logging(app_config: Config) -> None:
    logging_config = Config({
        "version": 1,
        "root": {
            "handlers": ["stdout"]
        },
        "loggers": {
            "moonship": {
                "level": "INFO"
            },
            "asyncio": {
                "level": "CRITICAL"
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
    })
    config = app_config.get("moonship.logging")
    if isinstance(config, Config):
        logging_config |= config
    handlers = logging_config.get("handlers")
    if isinstance(handlers, Config):
        for handler_name, handler_config in handlers.items():
            log_file_path = handler_config.get("filename")
            if isinstance(log_file_path, str):
                log_dir = os.path.dirname(log_file_path)
                if not os.path.isdir(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
    logging.config.dictConfig(logging_config.dict)


def launch():
    config = Config.load_from_file("config.yml" if len(sys.argv) < 2 else sys.argv[1])
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

    engine = None
    event_loop = asyncio.get_event_loop()
    try:
        engine = TradeEngine(config)
        event_loop.create_task(engine.start())
        event_loop.run_forever()
    except StartUpException:
        logger.exception("Critical start-up error!")
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down.")
        if isinstance(engine, TradeEngine):
            event_loop.run_until_complete(engine.stop())
        event_loop.run_until_complete(asyncio.sleep(0.5))
        event_loop.close()


if __name__ == "__main__":
    launch()
