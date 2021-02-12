import asyncio
import logging
import logging.config
import os
import os.path
import yaml

logger = logging.getLogger(__name__)


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


async def run(app_config: dict) -> None:
    logger.info("To The Moon!")


def launch():
    if os.path.isfile("config.yml"):
        with open("config.yml", "r") as config_file:
            config = yaml.safe_load(config_file)

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
    asyncio.run(run(config))


if __name__ == "__main__":
    launch()
