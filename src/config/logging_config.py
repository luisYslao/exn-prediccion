import logging
from config.settings import settings


def _setup_logging():
    valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    level_str = settings.LOG_LEVEL.upper()
    log_level = (
        getattr(logging, level_str, logging.INFO)
        if level_str in valid_levels
        else logging.INFO
    )

    root_logger = logging.getLogger()
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s]: %(message)s"
    )

    return logging.getLogger(__name__)


# Do not run this file directly.
if __name__ != "__main__":
    logger = _setup_logging()
else:
    raise RuntimeError("This file is not meant to be executed directly.")
