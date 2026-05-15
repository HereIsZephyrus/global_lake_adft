"""Shared logger bootstrap for CLI commands."""

import logging
import os
from datetime import datetime

from lakesource._version import log_runtime_version


class Logger:
    def __init__(self, name: str, log_dir: str = "logs", level: int = None, *, log_version: bool = True):
        log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
        level = level if level is not None else getattr(logging, log_level_str, logging.INFO)
        self.name = name
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"{name}_{timestamp}.log")

        logger_name = f"{name}_{timestamp}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(level)
        self.logger.propagate = False

        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        root = logging.getLogger()
        root.setLevel(level)
        if not any(
            isinstance(handler, logging.FileHandler)
            and getattr(handler, "baseFilename", None) == file_handler.baseFilename
            for handler in root.handlers
        ):
            root.addHandler(file_handler)
        if not any(
            isinstance(handler, logging.StreamHandler)
            and not isinstance(handler, logging.FileHandler)
            for handler in root.handlers
        ):
            root.addHandler(console_handler)

        self.log_file = log_file

        if log_version:
            log_runtime_version(self.logger.info)

    def debug(self, msg: str, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
