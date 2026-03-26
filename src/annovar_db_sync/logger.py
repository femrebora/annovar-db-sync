# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from __future__ import annotations

import logging
import sys

_LOGGER_NAME = "annovar_db_sync"
_FMT = "[%(levelname)s] %(asctime)s - %(message)s"
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_logger(verbose: bool = False) -> logging.Logger:
    """Return a configured logger for the package.

    Safe to call multiple times – attaches a handler only on first call.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_FMT, datefmt=_DATE_FMT))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logger


def get_logger() -> logging.Logger:
    """Return the package logger (must call setup_logger first)."""
    return logging.getLogger(_LOGGER_NAME)
