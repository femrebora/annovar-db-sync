# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import LOCK_SCHEMA_VERSION
from .logger import get_logger


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_empty_lock() -> dict[str, Any]:
    """Return a brand-new, empty lock structure."""
    return {
        "schema_version": LOCK_SCHEMA_VERSION,
        "last_updated": _now_iso(),
        "databases": {
            "clinvar": {},
            "intervar": {},
        },
    }


def read_lock_file(path: Path) -> dict[str, Any]:
    """Read and parse *path*.

    Returns an empty lock structure if the file does not exist.
    Raises ``json.JSONDecodeError`` on malformed JSON.
    """
    if not path.exists():
        get_logger().debug("Lock file not found at %s; starting fresh.", path)
        return create_empty_lock()
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def write_lock_file(path: Path, data: dict[str, Any]) -> None:
    """Atomically write *data* as JSON to *path*.

    Writes to a ``.tmp`` sibling first, then calls ``os.replace`` to ensure
    the final file is never partially written.
    """
    tmp = path.with_suffix(".json.tmp")
    updated = copy.deepcopy(data)
    updated["last_updated"] = _now_iso()
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(updated, fh, indent=2)
        fh.write("\n")
    os.replace(tmp, path)
    get_logger().debug("Lock file written to %s", path)


def record_clinvar_install(
    lock_data: dict[str, Any],
    build: str,
    version: str,
    md5_remote: str,
    md5_local: str,
    source_url: str,
    files: list[str],
) -> dict[str, Any]:
    """Return a *new* lock dict with the ClinVar entry for *build* added/updated.

    Does not mutate *lock_data*.
    """
    updated = copy.deepcopy(lock_data)
    updated.setdefault("databases", {}).setdefault("clinvar", {})[build] = {
        "version": version,
        "source": "ncbi_ftp",
        "source_url": source_url,
        "md5_remote": md5_remote,
        "md5_local": md5_local,
        "installed_date": _now_iso(),
        "files": files,
    }
    return updated


def record_intervar_install(
    lock_data: dict[str, Any],
    build: str,
    version: str,
    files: list[str],
) -> dict[str, Any]:
    """Return a *new* lock dict with the InterVar entry for *build* added/updated.

    Does not mutate *lock_data*.
    """
    updated = copy.deepcopy(lock_data)
    updated.setdefault("databases", {}).setdefault("intervar", {})[build] = {
        "version": version,
        "source": "annovar_webfrom",
        "installed_date": _now_iso(),
        "files": files,
    }
    return updated


def validate_lock_schema(lock_data: dict[str, Any]) -> list[str]:
    """Return a list of validation error strings (empty list if valid)."""
    errors: list[str] = []
    if not isinstance(lock_data, dict):
        return ["Lock data is not a dict"]

    sv = lock_data.get("schema_version")
    if sv != LOCK_SCHEMA_VERSION:
        errors.append(
            f"schema_version is {sv!r}, expected {LOCK_SCHEMA_VERSION}"
        )
    if "databases" not in lock_data:
        errors.append("Missing 'databases' key")
    return errors


def get_installed_version(
    lock_data: dict[str, Any], db_type: str, build: str
) -> str | None:
    """Return the installed version string for *db_type*/*build*, or None."""
    return (
        lock_data.get("databases", {})
        .get(db_type, {})
        .get(build, {})
        .get("version")
    )


def get_installed_files(
    lock_data: dict[str, Any], db_type: str, build: str
) -> list[str]:
    """Return the list of installed file paths for *db_type*/*build*."""
    return (
        lock_data.get("databases", {})
        .get(db_type, {})
        .get(build, {})
        .get("files", [])
    )
