# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from __future__ import annotations

import re
import subprocess
from pathlib import Path

from .config import INTERVAR_DEFAULT_VERSION, SyncConfig
from .lock_file import read_lock_file, record_intervar_install, write_lock_file
from .logger import get_logger

# InterVar ANNOVAR db version string must be exactly 8 digits (YYYYMMDD)
_VERSION_RE = re.compile(r"^\d{8}$")


def install_intervar_db(
    config: SyncConfig,
    build: str,
    version: str | None = None,
) -> bool:
    """Download and install an InterVar ANNOVAR database for *build*.

    Uses ``annotate_variation.pl -downdb -webfrom annovar`` to download the
    database directly from ANNOVAR's server.

    Parameters
    ----------
    config:
        Runtime configuration.
    build:
        ``"hg19"`` or ``"hg38"``.
    version:
        Database version date string, e.g. ``"20250721"``.  Defaults to the
        recommended version for the given build if ``None``.

    Returns
    -------
    bool
        ``True`` on success.
    """
    logger = get_logger()

    if version is None:
        version = INTERVAR_DEFAULT_VERSION.get(build, "20180118")
        logger.info(
            "No version specified for %s; using default: intervar_%s", build, version
        )

    if not _VERSION_RE.match(version):
        logger.error(
            "Invalid InterVar version %r – must be 8 digits (YYYYMMDD).", version
        )
        return False

    db_name = f"intervar_{version}"
    logger.info(
        "Installing InterVar ANNOVAR db '%s' for %s via annotate_variation.pl ...",
        db_name,
        build,
    )

    cmd = [
        "perl",
        str(config.annovar_paths.annotate_variation),
        f"-buildver",
        build,
        "-downdb",
        "-webfrom",
        "annovar",
        db_name,
        str(config.annovar_paths.humandb_dir) + "/",
    ]
    logger.debug("Running: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(
            "annotate_variation.pl failed (rc=%d):\n%s",
            result.returncode,
            result.stderr[:3000],
        )
        return False

    logger.info("annotate_variation.pl completed successfully.")

    # Discover installed files
    installed_files = _find_intervar_files(
        config.annovar_paths.humandb_dir, build, version
    )
    if not installed_files:
        logger.warning(
            "No InterVar files found in %s matching %s_%s* after install. "
            "The download may have failed silently.",
            config.annovar_paths.humandb_dir,
            build,
            db_name,
        )

    relative_files = [
        str(Path(f).relative_to(config.annovar_paths.annovar_dir))
        if Path(f).is_absolute()
        else f
        for f in installed_files
    ]

    lock_data = read_lock_file(config.lock_file_path)
    updated_lock = record_intervar_install(
        lock_data,
        build=build,
        version=version,
        files=relative_files,
    )
    write_lock_file(config.lock_file_path, updated_lock)

    logger.info(
        "InterVar db '%s' (%s) installed. %d file(s) tracked.",
        db_name,
        build,
        len(relative_files),
    )
    return True


def _find_intervar_files(humandb_dir: Path, build: str, version: str) -> list[str]:
    """Glob *humandb_dir* for files matching ``{build}_intervar_{version}*``."""
    pattern = f"{build}_intervar_{version}*"
    return [str(p) for p in sorted(humandb_dir.glob(pattern))]
