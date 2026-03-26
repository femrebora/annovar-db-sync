# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from __future__ import annotations

import hashlib
from pathlib import Path

import requests

from .logger import get_logger

_CHUNK = 65_536  # 64 KiB


class DownloadError(Exception):
    """Raised when an HTTP download fails."""


def download_file(url: str, dest: Path, chunk_size: int = _CHUNK) -> Path:
    """Stream *url* to *dest* on disk.

    Parameters
    ----------
    url:
        Remote resource URL.
    dest:
        Local destination path. Parent directory must already exist.
    chunk_size:
        Read/write chunk size in bytes.

    Returns
    -------
    Path
        The destination path (same as *dest*).

    Raises
    ------
    DownloadError
        On non-200 HTTP status or connection error.
    """
    logger = get_logger()
    logger.info("Downloading %s -> %s", url, dest)
    try:
        with requests.get(url, stream=True, timeout=120) as resp:
            if resp.status_code != 200:
                raise DownloadError(
                    f"HTTP {resp.status_code} when downloading {url}"
                )
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=chunk_size):
                    fh.write(chunk)
    except requests.RequestException as exc:
        raise DownloadError(f"Connection error downloading {url}: {exc}") from exc
    logger.debug("Saved %s bytes to %s", dest.stat().st_size, dest)
    return dest


def fetch_text(url: str, timeout: int = 30) -> str:
    """GET *url* and return the decoded response body as a string.

    Raises
    ------
    DownloadError
        On non-200 HTTP status or connection error.
    """
    try:
        resp = requests.get(url, timeout=timeout)
    except requests.RequestException as exc:
        raise DownloadError(f"Connection error fetching {url}: {exc}") from exc
    if resp.status_code != 200:
        raise DownloadError(f"HTTP {resp.status_code} fetching {url}")
    return resp.text


def compute_md5(file_path: Path, block_size: int = _CHUNK) -> str:
    """Return the lower-case hex MD5 digest of *file_path*."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as fh:
        buf = fh.read(block_size)
        while buf:
            hasher.update(buf)
            buf = fh.read(block_size)
    return hasher.hexdigest()


def verify_md5(file_path: Path, expected_md5: str) -> bool:
    """Return True if the file's MD5 matches *expected_md5* (case-insensitive)."""
    return compute_md5(file_path).lower() == expected_md5.strip().lower()
