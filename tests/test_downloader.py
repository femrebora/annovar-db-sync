"""Tests for annovar_db_sync.downloader."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import responses

import requests as _requests

from annovar_db_sync.downloader import (
    DownloadError,
    compute_md5,
    download_file,
    fetch_text,
    verify_md5,
)
from annovar_db_sync.logger import setup_logger

setup_logger()


class TestComputeMd5:
    def test_known_content(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        content = b"hello world"
        f.write_bytes(content)
        expected = hashlib.md5(content).hexdigest()
        assert compute_md5(f) == expected

    def test_empty_file(self, tmp_path: Path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")
        expected = hashlib.md5(b"").hexdigest()
        assert compute_md5(f) == expected


class TestVerifyMd5:
    def test_matching_md5_returns_true(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")
        md5 = compute_md5(f)
        assert verify_md5(f, md5) is True

    def test_wrong_md5_returns_false(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")
        assert verify_md5(f, "deadbeef" * 4) is False

    def test_case_insensitive(self, tmp_path: Path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"data")
        md5 = compute_md5(f).upper()
        assert verify_md5(f, md5) is True


class TestDownloadFile:
    @responses.activate
    def test_successful_download(self, tmp_path: Path):
        url = "https://example.com/file.txt"
        content = b"file content"
        responses.add(responses.GET, url, body=content, status=200)
        dest = tmp_path / "file.txt"
        result = download_file(url, dest)
        assert result == dest
        assert dest.read_bytes() == content

    @responses.activate
    def test_http_error_raises(self, tmp_path: Path):
        url = "https://example.com/missing.txt"
        responses.add(responses.GET, url, status=404)
        with pytest.raises(DownloadError, match="404"):
            download_file(url, tmp_path / "missing.txt")

    @responses.activate
    def test_connection_error_raises_download_error(self, tmp_path: Path):
        url = "https://example.com/crash.txt"
        responses.add(responses.GET, url, body=_requests.exceptions.ConnectionError("refused"))
        with pytest.raises(DownloadError, match="Connection error"):
            download_file(url, tmp_path / "crash.txt")


class TestFetchText:
    @responses.activate
    def test_returns_text(self):
        url = "https://example.com/page.html"
        responses.add(responses.GET, url, body="<html>content</html>", status=200)
        result = fetch_text(url)
        assert result == "<html>content</html>"

    @responses.activate
    def test_http_error_raises(self):
        url = "https://example.com/error"
        responses.add(responses.GET, url, status=500)
        with pytest.raises(DownloadError, match="500"):
            fetch_text(url)

    @responses.activate
    def test_connection_error_raises(self):
        url = "https://example.com/timeout"
        responses.add(responses.GET, url, body=_requests.exceptions.ConnectionError("Connection refused"))
        with pytest.raises(DownloadError, match="Connection error"):
            fetch_text(url)
