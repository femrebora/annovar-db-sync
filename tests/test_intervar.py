"""Tests for annovar_db_sync.intervar."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from annovar_db_sync.intervar import _find_intervar_files, install_intervar_db
from annovar_db_sync.config import SyncConfig
from annovar_db_sync.lock_file import get_installed_version, read_lock_file
from annovar_db_sync.logger import setup_logger

setup_logger()


class TestFindIntervarFiles:
    def test_finds_matching_files(self, tmp_path: Path):
        (tmp_path / "hg38_intervar_20250721.txt").touch()
        (tmp_path / "hg38_intervar_20250721.txt.idx").touch()
        (tmp_path / "hg38_clinvar_20250721.txt").touch()  # should not match
        found = _find_intervar_files(tmp_path, "hg38", "20250721")
        basenames = [Path(f).name for f in found]
        assert "hg38_intervar_20250721.txt" in basenames
        assert "hg38_intervar_20250721.txt.idx" in basenames
        assert "hg38_clinvar_20250721.txt" not in basenames

    def test_returns_empty_when_none_found(self, tmp_path: Path):
        assert _find_intervar_files(tmp_path, "hg38", "20250721") == []


class TestInstallIntervarDb:
    def test_invalid_version_returns_false(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        config = SyncConfig.build(
            annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file
        )
        result = install_intervar_db(config, "hg38", version="not-a-date")
        assert result is False

    def test_successful_install_updates_lock(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        config = SyncConfig.build(
            annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file
        )
        # Create fake output file so _find_intervar_files can detect it
        fake_output = config.annovar_paths.humandb_dir / "hg38_intervar_20250721.txt"

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""

        with patch("annovar_db_sync.intervar.subprocess.run", return_value=mock_result):
            fake_output.touch()  # simulate annotate_variation.pl creating the file
            ok = install_intervar_db(config, "hg38", version="20250721")

        assert ok is True
        lock = read_lock_file(empty_lock_file)
        assert get_installed_version(lock, "intervar", "hg38") == "20250721"

    def test_subprocess_failure_returns_false(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        config = SyncConfig.build(
            annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file
        )
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Error: resource not found"

        with patch("annovar_db_sync.intervar.subprocess.run", return_value=mock_result):
            ok = install_intervar_db(config, "hg38", version="20250721")

        assert ok is False

    def test_default_version_used_when_none_given(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        from annovar_db_sync.config import INTERVAR_DEFAULT_VERSION
        config = SyncConfig.build(
            annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file
        )
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        called_with = {}

        def capture(*args, **kwargs):
            called_with["cmd"] = args[0]
            return mock_result

        with patch("annovar_db_sync.intervar.subprocess.run", side_effect=capture):
            install_intervar_db(config, "hg19", version=None)

        default = INTERVAR_DEFAULT_VERSION["hg19"]
        assert any(f"intervar_{default}" in arg for arg in called_with.get("cmd", []))
