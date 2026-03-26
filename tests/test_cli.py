"""Tests for annovar_db_sync.cli."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from annovar_db_sync.cli import _build_parser, main
from annovar_db_sync.logger import setup_logger

setup_logger()


class TestParser:
    def test_status_subcommand_parsed(self, tmp_annovar_dir: Path):
        parser = _build_parser()
        args = parser.parse_args(["status", "--annovar-dir", str(tmp_annovar_dir)])
        assert args.command == "status"
        assert args.annovar_dir == str(tmp_annovar_dir)

    def test_update_clinvar_defaults(self, tmp_annovar_dir: Path):
        parser = _build_parser()
        args = parser.parse_args(
            ["update-clinvar", "--annovar-dir", str(tmp_annovar_dir)]
        )
        assert args.genome_build == "both"
        assert args.dry_run is False
        assert args.force is False

    def test_install_intervar_with_version(self, tmp_annovar_dir: Path):
        parser = _build_parser()
        args = parser.parse_args(
            [
                "install-intervar",
                "--annovar-dir",
                str(tmp_annovar_dir),
                "--genome-build",
                "hg38",
                "--version",
                "20250721",
            ]
        )
        assert args.genome_build == "hg38"
        assert args.version == "20250721"

    def test_version_flag(self):
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--version"])
        assert exc.value.code == 0

    def test_no_command_exits(self):
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])


class TestCmdStatus:
    def test_status_no_databases_tracked(self, tmp_annovar_dir: Path, capsys):
        with patch("sys.exit") as mock_exit:
            main(["status", "--annovar-dir", str(tmp_annovar_dir)])
        captured = capsys.readouterr()
        assert "No databases tracked" in captured.out or "Status" in captured.out

    def test_status_exits_zero_on_success(self, tmp_annovar_dir: Path):
        with pytest.raises(SystemExit) as exc:
            main(["status", "--annovar-dir", str(tmp_annovar_dir)])
        assert exc.value.code == 0


class TestCmdUpdateClinvar:
    def test_dispatches_to_update_clinvar(self, tmp_annovar_dir: Path):
        with patch("annovar_db_sync.cli.update_clinvar", return_value=False) as mock_fn:
            with pytest.raises(SystemExit) as exc:
                main(
                    [
                        "update-clinvar",
                        "--annovar-dir",
                        str(tmp_annovar_dir),
                        "--genome-build",
                        "hg38",
                    ]
                )
            assert mock_fn.called
        assert exc.value.code == 0

    def test_dry_run_flag_passed(self, tmp_annovar_dir: Path):
        with patch("annovar_db_sync.cli.update_clinvar", return_value=True) as mock_fn:
            with pytest.raises(SystemExit):
                main(
                    [
                        "update-clinvar",
                        "--annovar-dir",
                        str(tmp_annovar_dir),
                        "--dry-run",
                    ]
                )
            _, kwargs = mock_fn.call_args
            assert kwargs.get("dry_run") is True

    def test_force_flag_passed(self, tmp_annovar_dir: Path):
        with patch("annovar_db_sync.cli.update_clinvar", return_value=True) as mock_fn:
            with pytest.raises(SystemExit):
                main(
                    [
                        "update-clinvar",
                        "--annovar-dir",
                        str(tmp_annovar_dir),
                        "--force",
                    ]
                )
            _, kwargs = mock_fn.call_args
            assert kwargs.get("force") is True

    def test_both_builds_calls_update_twice(self, tmp_annovar_dir: Path):
        with patch("annovar_db_sync.cli.update_clinvar", return_value=True) as mock_fn:
            with pytest.raises(SystemExit):
                main(
                    [
                        "update-clinvar",
                        "--annovar-dir",
                        str(tmp_annovar_dir),
                        "--genome-build",
                        "both",
                    ]
                )
            assert mock_fn.call_count == 2


class TestCmdInstallIntervar:
    def test_dispatches_to_install_intervar(self, tmp_annovar_dir: Path):
        with patch(
            "annovar_db_sync.cli.install_intervar_db", return_value=True
        ) as mock_fn:
            with pytest.raises(SystemExit) as exc:
                main(
                    [
                        "install-intervar",
                        "--annovar-dir",
                        str(tmp_annovar_dir),
                        "--genome-build",
                        "hg38",
                        "--version",
                        "20250721",
                    ]
                )
            assert mock_fn.called
        assert exc.value.code == 0

    def test_returns_error_when_install_fails(self, tmp_annovar_dir: Path):
        with patch(
            "annovar_db_sync.cli.install_intervar_db", return_value=False
        ):
            with pytest.raises(SystemExit) as exc:
                main(
                    [
                        "install-intervar",
                        "--annovar-dir",
                        str(tmp_annovar_dir),
                        "--genome-build",
                        "hg38",
                    ]
                )
        assert exc.value.code == 1

    def test_invalid_annovar_dir_exits_1(self, tmp_path: Path):
        with pytest.raises(SystemExit) as exc:
            main(
                [
                    "install-intervar",
                    "--annovar-dir",
                    str(tmp_path / "nonexistent"),
                ]
            )
        assert exc.value.code == 1


class TestCmdCheck:
    def test_check_with_no_tracked_files(self, tmp_annovar_dir: Path, capsys):
        with pytest.raises(SystemExit) as exc:
            main(["check", "--annovar-dir", str(tmp_annovar_dir)])
        assert exc.value.code == 0

    def test_check_detects_missing_file(
        self, tmp_annovar_dir: Path, empty_lock_file: Path, capsys
    ):
        from annovar_db_sync.lock_file import record_clinvar_install, write_lock_file, create_empty_lock
        lock = record_clinvar_install(
            create_empty_lock(),
            "hg38",
            "20250721",
            "abc",
            "abc",
            "url",
            ["humandb/hg38_clinvar_20250721.txt"],
        )
        write_lock_file(empty_lock_file, lock)

        with pytest.raises(SystemExit) as exc:
            main(
                [
                    "check",
                    "--annovar-dir",
                    str(tmp_annovar_dir),
                    "--lock-file",
                    str(empty_lock_file),
                ]
            )
        assert exc.value.code == 2
        captured = capsys.readouterr()
        assert "FAIL" in captured.out or "MISSING" in captured.out
