"""Tests for annovar_db_sync.lock_file."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from annovar_db_sync.lock_file import (
    create_empty_lock,
    get_installed_files,
    get_installed_version,
    read_lock_file,
    record_clinvar_install,
    record_intervar_install,
    validate_lock_schema,
    write_lock_file,
)
from annovar_db_sync.config import LOCK_SCHEMA_VERSION


class TestCreateEmptyLock:
    def test_has_required_keys(self):
        lock = create_empty_lock()
        assert "schema_version" in lock
        assert "last_updated" in lock
        assert "databases" in lock

    def test_schema_version_matches_config(self):
        lock = create_empty_lock()
        assert lock["schema_version"] == LOCK_SCHEMA_VERSION

    def test_databases_has_clinvar_and_intervar(self):
        lock = create_empty_lock()
        assert "clinvar" in lock["databases"]
        assert "intervar" in lock["databases"]


class TestReadWriteLockFile:
    def test_read_missing_file_returns_empty_lock(self, tmp_path: Path):
        lock = read_lock_file(tmp_path / "nonexistent.json")
        assert lock["schema_version"] == LOCK_SCHEMA_VERSION

    def test_write_read_roundtrip(self, tmp_path: Path):
        path = tmp_path / "lock.json"
        original = create_empty_lock()
        write_lock_file(path, original)
        loaded = read_lock_file(path)
        assert loaded["schema_version"] == original["schema_version"]
        assert loaded["databases"] == original["databases"]

    def test_write_is_atomic(self, tmp_path: Path):
        """No .tmp file should be left after successful write."""
        path = tmp_path / "lock.json"
        write_lock_file(path, create_empty_lock())
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []

    def test_write_updates_last_updated(self, tmp_path: Path):
        path = tmp_path / "lock.json"
        lock = create_empty_lock()
        write_lock_file(path, lock)
        loaded = read_lock_file(path)
        assert loaded["last_updated"] >= lock["last_updated"]

    def test_read_malformed_json_raises(self, tmp_path: Path):
        path = tmp_path / "bad.json"
        path.write_text("{not valid json")
        with pytest.raises(json.JSONDecodeError):
            read_lock_file(path)


class TestRecordClinvarInstall:
    def test_adds_clinvar_entry(self):
        lock = create_empty_lock()
        updated = record_clinvar_install(
            lock,
            build="hg38",
            version="20250721",
            md5_remote="abc",
            md5_local="abc",
            source_url="https://example.com/clinvar.vcf.gz",
            files=["humandb/hg38_clinvar_20250721.txt"],
        )
        entry = updated["databases"]["clinvar"]["hg38"]
        assert entry["version"] == "20250721"
        assert entry["md5_remote"] == "abc"
        assert "humandb/hg38_clinvar_20250721.txt" in entry["files"]

    def test_does_not_mutate_original(self):
        lock = create_empty_lock()
        original_id = id(lock["databases"]["clinvar"])
        record_clinvar_install(lock, "hg38", "20250721", "a", "a", "u", [])
        assert id(lock["databases"]["clinvar"]) == original_id

    def test_overwrites_previous_entry(self):
        lock = create_empty_lock()
        lock = record_clinvar_install(lock, "hg38", "20250101", "a", "a", "u", [])
        lock = record_clinvar_install(lock, "hg38", "20250721", "b", "b", "u2", [])
        assert lock["databases"]["clinvar"]["hg38"]["version"] == "20250721"


class TestRecordIntervarInstall:
    def test_adds_intervar_entry(self):
        lock = create_empty_lock()
        updated = record_intervar_install(
            lock, build="hg19", version="20180118", files=["humandb/hg19_intervar.txt"]
        )
        entry = updated["databases"]["intervar"]["hg19"]
        assert entry["version"] == "20180118"
        assert entry["source"] == "annovar_webfrom"

    def test_does_not_mutate_original(self):
        lock = create_empty_lock()
        original_ref = lock["databases"]
        record_intervar_install(lock, "hg19", "20180118", [])
        assert lock["databases"] is original_ref


class TestValidateLockSchema:
    def test_valid_lock_returns_no_errors(self):
        assert validate_lock_schema(create_empty_lock()) == []

    def test_wrong_schema_version_returns_error(self):
        lock = create_empty_lock()
        lock["schema_version"] = 99
        errors = validate_lock_schema(lock)
        assert any("schema_version" in e for e in errors)

    def test_missing_databases_key_returns_error(self):
        lock = {"schema_version": LOCK_SCHEMA_VERSION, "last_updated": "x"}
        errors = validate_lock_schema(lock)
        assert any("databases" in e for e in errors)

    def test_non_dict_input_returns_error(self):
        errors = validate_lock_schema("not a dict")
        assert errors


class TestGetInstalledVersion:
    def test_returns_version_when_present(self):
        lock = create_empty_lock()
        lock = record_clinvar_install(lock, "hg38", "20250721", "a", "a", "u", [])
        assert get_installed_version(lock, "clinvar", "hg38") == "20250721"

    def test_returns_none_when_absent(self):
        lock = create_empty_lock()
        assert get_installed_version(lock, "clinvar", "hg38") is None


class TestGetInstalledFiles:
    def test_returns_files_list(self):
        lock = create_empty_lock()
        files = ["humandb/hg38_clinvar_20250721.txt"]
        lock = record_clinvar_install(lock, "hg38", "20250721", "a", "a", "u", files)
        assert get_installed_files(lock, "clinvar", "hg38") == files

    def test_returns_empty_list_when_absent(self):
        lock = create_empty_lock()
        assert get_installed_files(lock, "clinvar", "hg38") == []
