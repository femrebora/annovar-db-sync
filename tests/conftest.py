"""Shared pytest fixtures for annovar-db-sync tests."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"
TEST_AVINPUT = FIXTURES_DIR / "clinvar_test.avinput"


@pytest.fixture()
def tmp_annovar_dir(tmp_path: Path) -> Path:
    """Create a minimal mock ANNOVAR directory with stub Perl scripts."""
    annovar_dir = tmp_path / "annovar"
    annovar_dir.mkdir()
    (annovar_dir / "humandb").mkdir()

    # Create stub Perl scripts that just exit 0
    for script in ("convert2annovar.pl", "annotate_variation.pl", "index_annovar.pl"):
        stub = annovar_dir / script
        stub.write_text("#!/usr/bin/perl\nexit 0;\n")
        stub.chmod(0o755)

    return annovar_dir


@pytest.fixture()
def sample_avinput(tmp_path: Path) -> Path:
    """Copy the test avinput fixture to a temp directory."""
    dest = tmp_path / "clinvar_test.avinput"
    shutil.copy(TEST_AVINPUT, dest)
    return dest


@pytest.fixture()
def empty_lock_file(tmp_path: Path) -> Path:
    """Return path to a non-existent lock file (will be auto-created by read)."""
    return tmp_path / "db_versions.json"
