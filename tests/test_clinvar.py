"""Tests for annovar_db_sync.clinvar."""
from __future__ import annotations

import gzip
import hashlib
import shutil
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import responses

from annovar_db_sync.clinvar import check_clinvar_update, update_clinvar
from annovar_db_sync.config import SyncConfig
from annovar_db_sync.lock_file import (
    create_empty_lock,
    get_installed_version,
    read_lock_file,
    record_clinvar_install,
    write_lock_file,
)
from annovar_db_sync.logger import setup_logger

setup_logger()

# ── Shared test data ──────────────────────────────────────────────────────────

_FTP_HTML = """
<html><body>
<a href="clinvar_20250701.vcf.gz">clinvar_20250701.vcf.gz</a>
<a href="clinvar_20250721.vcf.gz">clinvar_20250721.vcf.gz</a>
</body></html>
"""
_LATEST_VERSION = "20250721"
_FAKE_VCF_CONTENT = b"fake vcf content"
_FAKE_MD5 = hashlib.md5(_FAKE_VCF_CONTENT).hexdigest()

_FTP_HG38 = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/"
_FTP_HG19 = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/"


# ── check_clinvar_update ──────────────────────────────────────────────────────


class TestCheckClinvarUpdate:
    @responses.activate
    def test_returns_latest_version_hg38(self):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        result = check_clinvar_update("hg38")
        assert result is not None
        version, url, md5 = result
        assert version == _LATEST_VERSION
        assert _LATEST_VERSION in url
        assert md5 == _FAKE_MD5

    @responses.activate
    def test_hg19_uses_grch37_url(self):
        responses.add(responses.GET, _FTP_HG19, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG19 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        result = check_clinvar_update("hg19")
        assert result is not None
        version, url, md5 = result
        assert "GRCh37" in url

    @responses.activate
    def test_returns_none_on_ftp_error(self):
        responses.add(responses.GET, _FTP_HG38, status=503)
        assert check_clinvar_update("hg38") is None

    @responses.activate
    def test_returns_none_on_no_matching_files(self):
        responses.add(responses.GET, _FTP_HG38, body="<html>no files</html>", status=200)
        assert check_clinvar_update("hg38") is None

    @responses.activate
    def test_returns_none_on_md5_fetch_error(self):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            status=404,
        )
        assert check_clinvar_update("hg38") is None

    @responses.activate
    def test_returns_none_on_malformed_md5(self):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body="not-a-valid-hash",
            status=200,
        )
        assert check_clinvar_update("hg38") is None


# ── update_clinvar dry-run ────────────────────────────────────────────────────


class TestUpdateClinvarDryRun:
    @responses.activate
    def test_dry_run_returns_true_when_update_available(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        config = SyncConfig.build(
            annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file
        )
        assert update_clinvar(config, "hg38", dry_run=True) is True

    @responses.activate
    def test_dry_run_does_not_write_lock_file(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        config = SyncConfig.build(
            annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file
        )
        update_clinvar(config, "hg38", dry_run=True)
        assert get_installed_version(read_lock_file(empty_lock_file), "clinvar", "hg38") is None


# ── update_clinvar already up-to-date ────────────────────────────────────────


class TestUpdateClinvarAlreadyUpToDate:
    @responses.activate
    def test_returns_false_when_md5_matches(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        lock = record_clinvar_install(
            create_empty_lock(), "hg38", _LATEST_VERSION, _FAKE_MD5, _FAKE_MD5, "u", []
        )
        write_lock_file(empty_lock_file, lock)

        config = SyncConfig.build(annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file)
        assert update_clinvar(config, "hg38", force=False) is False


# ── update_clinvar check failure path ────────────────────────────────────────


class TestUpdateClinvarErrors:
    @responses.activate
    def test_returns_false_when_ftp_unreachable(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        responses.add(responses.GET, _FTP_HG38, status=503)
        config = SyncConfig.build(annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file)
        assert update_clinvar(config, "hg38") is False

    @responses.activate
    def test_returns_false_on_download_failure(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        """FTP listing succeeds, but the actual VCF download fails."""
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz",
            status=503,
        )
        config = SyncConfig.build(annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file)
        assert update_clinvar(config, "hg38") is False


# ── update_clinvar full workflow (mocked subprocess) ─────────────────────────


class TestUpdateClinvarFullWorkflow:
    @responses.activate
    def test_successful_update_writes_lock_file(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        """Full happy-path test with mocked subprocess and file creation."""
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        # The VCF GZ download
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz",
            body=_FAKE_VCF_CONTENT,
            status=200,
        )
        # The MD5 sidecar download
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        # The TBI download (best-effort, may 404)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.tbi",
            status=404,
        )

        config = SyncConfig.build(annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file)

        # Mock convert2annovar.pl: exits 0 and creates the .avinput file
        def fake_convert(*args, **kwargs):
            # Find the -outfile argument and create its .avinput
            cmd = args[0]
            outfile_idx = cmd.index("-outfile") + 1
            avinput = Path(cmd[outfile_idx] + ".avinput")
            avinput.write_text("1\t100\t100\tA\tG\t.\t.\t.\t.\t.\t.\t.\tALLELEID=1;CLNDN=test;CLNDISDB=.;CLNREVSTAT=criteria;CLNSIG=Pathogenic\n")
            result = MagicMock()
            result.returncode = 0
            result.stderr = ""
            return result

        with patch("annovar_db_sync.clinvar.subprocess.run", side_effect=fake_convert):
            ok = update_clinvar(config, "hg38")

        assert ok is True
        lock = read_lock_file(empty_lock_file)
        assert get_installed_version(lock, "clinvar", "hg38") == _LATEST_VERSION

    @responses.activate
    def test_convert2annovar_failure_returns_false(
        self, tmp_annovar_dir: Path, empty_lock_file: Path
    ):
        responses.add(responses.GET, _FTP_HG38, body=_FTP_HTML, status=200)
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz",
            body=_FAKE_VCF_CONTENT,
            status=200,
        )
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.md5",
            body=f"{_FAKE_MD5}  clinvar_{_LATEST_VERSION}.vcf.gz\n",
            status=200,
        )
        responses.add(
            responses.GET,
            _FTP_HG38 + f"clinvar_{_LATEST_VERSION}.vcf.gz.tbi",
            status=404,
        )

        config = SyncConfig.build(annovar_dir=tmp_annovar_dir, lock_file=empty_lock_file)

        fail_result = MagicMock()
        fail_result.returncode = 1
        fail_result.stderr = "convert2annovar.pl error"

        with patch("annovar_db_sync.clinvar.subprocess.run", return_value=fail_result):
            ok = update_clinvar(config, "hg38")

        assert ok is False
