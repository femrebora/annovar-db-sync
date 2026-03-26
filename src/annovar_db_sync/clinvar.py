# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ── Attribution ──────────────────────────────────────────────────────────────
# The NCBI FTP discovery approach and MD5-based update-check strategy are
# inspired by update_resources.py from update_annovar_db:
#   https://github.com/mobidic/update_annovar_db
#   Original work Copyright (C) mobidic / CHU Montpellier
#   Licensed under the GNU General Public License v3.0
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from .config import (
    BUILD_TO_GRCH,
    CLINVAR_VCF_DIR_TEMPLATE,
    SyncConfig,
)
from .converter import convert_clinvar_avinput_to_annovar_db
from .downloader import DownloadError, download_file, fetch_text, verify_md5
from .lock_file import (
    get_installed_version,
    read_lock_file,
    record_clinvar_install,
    write_lock_file,
)
from .logger import get_logger

# Matches filenames like clinvar_20250721.vcf.gz in FTP directory HTML
_CLINVAR_FILENAME_RE = re.compile(r'"(clinvar_(\d{8})\.vcf\.gz)"')


def check_clinvar_update(build: str) -> tuple[str, str, str] | None:
    """Query NCBI FTP to find the latest available ClinVar VCF.

    Parameters
    ----------
    build:
        Genome build (``"hg19"`` or ``"hg38"``).

    Returns
    -------
    tuple[str, str, str] | None
        ``(version_date, vcf_gz_url, remote_md5)`` where *version_date* is
        the date string like ``"20250721"``.  Returns ``None`` if the FTP
        directory cannot be parsed.
    """
    logger = get_logger()
    grch = BUILD_TO_GRCH[build]
    ftp_dir_url = CLINVAR_VCF_DIR_TEMPLATE.format(grch=grch)

    logger.info("Checking NCBI FTP for latest ClinVar (%s) at %s", build, ftp_dir_url)
    try:
        html = fetch_text(ftp_dir_url)
    except DownloadError as exc:
        logger.warning("Could not reach NCBI FTP: %s", exc)
        return None

    # Collect all matching filenames and pick the most recent date
    matches = _CLINVAR_FILENAME_RE.findall(html)
    if not matches:
        logger.warning(
            "Could not parse ClinVar filenames from FTP listing. "
            "The NCBI FTP HTML format may have changed. Raw response (first 2000 chars):\n%s",
            html[:2000],
        )
        return None

    # matches is a list of (filename, date_str) tuples; pick newest date
    latest_filename, latest_date = max(matches, key=lambda m: m[1])
    vcf_gz_url = ftp_dir_url + latest_filename
    md5_url = vcf_dir_url = ftp_dir_url + latest_filename + ".md5"

    logger.info("Latest ClinVar version for %s: %s", build, latest_date)

    try:
        md5_text = fetch_text(md5_url)
    except DownloadError as exc:
        logger.warning("Could not fetch ClinVar MD5 from %s: %s", md5_url, exc)
        return None

    md5_match = re.search(r"^([0-9a-fA-F]{32})", md5_text)
    if not md5_match:
        logger.warning("Could not parse MD5 from: %r", md5_text[:200])
        return None

    return latest_date, vcf_gz_url, md5_match.group(1)


def update_clinvar(
    config: SyncConfig,
    build: str,
    dry_run: bool = False,
    force: bool = False,
) -> bool:
    """Download, convert, index, and install the latest ClinVar for *build*.

    Parameters
    ----------
    config:
        Runtime configuration (ANNOVAR paths, lock file location, etc.).
    build:
        ``"hg19"`` or ``"hg38"``.
    dry_run:
        If ``True``, report what would happen without downloading anything.
    force:
        If ``True``, re-download even when the local MD5 already matches.

    Returns
    -------
    bool
        ``True`` on successful update/install, ``False`` on any error or when
        already up-to-date (in non-force mode).
    """
    logger = get_logger()
    grch = BUILD_TO_GRCH[build]

    result = check_clinvar_update(build)
    if result is None:
        logger.error("Unable to determine latest ClinVar version for %s.", build)
        return False

    version, vcf_gz_url, remote_md5 = result
    lock_data = read_lock_file(config.lock_file_path)
    current_version = get_installed_version(lock_data, "clinvar", build)
    current_md5 = (
        lock_data.get("databases", {})
        .get("clinvar", {})
        .get(build, {})
        .get("md5_remote", "")
    )

    if not force and current_md5.lower() == remote_md5.lower():
        logger.info(
            "ClinVar %s (%s) is already up-to-date (version %s). "
            "Use --force to re-download.",
            build,
            grch,
            version,
        )
        return False

    if dry_run:
        logger.info(
            "[DRY RUN] Would update ClinVar %s: %s -> %s",
            build,
            current_version or "none",
            version,
        )
        return True

    logger.info(
        "Updating ClinVar %s: %s -> %s",
        build,
        current_version or "none",
        version,
    )

    # Work inside a temporary directory; only move files on success
    with tempfile.TemporaryDirectory(
        prefix="annovar_db_sync_clinvar_", dir=config.annovar_paths.annovar_dir
    ) as tmpdir:
        tmp = Path(tmpdir)
        vcf_gz = tmp / f"clinvar_{version}.vcf.gz"
        vcf_gz_md5 = tmp / f"clinvar_{version}.vcf.gz.md5"
        vcf_gz_tbi = tmp / f"clinvar_{version}.vcf.gz.tbi"

        # Download VCF, MD5 sidecar, and tabix index
        try:
            download_file(vcf_gz_url, vcf_gz)
            download_file(vcf_gz_url + ".md5", vcf_gz_md5)
            # TBI may not exist for all releases – best-effort
            try:
                download_file(vcf_gz_url + ".tbi", vcf_gz_tbi)
            except DownloadError:
                logger.debug("No .tbi file available for this release; skipping.")
        except DownloadError as exc:
            logger.error("Download failed: %s", exc)
            return False

        # Verify integrity
        if not verify_md5(vcf_gz, remote_md5):
            local_md5 = __import__(
                "hashlib"
            ).md5(vcf_gz.read_bytes()).hexdigest()
            logger.error(
                "MD5 mismatch for %s: remote=%s local=%s",
                vcf_gz.name,
                remote_md5,
                local_md5,
            )
            return False
        logger.info("MD5 verified OK for clinvar_%s.vcf.gz", version)

        # Step 1: convert2annovar.pl  VCF → avinput
        avinput = tmp / f"clinvar_{version}.avinput"
        logger.info("Running convert2annovar.pl ...")
        result_cv = subprocess.run(
            [
                "perl",
                str(config.annovar_paths.convert2annovar),
                "-format",
                "vcf4",
                "-includeinfo",
                str(vcf_gz),
                "-outfile",
                str(avinput.with_suffix("")),  # convert2annovar appends extension
            ],
            capture_output=True,
            text=True,
        )
        # convert2annovar.pl writes to <outfile>.avinput
        produced_avinput = avinput.with_suffix("").with_suffix(".avinput")
        if result_cv.returncode != 0 or not produced_avinput.exists():
            logger.error(
                "convert2annovar.pl failed (rc=%d):\n%s",
                result_cv.returncode,
                result_cv.stderr[:2000],
            )
            return False
        logger.info("convert2annovar.pl succeeded.")

        # Step 2: custom converter  avinput → ANNOVAR db .txt
        annovar_db_txt = tmp / f"clinvar_{version}.txt"
        try:
            convert_clinvar_avinput_to_annovar_db(produced_avinput, annovar_db_txt)
        except Exception as exc:
            logger.error("Conversion to ANNOVAR db format failed: %s", exc)
            return False

        # Step 3: index_annovar.pl (optional)
        dest_txt = config.annovar_paths.humandb_dir / f"{build}_clinvar_{version}.txt"
        dest_idx = dest_txt.with_suffix(".txt.idx")

        if config.annovar_paths.index_annovar:
            logger.info("Running index_annovar.pl ...")
            result_idx = subprocess.run(
                [
                    "perl",
                    str(config.annovar_paths.index_annovar),
                    str(annovar_db_txt),
                    "-outfile",
                    str(dest_txt),
                ],
                capture_output=True,
                text=True,
            )
            if result_idx.returncode != 0:
                logger.warning(
                    "index_annovar.pl exited non-zero (rc=%d); "
                    "ANNOVAR will still work but lookups may be slower.\n%s",
                    result_idx.returncode,
                    result_idx.stderr[:1000],
                )
            else:
                logger.info("index_annovar.pl succeeded.")
        else:
            # No index_annovar.pl – copy the .txt directly
            logger.warning(
                "index_annovar.pl not found in %s; skipping indexing. "
                "ANNOVAR will still work without an index file.",
                config.annovar_paths.annovar_dir,
            )
            shutil.copy2(annovar_db_txt, dest_txt)

        # Verify destination file was created
        if not dest_txt.exists():
            # Fallback: plain copy
            shutil.copy2(annovar_db_txt, dest_txt)

    # Record installed files (relative to annovar_dir for portability)
    installed_files = [str(dest_txt.relative_to(config.annovar_paths.annovar_dir))]
    if dest_idx.exists():
        installed_files.append(
            str(dest_idx.relative_to(config.annovar_paths.annovar_dir))
        )

    # Update lock file
    updated_lock = record_clinvar_install(
        lock_data,
        build=build,
        version=version,
        md5_remote=remote_md5,
        md5_local=remote_md5,  # verified above
        source_url=vcf_gz_url,
        files=installed_files,
    )
    write_lock_file(config.lock_file_path, updated_lock)

    logger.info(
        "ClinVar %s successfully updated to version %s -> %s",
        build,
        version,
        dest_txt,
    )
    return True
