# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

# ── NCBI FTP ──────────────────────────────────────────────────────────────────
CLINVAR_FTP_BASE = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/"
# e.g. https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/
CLINVAR_VCF_DIR_TEMPLATE = CLINVAR_FTP_BASE + "vcf_{grch}/"

# ── Genome build mappings ──────────────────────────────────────────────────────
# UCSC ↔ GRCh nomenclature
BUILD_TO_GRCH: dict[str, str] = {
    "hg19": "GRCh37",
    "hg38": "GRCh38",
}
GRCH_TO_BUILD: dict[str, str] = {v: k for k, v in BUILD_TO_GRCH.items()}
SUPPORTED_BUILDS: tuple[str, ...] = ("hg19", "hg38")

# ── Default InterVar ANNOVAR db versions ──────────────────────────────────────
INTERVAR_DEFAULT_VERSION: dict[str, str] = {
    "hg19": "20180118",
    "hg38": "20250721",
}

# ── ClinVar INFO fields to extract into ANNOVAR format ───────────────────────
CLINVAR_INFO_FIELDS: tuple[str, ...] = (
    "ALLELEID",
    "CLNDN",
    "CLNDISDB",
    "CLNREVSTAT",
    "CLNSIG",
)

# ── Lock file ─────────────────────────────────────────────────────────────────
LOCK_FILE_NAME = "db_versions.json"
LOCK_SCHEMA_VERSION = 1

# ── Working directory for intermediate VCF/avinput files ─────────────────────
WORKING_DIR_NAME = ".annovar_db_sync_work"


@dataclass(frozen=True)
class AnnovarPaths:
    """Validated paths to an ANNOVAR installation directory."""

    annovar_dir: Path
    humandb_dir: Path
    convert2annovar: Path
    annotate_variation: Path
    index_annovar: Path | None  # optional – not bundled in all installations

    @classmethod
    def from_dir(cls, annovar_dir: str | Path) -> "AnnovarPaths":
        """
        Build and validate paths from an ANNOVAR installation directory.

        Raises
        ------
        FileNotFoundError
            If the ANNOVAR directory or required Perl scripts are missing.
        """
        d = Path(annovar_dir).expanduser().resolve()
        if not d.is_dir():
            raise FileNotFoundError(
                f"ANNOVAR directory not found: {d}\n"
                "Provide the path to the folder containing annotate_variation.pl."
            )

        required = {
            "convert2annovar": d / "convert2annovar.pl",
            "annotate_variation": d / "annotate_variation.pl",
        }
        for name, path in required.items():
            if not path.exists():
                raise FileNotFoundError(
                    f"Required ANNOVAR script not found: {path}\n"
                    f"Expected to find {name}.pl inside {d}"
                )

        humandb = d / "humandb"
        humandb.mkdir(exist_ok=True)

        index_pl = d / "index_annovar.pl"
        index_annovar = index_pl if index_pl.exists() else None

        return cls(
            annovar_dir=d,
            humandb_dir=humandb,
            convert2annovar=required["convert2annovar"],
            annotate_variation=required["annotate_variation"],
            index_annovar=index_annovar,
        )


@dataclass(frozen=True)
class SyncConfig:
    """Runtime configuration assembled from CLI arguments."""

    annovar_paths: AnnovarPaths
    lock_file_path: Path
    genome_builds: tuple[str, ...] = field(default_factory=lambda: SUPPORTED_BUILDS)

    @classmethod
    def build(
        cls,
        annovar_dir: str | Path,
        lock_file: str | Path | None = None,
        genome_builds: tuple[str, ...] | None = None,
    ) -> "SyncConfig":
        paths = AnnovarPaths.from_dir(annovar_dir)
        lf = (
            Path(lock_file).expanduser().resolve()
            if lock_file
            else paths.annovar_dir / LOCK_FILE_NAME
        )
        builds = genome_builds if genome_builds else SUPPORTED_BUILDS
        return cls(annovar_paths=paths, lock_file_path=lf, genome_builds=builds)
