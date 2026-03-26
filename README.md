# annovar-db-sync

**Automated ANNOVAR database synchronization for ClinVar and InterVar — with version locking for reproducible variant annotation.**

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## Overview

When running variant annotation pipelines with [ANNOVAR](https://annovar.openbioinformatics.org/) and [InterVar](https://github.com/WGLab/InterVar), knowing *exactly* which database versions were used is critical for clinical reproducibility and audit trails. The built-in ANNOVAR download mechanism gives you the latest databases but doesn't track what you installed or when.

**annovar-db-sync** solves this by:

1. **Auto-downloading ClinVar** directly from NCBI FTP with MD5 integrity verification, converting to ANNOVAR format.
2. **Installing InterVar ANNOVAR databases** for any version via the standard ANNOVAR mechanism.
3. **Writing a `db_versions.json` lock file** that records exactly which versions are installed, their checksums, and install dates.

Commit `db_versions.json` into your analysis repository to ensure that any collaborator — or future you — can reproduce the exact annotation environment.

---

## Features

- Auto-detects the latest ClinVar release from NCBI FTP
- MD5 verification of all downloaded files
- Converts ClinVar VCF to ANNOVAR db format (CLNALLELEID, CLNDN, CLNDISDB, CLNREVSTAT, CLNSIG)
- Supports both **hg19 (GRCh37)** and **hg38 (GRCh38)** genome builds
- Installs any InterVar ANNOVAR database version via `annotate_variation.pl`
- Version lock file (`db_versions.json`) for full reproducibility
- `--dry-run` mode to preview updates without downloading
- `check` command to verify all tracked files are present on disk

---

## Prerequisites

- **ANNOVAR** installed (contains `annotate_variation.pl`, `convert2annovar.pl`)
  - Register and download from: https://annovar.openbioinformatics.org
- **Python 3.10+**
- **Perl 5** (used by ANNOVAR scripts)

---

## Installation

### Using conda (recommended)

```bash
git clone https://github.com/YOUR_USERNAME/annovar-db-sync.git
cd annovar-db-sync
conda env create -f environment.yml
conda activate annovar-db-sync
pip install -e .
```

### Using pip

```bash
git clone https://github.com/YOUR_USERNAME/annovar-db-sync.git
cd annovar-db-sync
pip install .
```

For development (includes test dependencies):

```bash
pip install -e ".[dev]"
```

---

## Quick Start

```bash
# 1. Check what's installed
annovar-db-sync status --annovar-dir /opt/annovar

# 2. Update ClinVar for both genome builds
annovar-db-sync update-clinvar --annovar-dir /opt/annovar

# 3. Install InterVar databases
annovar-db-sync install-intervar --annovar-dir /opt/annovar

# 4. Verify everything is in place
annovar-db-sync check --annovar-dir /opt/annovar
```

After running, a `db_versions.json` will be created in your ANNOVAR directory.

---

## Commands Reference

### `status`

Show all installed database versions tracked in the lock file.

```
annovar-db-sync status
  --annovar-dir PATH     Path to ANNOVAR installation (required)
  --lock-file PATH       Path to db_versions.json (default: <annovar-dir>/db_versions.json)
```

**Example output:**

```
ANNOVAR Database Status (/opt/annovar)
======================================================================
Database       Build   Version       Installed              Source
-----------    -----   ----------    -------------------    ---------
clinvar        hg19    20250721      2026-03-26 14:30       ncbi_ftp
clinvar        hg38    20250721      2026-03-26 14:30       ncbi_ftp
intervar       hg19    20180118      2026-03-26 14:35       annovar_webfrom
intervar       hg38    20250721      2026-03-26 14:35       annovar_webfrom
```

---

### `update-clinvar`

Download the latest ClinVar VCF from NCBI FTP, verify its integrity, convert it to ANNOVAR format, and install it into `humandb/`.

```
annovar-db-sync update-clinvar
  --annovar-dir PATH      Path to ANNOVAR installation (required)
  --genome-build BUILD    hg19 | hg38 | both  (default: both)
  --lock-file PATH        Path to db_versions.json
  --dry-run               Preview update without downloading
  --force                 Re-download even if already up-to-date
```

The command is **idempotent**: it skips the download if the remote MD5 matches the
locally recorded MD5. Use `--force` to override.

---

### `install-intervar`

Install an InterVar ANNOVAR database using `annotate_variation.pl -downdb -webfrom annovar`.

```
annovar-db-sync install-intervar
  --annovar-dir PATH      Path to ANNOVAR installation (required)
  --genome-build BUILD    hg19 | hg38 | both  (default: both)
  --version YYYYMMDD      Database version, e.g. 20250721
                          (default: 20180118 for hg19, 20250721 for hg38)
  --lock-file PATH        Path to db_versions.json
```

**Pinning a specific version:**

```bash
# Pin hg38 to a specific InterVar release
annovar-db-sync install-intervar \
  --annovar-dir /opt/annovar \
  --genome-build hg38 \
  --version 20250721
```

---

### `check`

Verify that all files referenced in the lock file actually exist on disk and are non-empty.

```
annovar-db-sync check
  --annovar-dir PATH     Path to ANNOVAR installation (required)
  --lock-file PATH       Path to db_versions.json
```

**Exit codes:**
- `0` — All files present
- `2` — One or more files missing or empty

---

## The `db_versions.json` Lock File

The lock file is the heart of reproducibility. After installation, it looks like this:

```json
{
  "schema_version": 1,
  "last_updated": "2026-03-26T14:30:00Z",
  "databases": {
    "clinvar": {
      "hg19": {
        "version": "20250721",
        "source": "ncbi_ftp",
        "source_url": "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh37/clinvar_20250721.vcf.gz",
        "md5_remote": "a1b2c3...",
        "md5_local": "a1b2c3...",
        "installed_date": "2026-03-26T14:30:00Z",
        "files": [
          "humandb/hg19_clinvar_20250721.txt",
          "humandb/hg19_clinvar_20250721.txt.idx"
        ]
      },
      "hg38": { "..." : "..." }
    },
    "intervar": {
      "hg19": {
        "version": "20180118",
        "source": "annovar_webfrom",
        "installed_date": "2026-03-26T14:35:00Z",
        "files": [
          "humandb/hg19_intervar_20180118.txt"
        ]
      }
    }
  }
}
```

**Best practice:** Commit `db_versions.json` to your analysis repository (not the large `.txt` database files themselves). This lets collaborators see exactly what databases were used for every analysis run.

---

## Integrating with InterVar

After installing databases, update your `config.ini` to reference them by the exact versioned filename. For example, if you installed `clinvar_20250721` and `intervar_20250721` for hg38:

```ini
[Annovar]
database_locat = humandb
database_names = refGene exac03 gene4denovo201907 esp6500siv2_all 1000g2015aug_all \
                 avsnp150 dbnsfp42a intervar_20250721 gnomad211_genome dbscsnv11 \
                 rmsk ensGene knownGene

[InterVar]
buildver = hg38
```

The versioned filename in `database_names` ensures your pipeline always uses the exact database you installed, not whatever happens to be latest.

---

## How It Works

### ClinVar update pipeline

```
NCBI FTP directory listing
         ↓
   Find latest clinvar_YYYYMMDD.vcf.gz
         ↓
   Compare remote MD5 with lock file MD5
         ↓ (if different)
   Download clinvar_YYYYMMDD.vcf.gz + .md5 + .tbi
         ↓
   Verify MD5 integrity
         ↓
   convert2annovar.pl  -format vcf4 -includeinfo
         ↓
   Custom converter: VCF INFO → ANNOVAR db columns
   (CLNALLELEID, CLNDN, CLNDISDB, CLNREVSTAT, CLNSIG)
         ↓
   index_annovar.pl  (if available)
         ↓
   Install to humandb/ + update db_versions.json
```

### InterVar database installation

```
annotate_variation.pl -buildver hg38 -downdb -webfrom annovar intervar_20250721 humandb/
         ↓
   Scan humandb/ for new intervar_* files
         ↓
   Update db_versions.json
```

---

## Attribution

This tool is built on top of
[**update_annovar_db**](https://github.com/mobidic/update_annovar_db)
by the MoBiDiC team (CHU Montpellier), licensed under GPL v3.

Specifically:
- The ClinVar VCF-to-ANNOVAR-format conversion logic (`converter.py`) is derived from `avinput2annovardb.py`.
- The NCBI FTP discovery and MD5-based update-check approach (`clinvar.py`) is inspired by `update_resources.py`.

See [NOTICE](NOTICE) for full attribution details.

---

## License

This program is free software: you can redistribute it and/or modify it under the
terms of the **GNU General Public License version 3** (or any later version) as
published by the Free Software Foundation.

See [LICENSE](LICENSE) for the full text.

---

## Contributing

Issues and pull requests welcome. Please ensure all tests pass:

```bash
pytest --cov=annovar_db_sync --cov-report=term-missing
```
