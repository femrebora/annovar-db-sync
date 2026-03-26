# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import __version__
from .config import INTERVAR_DEFAULT_VERSION, LOCK_FILE_NAME, SUPPORTED_BUILDS, SyncConfig
from .clinvar import update_clinvar
from .intervar import install_intervar_db
from .lock_file import read_lock_file, validate_lock_schema
from .logger import get_logger, setup_logger


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_builds(build_arg: str) -> tuple[str, ...]:
    """Convert ``"hg19"``, ``"hg38"``, or ``"both"`` to a builds tuple."""
    if build_arg == "both":
        return SUPPORTED_BUILDS
    if build_arg in SUPPORTED_BUILDS:
        return (build_arg,)
    print(
        f"[ERROR] Unknown genome build {build_arg!r}. "
        f"Valid options: {', '.join(SUPPORTED_BUILDS)}, both",
        file=sys.stderr,
    )
    sys.exit(1)


def _build_config(args: argparse.Namespace) -> SyncConfig | None:
    """Build SyncConfig from parsed args, printing errors on failure."""
    lock_file = getattr(args, "lock_file", None)
    try:
        return SyncConfig.build(
            annovar_dir=args.annovar_dir,
            lock_file=lock_file,
        )
    except FileNotFoundError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return None


# ── Sub-commands ──────────────────────────────────────────────────────────────


def cmd_status(args: argparse.Namespace) -> int:
    config = _build_config(args)
    if config is None:
        return 1

    lock_data = read_lock_file(config.lock_file_path)
    errors = validate_lock_schema(lock_data)
    if errors:
        print(f"[WARNING] Lock file issues: {', '.join(errors)}")

    print(f"\nANNOVAR Database Status ({config.annovar_paths.annovar_dir})")
    print("=" * 70)

    dbs = lock_data.get("databases", {})
    if not any(dbs.get(k) for k in ("clinvar", "intervar")):
        print("No databases tracked yet.")
        print(f"\nRun 'annovar-db-sync update-clinvar' or 'annovar-db-sync install-intervar'")
        print(f"Lock file: {config.lock_file_path}")
        return 0

    fmt = "{:<14} {:<6}  {:<12}  {:<22}  {}"
    print(fmt.format("Database", "Build", "Version", "Installed", "Source"))
    print("-" * 70)

    for db_type in ("clinvar", "intervar"):
        for build in SUPPORTED_BUILDS:
            entry = dbs.get(db_type, {}).get(build)
            if entry:
                installed = entry.get("installed_date", "unknown")[:19].replace("T", " ")
                print(
                    fmt.format(
                        db_type,
                        build,
                        entry.get("version", "?"),
                        installed,
                        entry.get("source", "?"),
                    )
                )

    print(f"\nLock file: {config.lock_file_path}")
    return 0


def cmd_update_clinvar(args: argparse.Namespace) -> int:
    config = _build_config(args)
    if config is None:
        return 1

    builds = _parse_builds(args.genome_build)
    success = True
    for build in builds:
        ok = update_clinvar(
            config,
            build=build,
            dry_run=args.dry_run,
            force=args.force,
        )
        if not ok and not args.dry_run:
            # "already up-to-date" returns False but is not an error
            pass
    return 0 if success else 1


def cmd_install_intervar(args: argparse.Namespace) -> int:
    config = _build_config(args)
    if config is None:
        return 1

    builds = _parse_builds(args.genome_build)
    all_ok = True
    for build in builds:
        version = getattr(args, "version", None) or INTERVAR_DEFAULT_VERSION.get(build)
        ok = install_intervar_db(config, build=build, version=version)
        if not ok:
            all_ok = False
    return 0 if all_ok else 1


def cmd_check(args: argparse.Namespace) -> int:
    config = _build_config(args)
    if config is None:
        return 1

    lock_data = read_lock_file(config.lock_file_path)
    errors = validate_lock_schema(lock_data)
    if errors:
        for err in errors:
            print(f"[SCHEMA ERROR] {err}")
        return 2

    print("Checking database integrity ...")
    issues = 0

    for db_type in ("clinvar", "intervar"):
        for build in SUPPORTED_BUILDS:
            entry = lock_data.get("databases", {}).get(db_type, {}).get(build)
            if not entry:
                continue
            for rel_path in entry.get("files", []):
                full = config.annovar_paths.annovar_dir / rel_path
                if full.exists() and full.stat().st_size > 0:
                    size_mb = full.stat().st_size / (1024 * 1024)
                    print(f"  [OK]   {rel_path} ({size_mb:.1f} MB)")
                elif full.exists():
                    print(f"  [WARN] {rel_path} -- FILE IS EMPTY")
                    issues += 1
                else:
                    print(f"  [FAIL] {rel_path} -- FILE MISSING")
                    issues += 1

    if issues:
        print(f"\nResult: {issues} issue(s) found.")
        return 2
    print("\nResult: All tracked files are present.")
    return 0


# ── Parser ────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="annovar-db-sync",
        description=(
            "Manage ANNOVAR database versions for ClinVar and InterVar.\n"
            "Maintains a db_versions.json lock file for reproducible annotation."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version", action="version", version=f"annovar-db-sync {__version__}"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging."
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ── status ────────────────────────────────────────────────────────────────
    p_status = sub.add_parser("status", help="Show installed database versions.")
    p_status.add_argument(
        "--annovar-dir",
        required=True,
        metavar="PATH",
        help="Path to the ANNOVAR installation directory.",
    )
    p_status.add_argument(
        "--lock-file",
        metavar="PATH",
        default=None,
        help=f"Path to the lock file (default: <annovar-dir>/{LOCK_FILE_NAME}).",
    )
    p_status.set_defaults(func=cmd_status)

    # ── update-clinvar ─────────────────────────────────────────────────────────
    p_cv = sub.add_parser(
        "update-clinvar",
        help="Download and install the latest ClinVar database from NCBI.",
    )
    p_cv.add_argument(
        "--annovar-dir",
        required=True,
        metavar="PATH",
        help="Path to the ANNOVAR installation directory.",
    )
    p_cv.add_argument(
        "--genome-build",
        default="both",
        choices=list(SUPPORTED_BUILDS) + ["both"],
        metavar="BUILD",
        help="Genome build: hg19, hg38, or both (default: both).",
    )
    p_cv.add_argument(
        "--lock-file",
        metavar="PATH",
        default=None,
        help=f"Path to the lock file (default: <annovar-dir>/{LOCK_FILE_NAME}).",
    )
    p_cv.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be updated without downloading.",
    )
    p_cv.add_argument(
        "--force",
        action="store_true",
        help="Re-download even if the local version is current.",
    )
    p_cv.set_defaults(func=cmd_update_clinvar)

    # ── install-intervar ───────────────────────────────────────────────────────
    p_iv = sub.add_parser(
        "install-intervar",
        help="Install an InterVar ANNOVAR database via annotate_variation.pl.",
    )
    p_iv.add_argument(
        "--annovar-dir",
        required=True,
        metavar="PATH",
        help="Path to the ANNOVAR installation directory.",
    )
    p_iv.add_argument(
        "--genome-build",
        default="both",
        choices=list(SUPPORTED_BUILDS) + ["both"],
        metavar="BUILD",
        help="Genome build: hg19, hg38, or both (default: both).",
    )
    p_iv.add_argument(
        "--version",
        metavar="YYYYMMDD",
        default=None,
        help=(
            "InterVar database version date, e.g. 20250721. "
            f"Defaults: hg19={INTERVAR_DEFAULT_VERSION['hg19']}, "
            f"hg38={INTERVAR_DEFAULT_VERSION['hg38']}."
        ),
    )
    p_iv.add_argument(
        "--lock-file",
        metavar="PATH",
        default=None,
        help=f"Path to the lock file (default: <annovar-dir>/{LOCK_FILE_NAME}).",
    )
    p_iv.set_defaults(func=cmd_install_intervar)

    # ── check ──────────────────────────────────────────────────────────────────
    p_check = sub.add_parser(
        "check",
        help="Verify that all tracked database files exist on disk.",
    )
    p_check.add_argument(
        "--annovar-dir",
        required=True,
        metavar="PATH",
        help="Path to the ANNOVAR installation directory.",
    )
    p_check.add_argument(
        "--lock-file",
        metavar="PATH",
        default=None,
        help=f"Path to the lock file (default: <annovar-dir>/{LOCK_FILE_NAME}).",
    )
    p_check.set_defaults(func=cmd_check)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    setup_logger(verbose=args.verbose)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
