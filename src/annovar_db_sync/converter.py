# annovar-db-sync - Automated ANNOVAR database synchronization
# Copyright (C) 2026 Emre Bora
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# ── Attribution ──────────────────────────────────────────────────────────────
# This file is derived from avinput2annovardb.py, part of update_annovar_db:
#   https://github.com/mobidic/update_annovar_db
#   Original work Copyright (C) mobidic / CHU Montpellier
#   Licensed under the GNU General Public License v3.0
#
# Modifications from the original:
#   - Streaming line-by-line I/O instead of readlines() to reduce memory use
#   - Immutable helper functions (_parse_info_fields, _escape_commas)
#   - Explicit output path parameter instead of implicit side-effect naming
#   - Type hints and error handling for malformed lines
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

from pathlib import Path
from typing import Iterator

from .config import CLINVAR_INFO_FIELDS
from .logger import get_logger

# ANNOVAR db header produced by this converter
_HEADER = "#Chr\tStart\tEnd\tRef\tAlt\tCLNALLELEID\tCLNDN\tCLNDISDB\tCLNREVSTAT\tCLNSIG\n"

# Minimum number of tab-separated columns expected in a valid avinput line
_MIN_COLS = 13


def convert_clinvar_avinput_to_annovar_db(
    avinput_path: Path,
    output_path: Path,
    fields: tuple[str, ...] = CLINVAR_INFO_FIELDS,
) -> Path:
    """Convert a ClinVar .avinput file to ANNOVAR database format.

    The function reads *avinput_path* line-by-line (streaming) and writes
    ANNOVAR database rows to *output_path*.  Commas inside field values are
    replaced with the ANNOVAR escape sequence ``\\x2c``.

    Parameters
    ----------
    avinput_path:
        Path to the ``.avinput`` file produced by ``convert2annovar.pl``.
    output_path:
        Destination ``.txt`` file path for the ANNOVAR database.
    fields:
        Ordered tuple of VCF INFO keys to extract; must include ``"ALLELEID"``.

    Returns
    -------
    Path
        *output_path* on success.

    Raises
    ------
    ValueError
        If ``"ALLELEID"`` is not in *fields*.
    """
    if "ALLELEID" not in fields:
        raise ValueError("'ALLELEID' must be in fields (maps to CLNALLELEID column)")

    logger = get_logger()
    skipped = 0
    written = 0

    with open(avinput_path, encoding="utf-8") as src, open(
        output_path, "w", encoding="utf-8"
    ) as dst:
        dst.write(_HEADER)
        for line_no, line in enumerate(_iter_data_lines(src), start=1):
            cols = line.rstrip("\n").split("\t")
            if len(cols) < _MIN_COLS:
                logger.debug("Skipping short line %d (%d cols)", line_no, len(cols))
                skipped += 1
                continue

            chr_, start, end, ref, alt = cols[0], cols[1], cols[2], cols[3], cols[4]
            info_string = cols[12]

            info = _parse_info_fields(info_string, fields)

            row = "\t".join(
                [
                    chr_,
                    start,
                    end,
                    ref,
                    alt,
                    info["CLNALLELEID"],
                    _escape_commas(info.get("CLNDN", ".")),
                    _escape_commas(info.get("CLNDISDB", ".")),
                    _escape_commas(info.get("CLNREVSTAT", ".")),
                    info.get("CLNSIG", "."),
                ]
            )
            dst.write(row + "\n")
            written += 1

    logger.info(
        "Conversion complete: %d rows written, %d lines skipped -> %s",
        written,
        skipped,
        output_path,
    )
    return output_path


# ── Private helpers ───────────────────────────────────────────────────────────


def _iter_data_lines(file_obj) -> Iterator[str]:
    """Yield non-empty, non-comment lines from an open file object."""
    for line in file_obj:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            yield line


def _parse_info_fields(info_string: str, fields: tuple[str, ...]) -> dict[str, str]:
    """Parse a VCF INFO column string into a dict with only the requested keys.

    ``ALLELEID`` is mapped to ``CLNALLELEID`` to match the ANNOVAR db column name.
    All other keys in *fields* default to ``"."`` if absent from *info_string*.
    """
    result: dict[str, str] = {
        ("CLNALLELEID" if f == "ALLELEID" else f): "."
        for f in fields
        if f != "ALLELEID"
    }
    result.setdefault("CLNALLELEID", ".")

    for token in info_string.split(";"):
        if "=" not in token:
            continue
        key, _, value = token.partition("=")
        key = key.strip()
        if key == "ALLELEID":
            result["CLNALLELEID"] = value
        elif key in fields:
            result[key] = value

    return result


def _escape_commas(value: str) -> str:
    """Replace ``,`` with ``\\x2c`` per ANNOVAR field encoding convention."""
    return value.replace(",", "\\x2c")
