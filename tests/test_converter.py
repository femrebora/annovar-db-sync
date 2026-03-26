"""Tests for annovar_db_sync.converter."""
from __future__ import annotations

from pathlib import Path

import pytest

from annovar_db_sync.converter import (
    _escape_commas,
    _parse_info_fields,
    convert_clinvar_avinput_to_annovar_db,
)


class TestEscapeCommas:
    def test_no_commas(self):
        assert _escape_commas("hello_world") == "hello_world"

    def test_single_comma(self):
        assert _escape_commas("a,b") == r"a\x2cb"

    def test_multiple_commas(self):
        assert _escape_commas("a,b,c") == r"a\x2cb\x2cc"

    def test_empty_string(self):
        assert _escape_commas("") == ""


class TestParseInfoFields:
    def test_basic_fields(self):
        info = "ALLELEID=12345;CLNDN=not_provided;CLNSIG=Pathogenic"
        fields = ("ALLELEID", "CLNDN", "CLNSIG", "CLNDISDB", "CLNREVSTAT")
        result = _parse_info_fields(info, fields)
        assert result["CLNALLELEID"] == "12345"
        assert result["CLNDN"] == "not_provided"
        assert result["CLNSIG"] == "Pathogenic"

    def test_missing_fields_default_to_dot(self):
        info = "ALLELEID=999"
        fields = ("ALLELEID", "CLNDN", "CLNDISDB", "CLNREVSTAT", "CLNSIG")
        result = _parse_info_fields(info, fields)
        assert result["CLNDN"] == "."
        assert result["CLNDISDB"] == "."
        assert result["CLNREVSTAT"] == "."
        assert result["CLNSIG"] == "."

    def test_alleleid_maps_to_clnalleleid(self):
        info = "ALLELEID=42"
        result = _parse_info_fields(info, ("ALLELEID",))
        assert "CLNALLELEID" in result
        assert result["CLNALLELEID"] == "42"
        assert "ALLELEID" not in result

    def test_token_without_equals_ignored(self):
        info = "FLAG_WITHOUT_VALUE;ALLELEID=1"
        result = _parse_info_fields(info, ("ALLELEID",))
        assert result["CLNALLELEID"] == "1"


class TestConvertClinvarAvinput:
    def test_converts_test_fixture(self, sample_avinput: Path, tmp_path: Path):
        out = tmp_path / "output.txt"
        result = convert_clinvar_avinput_to_annovar_db(sample_avinput, out)
        assert result == out
        assert out.exists()
        lines = out.read_text().splitlines()
        # First line is the header
        assert lines[0].startswith("#Chr")
        assert "CLNALLELEID" in lines[0]
        # At least some data rows
        assert len(lines) > 1

    def test_header_columns(self, sample_avinput: Path, tmp_path: Path):
        out = tmp_path / "output.txt"
        convert_clinvar_avinput_to_annovar_db(sample_avinput, out)
        header = out.read_text().splitlines()[0]
        for col in ("#Chr", "Start", "End", "Ref", "Alt",
                    "CLNALLELEID", "CLNDN", "CLNDISDB", "CLNREVSTAT", "CLNSIG"):
            assert col in header

    def test_commas_escaped_in_output(self, sample_avinput: Path, tmp_path: Path):
        out = tmp_path / "output.txt"
        convert_clinvar_avinput_to_annovar_db(sample_avinput, out)
        content = out.read_text()
        # Unescaped commas should NOT appear inside data rows
        # (they can appear in header's tab-separated columns but not values)
        data_rows = [l for l in content.splitlines() if not l.startswith("#")]
        for row in data_rows:
            # commas inside individual fields should be escaped
            cols = row.split("\t")
            for col in cols[5:]:  # annotation columns
                assert "," not in col or r"\x2c" in col

    def test_missing_alleleid_raises(self, sample_avinput: Path, tmp_path: Path):
        out = tmp_path / "output.txt"
        with pytest.raises(ValueError, match="ALLELEID"):
            convert_clinvar_avinput_to_annovar_db(
                sample_avinput, out, fields=("CLNDN", "CLNSIG")
            )

    def test_empty_avinput_produces_only_header(self, tmp_path: Path):
        empty = tmp_path / "empty.avinput"
        empty.write_text("")
        out = tmp_path / "output.txt"
        convert_clinvar_avinput_to_annovar_db(empty, out)
        lines = out.read_text().splitlines()
        assert len(lines) == 1
        assert lines[0].startswith("#Chr")
