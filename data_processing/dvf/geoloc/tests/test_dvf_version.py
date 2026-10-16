"""Unit tests for DvfVersion (utils/dvf_version)."""

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


class TestFromZipFileName:
    def test_full_year(self):
        v = DvfVersion.from_zip_file_name("valeursfoncieres-2021.txt.zip")
        assert v.year == "2021"
        assert v.semester is None

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester(self, semester):
        v = DvfVersion.from_zip_file_name(f"valeursfoncieres-2021-{semester}.txt.zip")
        assert v.year == "2021"
        assert v.semester == semester

    @pytest.mark.parametrize(
        "file_name",
        [
            "valeursfoncieres-2021.txt",  # missing .zip
            "valeursfoncieres-2021.zip",  # missing .txt
            "valeursfoncieres-21-s1.txt.zip",  # not 4-digit year
            "valeursfoncieres-2021-s1.txt.zipx",  # wrong extension
            "valeursfoncieres-2021-S1.txt.zip",  # uppercase semester
            "valeursfoncieres-2021-s3.txt.zip",  # invalid semester
            "ValeurFoncieres-2021.txt.zip",  # wrong prefix case
            "valeursfoncieres-2021-s1.txt.zip/stuff",  # trailing content
            "other.csv",
        ],
    )
    def test_invalid_name_raises(self, file_name):
        with pytest.raises(ValueError, match="Unexpected file name"):
            DvfVersion.from_zip_file_name(file_name)


class TestGetZipFileName:
    def test_full_year_lowercase(self):
        assert DvfVersion("2021", None).get_zip_file_name() == (
            "valeursfoncieres-2021.txt.zip"
        )

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_lowercase(self, semester):
        assert DvfVersion("2021", semester).get_zip_file_name() == (
            f"valeursfoncieres-2021-{semester}.txt.zip"
        )

    @pytest.mark.parametrize(
        "version",
        [
            DvfVersion("2021", None),
            DvfVersion("2021", "s1"),
            DvfVersion("2021", "s2"),
            DvfVersion("1999", "s1"),
            DvfVersion("2030", None),
        ],
    )
    def test_idempotent(self, version):
        zip_name = version.get_zip_file_name()
        assert DvfVersion.from_zip_file_name(zip_name) == version


class TestGetUnzippedFileName:
    def test_full_year(self):
        assert DvfVersion("2021", None).get_unzipped_file_name() == (
            "ValeurFoncieres-2021.txt"
        )

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_uppercase(self, semester):
        assert DvfVersion("2021", semester).get_unzipped_file_name() == (
            f"ValeurFoncieres-2021-{semester.upper()}.txt"
        )


class TestDataclass:
    def test_field_values(self):
        v = DvfVersion(year="2021", semester=None)
        assert v.year == "2021"
        assert v.semester is None

    def test_equality(self):
        assert DvfVersion("2021", "s1") == DvfVersion("2021", "s1")
        assert DvfVersion("2021", "s1") != DvfVersion("2021", "s2")
        assert DvfVersion("2021", None) != DvfVersion("2022", None)
