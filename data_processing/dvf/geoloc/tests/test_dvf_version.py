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


class TestFromUnzippedFileName:
    def test_full_year(self):
        v = DvfVersion.from_unzipped_file_name("ValeurFoncieres-2021.txt")
        assert v.year == "2021"
        assert v.semester is None

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_uppercase_normalized_to_lowercase(self, semester):
        v = DvfVersion.from_unzipped_file_name(
            f"ValeurFoncieres-2021-{semester.upper()}.txt"
        )
        assert v.year == "2021"
        assert v.semester == semester

    @pytest.mark.parametrize(
        "file_name",
        [
            "ValeurFoncieres-2021",  # missing .txt extension
            "ValeurFoncieres-2021.txt.zip",  # wrong extension
            "ValeurFoncieres-21-S1.txt",  # not 4-digit year
            "ValeurFoncieres-2021-S3.txt",  # invalid semester
            "valeursfoncieres-2021-S1.txt",  # wrong prefix case
            "ValeurFoncieres-2021-S1.txt/stuff",  # trailing content
            "other.csv",
        ],
    )
    def test_invalid_name_raises(self, file_name):
        with pytest.raises(ValueError, match="Unexpected file name"):
            DvfVersion.from_unzipped_file_name(file_name)


class TestFromTempParquetFileName:
    def test_full_year(self):
        v = DvfVersion.from_temp_parquet_file_name("temp-2021.parquet")
        assert v.year == "2021"
        assert v.semester is None

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester(self, semester):
        v = DvfVersion.from_temp_parquet_file_name(f"temp-2021-{semester}.parquet")
        assert v.year == "2021"
        assert v.semester == semester

    @pytest.mark.parametrize(
        "file_name",
        [
            "temp-2021",  # missing .parquet
            "temp-2021.parquet.gz",  # wrong extension
            "temp-21-s1.parquet",  # not 4-digit year
            "temp-2021-S1.parquet",  # uppercase semester
            "temp-2021-s3.parquet",  # invalid semester
            "valeursfoncieres-2021.parquet",  # wrong prefix
            "temp-2021-s1.parquet/stuff",  # trailing content
            "other.parquet",
        ],
    )
    def test_invalid_name_raises(self, file_name):
        with pytest.raises(ValueError, match="Unexpected file name"):
            DvfVersion.from_temp_parquet_file_name(file_name)


class TestIsTempParquetFileName:
    def test_full_year_true(self):
        assert DvfVersion.is_temp_parquet_file_name("temp-2021.parquet") is True

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_true(self, semester):
        assert (
            DvfVersion.is_temp_parquet_file_name(f"temp-2021-{semester}.parquet")
            is True
        )

    @pytest.mark.parametrize(
        "file_name",
        [
            "temp-2021",  # missing .parquet
            "temp-2021.parquet.gz",  # wrong extension
            "temp-21-s1.parquet",  # not 4-digit year
            "temp-2021-S1.parquet",  # uppercase semester
            "temp-2021-s3.parquet",  # invalid semester
            "valeursfoncieres-2021.parquet",  # wrong prefix
            "temp-2021-s1.parquet/stuff",  # trailing content
            "other.parquet",
            "",
            "cadastre.parquet",
        ],
    )
    def test_invalid_name_false(self, file_name):
        assert DvfVersion.is_temp_parquet_file_name(file_name) is False

    @pytest.mark.parametrize(
        "version",
        [
            DvfVersion("2021", None),
            DvfVersion("2021", "s1"),
            DvfVersion("2021", "s2"),
            DvfVersion("1999", "s2"),
        ],
    )
    def test_round_trip_is_true(self, version):
        assert (
            DvfVersion.is_temp_parquet_file_name(version.get_temp_parquet_file_name())
            is True
        )


class TestFromOutputFileName:
    def test_full_year(self):
        v = DvfVersion.from_output_file_name("full-2021.csv.gz")
        assert v.year == "2021"
        assert v.semester is None

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester(self, semester):
        v = DvfVersion.from_output_file_name(f"full-2021-{semester}.csv.gz")
        assert v.year == "2021"
        assert v.semester == semester

    @pytest.mark.parametrize(
        "file_name",
        [
            "full-2021",  # missing .csv.gz
            "full-2021.csv.gz.gz",  # wrong extension
            "full-21-s1.csv.gz",  # not 4-digit year
            "full-2021-S1.csv.gz",  # uppercase semester
            "full-2021-s3.csv.gz",  # invalid semester
            "temp-2021.csv.gz",  # wrong prefix
            "full-2021-s1.csv.gz/stuff",  # trailing content
            "other.csv.gz",
            "",
        ],
    )
    def test_invalid_name_raises(self, file_name):
        with pytest.raises(ValueError, match="Unexpected file name"):
            DvfVersion.from_output_file_name(file_name)


class TestIsOutputFileName:
    def test_full_year_true(self):
        assert DvfVersion.is_output_file_name("full-2021.csv.gz") is True

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_true(self, semester):
        assert DvfVersion.is_output_file_name(f"full-2021-{semester}.csv.gz") is True

    @pytest.mark.parametrize(
        "file_name",
        [
            "full-2021",  # missing .csv.gz
            "full-2021.csv.gz.gz",  # wrong extension
            "full-21-s1.csv.gz",  # not 4-digit year
            "full-2021-S1.csv.gz",  # uppercase semester
            "full-2021-s3.csv.gz",  # invalid semester
            "temp-2021.csv.gz",  # wrong prefix
            "full-2021-s1.csv.gz/stuff",  # trailing content
            "other.csv.gz",
            "",
        ],
    )
    def test_invalid_name_false(self, file_name):
        assert DvfVersion.is_output_file_name(file_name) is False

    @pytest.mark.parametrize(
        "version",
        [
            DvfVersion("2021", None),
            DvfVersion("2021", "s1"),
            DvfVersion("2021", "s2"),
            DvfVersion("1999", "s2"),
        ],
    )
    def test_round_trip_is_true(self, version):
        assert DvfVersion.is_output_file_name(version.get_output_file_name()) is True


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

    @pytest.mark.parametrize(
        "version",
        [
            DvfVersion("2021", None),
            DvfVersion("2021", "s1"),
            DvfVersion("2021", "s2"),
            DvfVersion("1999", "s2"),
            DvfVersion("2030", None),
        ],
    )
    def test_idempotent(self, version):
        unzipped_name = version.get_unzipped_file_name()
        assert DvfVersion.from_unzipped_file_name(unzipped_name) == version


class TestGetTempParquetFileName:
    def test_full_year(self):
        assert DvfVersion("2021", None).get_temp_parquet_file_name() == (
            "temp-2021.parquet"
        )

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_lowercase(self, semester):
        assert DvfVersion("2021", semester).get_temp_parquet_file_name() == (
            f"temp-2021-{semester}.parquet"
        )

    @pytest.mark.parametrize(
        "version",
        [
            DvfVersion("2021", None),
            DvfVersion("2021", "s1"),
            DvfVersion("2021", "s2"),
            DvfVersion("1999", "s2"),
            DvfVersion("2030", None),
        ],
    )
    def test_idempotent(self, version):
        parquet_name = version.get_temp_parquet_file_name()
        assert DvfVersion.from_temp_parquet_file_name(parquet_name) == version


class TestGetOutputFileName:
    def test_full_year(self):
        assert DvfVersion("2021", None).get_output_file_name() == ("full-2021.csv.gz")

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_lowercase(self, semester):
        assert DvfVersion("2021", semester).get_output_file_name() == (
            f"full-2021-{semester}.csv.gz"
        )

    @pytest.mark.parametrize(
        "version",
        [
            DvfVersion("2021", None),
            DvfVersion("2021", "s1"),
            DvfVersion("2021", "s2"),
            DvfVersion("1999", "s2"),
            DvfVersion("2030", None),
        ],
    )
    def test_idempotent(self, version):
        output_name = version.get_output_file_name()
        assert DvfVersion.from_output_file_name(output_name) == version


class TestGetId:
    def test_full_year(self):
        assert DvfVersion("2021", None).get_id() == "2021"

    @pytest.mark.parametrize("semester", ["s1", "s2"])
    def test_semester_lowercase(self, semester):
        assert DvfVersion("2021", semester).get_id() == f"2021-{semester}"


class TestDataclass:
    def test_field_values(self):
        v = DvfVersion(year="2021", semester=None)
        assert v.year == "2021"
        assert v.semester is None

    def test_equality(self):
        assert DvfVersion("2021", "s1") == DvfVersion("2021", "s1")
        assert DvfVersion("2021", "s1") != DvfVersion("2021", "s2")
        assert DvfVersion("2021", None) != DvfVersion("2022", None)
