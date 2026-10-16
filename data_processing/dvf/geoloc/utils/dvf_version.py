import re
from dataclasses import dataclass
from typing import Literal, cast


@dataclass
class DvfVersion:
    year: str
    semester: (
        Literal["s1", "s2"] | None
    )  # "s1"/"s2" for half-year files, None for full years

    @classmethod
    def from_zip_file_name(cls, file_name: str) -> "DvfVersion":
        # checking that the year and optional semester are where we expect them
        m = re.search(
            r"^valeursfoncieres-(?P<year>\d{4})(?:-(?P<semester>s[12]))?\.txt\.zip$",
            file_name,
        )
        if not m:
            raise ValueError(f"Unexpected file name: {file_name}")
        return cls(
            year=m.group("year"),
            semester=cast(Literal["s1", "s2"] | None, m.group("semester")),
        )

    @classmethod
    def from_unzipped_file_name(cls, file_name: str) -> "DvfVersion":
        # The unzipped file has an uppercase semester suffix (-S1 or -S2),
        # which is normalized back to the lowercase internal form.
        m = re.search(
            r"^ValeurFoncieres-(?P<year>\d{4})(?:-(?P<semester>[Ss][12]))?\.txt$",
            file_name,
        )
        if not m:
            raise ValueError(f"Unexpected file name: {file_name}")
        semester = m.group("semester")
        return cls(
            year=m.group("year"),
            semester=cast(
                Literal["s1", "s2"] | None, semester.lower() if semester else None
            ),
        )

    @classmethod
    def from_temp_parquet_file_name(cls, file_name: str) -> "DvfVersion":
        # The temp parquet file uses the same lowercase suffix as the zip file.
        m = re.search(
            r"^temp-(?P<year>\d{4})(?:-(?P<semester>s[12]))?\.parquet$",
            file_name,
        )
        if not m:
            raise ValueError(f"Unexpected file name: {file_name}")
        return cls(
            year=m.group("year"),
            semester=cast(Literal["s1", "s2"] | None, m.group("semester")),
        )

    @classmethod
    def is_temp_parquet_file_name(cls, file_name: str) -> bool:
        try:
            cls.from_temp_parquet_file_name(file_name)
            return True
        except ValueError:
            return False

    @classmethod
    def from_output_file_name(cls, file_name: str) -> "DvfVersion":
        m = re.search(
            r"^full-(?P<year>\d{4})(?:-(?P<semester>s[12]))?\.csv.gz$",
            file_name,
        )
        if not m:
            raise ValueError(f"Unexpected file name: {file_name}")
        return cls(
            year=m.group("year"),
            semester=cast(Literal["s1", "s2"] | None, m.group("semester")),
        )

    @classmethod
    def is_output_file_name(cls, file_name: str) -> bool:
        try:
            cls.from_output_file_name(file_name)
            return True
        except ValueError:
            return False

    def get_id(self) -> str:
        id = self.year
        if self.semester:
            id += f"-{self.semester}"
        return id

    def get_unzipped_file_name(self) -> str:
        suffix = self.get_id().upper()
        # ^ unzipped file has uppercase semester suffix: -S1 or -S2
        return f"ValeurFoncieres-{suffix}.txt"

    def get_zip_file_name(self) -> str:
        return f"valeursfoncieres-{self.get_id()}.txt.zip"

    def get_temp_parquet_file_name(self) -> str:
        return f"temp-{self.get_id()}.parquet"

    def get_output_file_name(self) -> str:
        return f"full-{self.get_id()}.csv.gz"
