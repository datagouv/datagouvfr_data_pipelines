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

    def get_unzipped_file_name(self) -> str:
        year_and_semester = self.year
        if self.semester:
            year_and_semester += f"-{self.semester.upper()}"
            # ^ unzipped file has uppercase semester suffix: -S1 or -S2
        return f"ValeurFoncieres-{year_and_semester}.txt"

    def get_zip_file_name(self) -> str:
        year_and_semester = self.year
        if self.semester:
            year_and_semester += f"-{self.semester}"
        return f"valeursfoncieres-{year_and_semester}.txt.zip"
