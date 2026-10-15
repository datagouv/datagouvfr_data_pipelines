from dataclasses import dataclass
from typing import Literal


@dataclass
class DvfVersion:
    year: str
    semester: (
        Literal["s1", "s2"] | None
    )  # "s1"/"s2" for half-year files, None for full years

    def get_unzipped_file_name(self) -> str:
        year_and_semester = self.year
        if self.semester:
            year_and_semester += f"-{self.semester.upper()}"
        return f"ValeurFoncieres-{year_and_semester}.txt"
