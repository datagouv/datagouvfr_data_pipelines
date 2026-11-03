from abc import ABC, abstractmethod
from datetime import date

from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


class Delivery(ABC):
    _registry: dict[str, type["Delivery"]] = {}
    key: str  # month of the DVF delivery: "april" or "october"
    count: int  # number of the month of the DVF delivery: 4 or 10
    month: int  # number of data files / resources / years
    cadastre_millesime: str  # month of the cadastre millesime: "03", "06"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._registry[cls.key] = cls

    @classmethod
    def from_key(cls, key: str) -> "Delivery":
        if key in cls._registry:
            return cls._registry[key]()
        raise ValueError(f"Unknown delivery: {key!r}")

    @classmethod
    def from_date(cls, run_date: date) -> "Delivery":
        key = "april" if 4 <= run_date.month < 10 else "october"
        return cls.from_key(key)

    @abstractmethod
    def check_dvf_versions(self, dvf_versions: list[DvfVersion]):
        """Check that the list of DVF versions match the delivery

        The number of versions must match, years must be consecutive,
        and the half/full years must match the expected pattern.

        Raises if the check fails.
        """

        pass


class AprilDelivery(Delivery):
    key = "april"
    count = 5
    month = 4
    cadastre_millesime = "03"

    def check_dvf_versions(self, dvf_versions: list[DvfVersion]):
        """Check that the list of DVF versions are 5 consecutive full years

        Raises if the check fails.
        """
        if len(dvf_versions) != self.count:
            raise ValueError(
                f"april delivery requires {self.count} full years, got {len(dvf_versions)} versions"
            )
        if any(v.semester for v in dvf_versions):
            raise ValueError("april delivery can only contain full years")

        _check_consecutive_years([v.year for v in dvf_versions])


class OctoberDelivery(Delivery):
    key = "october"
    count = 6
    month = 10
    cadastre_millesime = "06"

    def check_dvf_versions(self, dvf_versions: list[DvfVersion]):
        """Check that the list of DVF versions match october delivery.

        Expected: 1 second semester, 4 full years, 1 first semester, all consecutive years

        Raises if the check fails.
        """
        if len(dvf_versions) != 6:
            raise ValueError(
                f"october delivery requires {self.count} files, got {len(dvf_versions)} versions"
            )
        # oldest year holds its last semester (s2)
        if dvf_versions[0].semester != "s2":
            raise ValueError(
                "october delivery oldest year must hold its second semester (s2): "
                f"{dvf_versions[0].get_zip_file_name()}"
            )
        # newest year holds its first semester (s1)
        if dvf_versions[-1].semester != "s1":
            raise ValueError(
                "october delivery newest year must hold its first semester (s1): "
                f"{dvf_versions[-1].get_zip_file_name()}"
            )
        # the 4 middle years are full years
        if any(v.semester for v in dvf_versions[1:-1]):
            raise ValueError("october delivery full years cannot hold a semester")

        _check_consecutive_years([v.year for v in dvf_versions])


def _check_consecutive_years(years: list[str]) -> None:
    """Raise if the years are not strictly consecutive and increasing."""
    for prev, curr in zip(years, years[1:]):
        if int(curr) != int(prev) + 1:
            raise ValueError(f"Years are not consecutive: {years}")
