from dataclasses import dataclass
from datetime import date
from typing import Literal

from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


@dataclass
class DeliveryInfo:
    delivery: str  # month of the DVF delivery: "april" or "october"
    month: int  # month of the DVF delivery: 4 or 10
    file_count: int  # number of data files: 5 or 6
    cadastre_millesime: str  # month of the cadastre millesime: "03", "06"


DELIVERIES = {
    "april": DeliveryInfo(
        delivery="april", month=4, file_count=5, cadastre_millesime="03"
    ),
    "october": DeliveryInfo(
        delivery="october", month=10, file_count=6, cadastre_millesime="06"
    ),
}


def get_delivery_from_today(today: date) -> str:
    return "april" if 4 <= today.month < 10 else "october"


def get_delivery_info(delivery: str | None, today: date) -> DeliveryInfo:
    if delivery is None:
        delivery = get_delivery_from_today(today)
    delivery_info = DELIVERIES.get(delivery)
    if delivery_info is None:
        raise ValueError(f"Unknown delivery: {delivery!r}")
    return delivery_info


def check_delivery_versions(
    delivery: Literal["april", "october"], dvf_versions: list[DvfVersion]
) -> None:
    """Check that the list of DVF versions match the delivery

    For april: 5 consecutive full years
    For october: 1 second semester, 4 full years, 1 first semester, all consecutive years

    Raises if the check fails.
    """
    # the versions are expected in oldest -> newest order, which is what "consecutive
    # years" and the shape checks below rely on.
    if delivery == "april":
        if len(dvf_versions) != 5:
            raise ValueError(
                f"april delivery requires 5 full years, got {len(dvf_versions)} versions"
            )
        if any(v.semester for v in dvf_versions):
            raise ValueError("april delivery can only contain full years")
    else:  # october
        if len(dvf_versions) != 6:
            raise ValueError(
                f"october delivery requires 6 files, got {len(dvf_versions)} versions"
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
