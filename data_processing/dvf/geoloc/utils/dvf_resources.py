import logging
import re
from dataclasses import dataclass
from datetime import date

from datagouv import Resource
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.deliveries import (
    get_delivery_info,
)


@dataclass
class DvfResource:
    file_name: str
    resource: Resource
    year: str
    semester: str | None = None  # "s1"/"s2" for half-year files, None for full years


def get_dvf_resources(
    resources: list[Resource],
    delivery: str | None = None,
    today: date | None = None,
) -> list[DvfResource]:
    """Return the DVF source resources of the rolling window for a given delivery.

    Only the "main" resources (ie. data files) are kept, and their file names are
    checked against the ``valeursfoncieres-<year>[-s<semester>].txt.zip`` pattern.
    The number and the shape of the files are validated against the delivery:
    five full years for "april", and for "october" six files, the oldest year holding
    its last semester (``-s2``) and the newest year its first one (``-s1``).

    Parameters
    ----------
    resources : list[Resource]
        The resources of the DVF source dataset.
    delivery : str | None
        Which delivery the window is built for, ``"april"`` or ``"october"``.
        Defaults to the delivery matching ``today``.
    today : date | None
        The reference date used to infer the delivery when it is not given.
        Defaults to ``date.today()``.

    Returns
    -------
    list[DvfResource]
        A list of each yearly (or half-yearly) data file with its Resource, file
        name (e.g. "valeursfoncieres-2021-s2.txt.zip"), year and semester (if any),
        sorted by year then semester.

    Raises
    ------
    ValueError
        If the delivery is unknown, if the number or the shape of the files does not
        match the delivery's rolling window, or if a file name does not match the
        expected pattern.
    """
    if today is None:
        today = date.today()
    delivery_info = get_delivery_info(delivery, today)

    main_resources = [res for res in resources if res.type == "main"]
    if len(main_resources) != delivery_info.file_count:
        raise ValueError(
            f"Unexpected number of resources for a {delivery_info.delivery} delivery: "
            f"{len(main_resources)} (expected {delivery_info.file_count})"
        )
    titles = [f'"{res.title}"' for res in main_resources]
    logging.info(f"Available ressources : {','.join(titles)}...")

    dvf_resources: list[DvfResource] = []
    for resource in main_resources:
        file_name = resource.url.split("/")[-1]
        # checking that the year and optional semester are where we expect them
        m = re.search(
            r"^valeursfoncieres-(?P<year>\d{4})(?:-(?P<semester>s[12]))?\.txt\.zip$",
            file_name,
        )
        if not m:
            raise ValueError(f"Unexpected file name: {file_name}")
        dvf_resources.append(
            DvfResource(
                file_name=file_name,
                resource=resource,
                year=m.group("year"),
                semester=m.group("semester"),
            )
        )
    dvf_resources.sort(key=lambda d: (d.year, d.semester or ""))

    if delivery_info.delivery == "april":
        # five full years, no half-year files
        if any(d.semester for d in dvf_resources):
            raise ValueError(
                f"Unexpected semester file in an april delivery: "
                f"{[d.file_name for d in dvf_resources if d.semester]}"
            )
    else:
        # october: oldest year holds its last semester (s2), newest year its first (s1)
        if dvf_resources[0].semester != "s2":
            raise ValueError(
                f"Oldest year must hold its last semester (s2): "
                f"{dvf_resources[0].file_name}"
            )
        if dvf_resources[-1].semester != "s1":
            raise ValueError(
                f"Newest year must hold its first semester (s1): "
                f"{dvf_resources[-1].file_name}"
            )
        if any(d.semester for d in dvf_resources[1:-1]):
            raise ValueError(
                f"Unexpected semester file in a full year: "
                f"{[d.file_name for d in dvf_resources[1:-1] if d.semester]}"
            )

    return dvf_resources
