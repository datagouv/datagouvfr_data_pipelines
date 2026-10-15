import logging
from dataclasses import dataclass
from datetime import date

from datagouv import Resource
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    get_delivery_info,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


@dataclass
class DvfResource:
    resource: Resource
    dvf_version: DvfVersion


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

        # will raise if the file name is not formed as expected
        dvf_version = DvfVersion.from_zip_file_name(file_name)
        dvf_resources.append(
            DvfResource(
                resource=resource,
                dvf_version=dvf_version,
            )
        )
    dvf_resources.sort(key=lambda d: (d.dvf_version.year, d.dvf_version.semester or ""))

    if delivery_info.delivery == "april":
        # five full years, no half-year files
        if any(d.dvf_version.semester for d in dvf_resources):
            raise ValueError(
                f"Unexpected semester file in an april delivery: "
                f"{[d.dvf_version.get_zip_file_name() for d in dvf_resources if d.dvf_version.semester]}"
            )
    else:
        # october: oldest year holds its last semester (s2), newest year its first (s1)
        if dvf_resources[0].dvf_version.semester != "s2":
            raise ValueError(
                f"Oldest year must hold its last semester (s2): "
                f"{dvf_resources[0].dvf_version.get_zip_file_name()}"
            )
        if dvf_resources[-1].dvf_version.semester != "s1":
            raise ValueError(
                f"Newest year must hold its first semester (s1): "
                f"{dvf_resources[-1].dvf_version.get_zip_file_name()}"
            )
        if any(d.dvf_version.semester for d in dvf_resources[1:-1]):
            raise ValueError(
                f"Unexpected semester file in a full year: "
                f"{[d.dvf_version.get_zip_file_name() for d in dvf_resources[1:-1] if d.dvf_version.semester]}"
            )

    return dvf_resources
