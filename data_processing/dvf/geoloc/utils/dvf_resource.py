import logging
from dataclasses import dataclass

from datagouv import Resource
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    Delivery,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


@dataclass
class DvfResource:
    resource: Resource
    dvf_version: DvfVersion


def get_dvf_resources(
    resources: list[Resource], delivery: Delivery
) -> list[DvfResource]:
    """Return the DVF source resources of the rolling window for a given delivery (chronologically sorted).

    Only the "main" resources (ie. data files) are kept, and their file names are
    checked against the ``valeursfoncieres-<year>[-s<semester>].txt.zip`` pattern.
    The number and the shape of the files are validated against the delivery:
    five full years for "april", and for "october" six files, the oldest year holding
    its last semester (``-s2``) and the newest year its first one (``-s1``).

    Parameters
    ----------
    resources : list[Resource]
        The resources of the DVF source dataset.
    delivery : Delivery
        The source data delivery (april or october).

    Returns
    -------
    list[DvfResource]
        A list of each yearly (or half-yearly) data file with its Resource, file
        name (e.g. "valeursfoncieres-2021-s2.txt.zip"), year and semester (if any),
        sorted by year then semester.

    Raises
    ------
    ValueError
        If the number or the shape of the files does not match the delivery's
        rolling window, or if a file name does not match the expected pattern.
    """
    main_resources = [res for res in resources if res.type == "main"]
    if len(main_resources) != delivery.count:
        raise ValueError(
            f"Unexpected number of resources for a {delivery.key} delivery: "
            f"{len(main_resources)} (expected {delivery.count})"
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

    # Ensure the source files respect the expected pattern (years, semesters)
    delivery.check_dvf_versions([d.dvf_version for d in dvf_resources])

    return dvf_resources
