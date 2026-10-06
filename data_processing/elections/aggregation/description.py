"""Description of the dataset: the manual text of description.yaml followed by the list
of the sources, generated from sources.json (no Airflow, called by the
publish_description task)."""

import re

DATASET_URL = "https://www.data.gouv.fr/datasets/{}"
# most recent first within a year (approximately the calendar of the elections)
TYPES_ORDER = ["sena", "legi", "pres", "euro", "regi", "dpmt", "cant", "muni"]


def sort_key(key: str) -> tuple:
    # "2022_legi_t2", "2001_cant", "2016_03_legi_part"
    year, rest = key.split("_", 1)
    partial = rest.endswith("_part")
    election_type = re.search(r"(sena|legi|pres|euro|regi|dpmt|cant|muni)", rest).group(
        1
    )
    round_ = re.search(r"_t(\d)$", rest)
    # the partial elections keys carry their month: "2016_03_legi_part"
    month = re.match(r"(\d{2})_", rest)
    # partial elections after the general ones, then most recent first
    return (
        partial,
        -int(year),
        -int(month.group(1)) if month else 0,
        TYPES_ORDER.index(election_type),
        -int(round_.group(1)) if round_ else 0,
    )


def source_line(source: dict) -> str:
    description = source["description"]
    dataset_id = (
        source["resultats"]["source_dataset_id"]
        if "resultats" in source
        else description["source_dataset_id"]
    )
    link = f"[{description['libelle']}]({DATASET_URL.format(dataset_id)})"
    # the elections that are not integrated are struck through
    line = f"- {link}" if "resultats" in source else f"- ~~{link}~~"
    if description.get("commentaire"):
        line += f" ({description['commentaire']})"
    return line


def build_description(texts: dict, sources: dict) -> str:
    lines = [source_line(sources[key]) for key in sorted(sources, key=sort_key)]
    description = (
        texts["introduction"].rstrip()
        + "\n\n"
        + texts["titre_sources"]
        + "\n"
        + "\n".join(lines)
        + "\n"
    )
    # manual history of the important changes, after the list of the sources
    if texts.get("historique"):
        description += "\n" + texts["historique"].rstrip() + "\n"
    return description
